#!/usr/bin/env python3
# vim:fileencoding=utf8
# pylint: disable=missing-docstring
import logging
import os
import base64
import json
import itertools

import elasticsearch
import pymongo
import pykafka
import cbor2

# TODO: Use the Kafka Streams API? Might be better supported in java?
# https://github.com/wintoncode/winton-kafka-streams
# Without that, issues such as failover, exactly-once processing and order
# guarantees between config packets and data packets are unsolved.

def process_message(message):
    try:
        msg_as_string = message.value.decode('utf8')
        logging.debug('Received message %s: %s', message.offset, msg_as_string)
        msg_obj = json.loads(msg_as_string)
        payload = base64.b64decode(msg_obj.get('payload_raw', ''))
    except json.JSONDecodeError as ex:
        logging.warning('Error parsing JSON payload')
        logging.warning(ex)
        return None

    try:
        decoded = decode_message(msg_obj, payload)
    # pylint: disable=broad-except
    except Exception as ex:
        logging.exception('Error processing packet: %s', ex)
        return None

    if decoded is None:
        return None

    result = json.dumps(decoded)
    logging.debug('Returning message %s', result)
    return result.encode('utf8')


def decode_message(msg, payload):
    port = msg["port"]
    if port == 1:
        return decode_config_message(msg, payload)
    if port == 2:
        return decode_data_message(msg, payload)
    logging.warning('Ignoring message with unknown port: %s', port)
    return None


def make_ttn_node_id(msg):
    return 'ttn/{}/{}'.format(msg['app_id'], msg['dev_id'])


def make_msg_id(node_id, msg):
    return '{}/{}'.format(node_id, msg['metadata']['time'])

def make_meas_id(msg_id, chan_id):
    return '{}/{}'.format(msg_id, chan_id)


def decode_config_message(msg, payload):
    entries = decode_config_packet(payload)
    logging.debug("Decoded config entries: %s", entries)
    config = decode_config_entries(entries)

    node_id = make_ttn_node_id(msg)
    msg_id = make_msg_id(node_id, msg)

    # HACK: Elasticsearch breaks if a field is sometimes a timestamp and
    # sometimes the empty string, so remove empty time fields for now...
    for gw_data in msg.get('metadata', {}).get('gateways', []):
        if 'time' in gw_data and not gw_data['time']:
            gw_data.pop('time')

    config.update({
        '_id': msg_id,
        'node_id': node_id,
        'timestamp': msg['metadata']['time'],
        # TODO: Should this be a reference?
        'sources': {
            'ttn': msg,
        }
    })
    logging.debug("Decoded config: %s", config)

    mongodb.config.update_one({'_id': msg_id}, {'$set': config}, upsert=True)
    config.pop('_id') # ES does not allow id inside the data
    es.index(index="config", id=msg_id, body=config)

    return config


def decode_config_packet(payload):
    packet = cbor2.loads(payload)
    if not isinstance(packet, list):
        logging.warning("Config packet is not list: %s", packet)

    def decode(obj):
        return decode_cbor_obj(obj, CONFIG_PACKET_KEYS, CONFIG_PACKET_VALUES)
    return list(map(decode, packet))


def decode_config_entries(entries):
    channels = {}
    node = {}
    for entry in entries:
        data = dict(entry)
        try:
            item = data.pop('item_type')
            if item == 'node':
                node.update(data)
            elif item == 'channel':
                chan_id = data.pop('channel_id')
                if chan_id in channels:
                    logging.warning("Duplicate channel entry in config message: %s", entry)
                else:
                    # Convert id to string, since mongo can only do string keys
                    channels[str(chan_id)] = data
            else:
                logging.warning("Unknown entry type in config message: %s", entry)
        except KeyError as ex:
            logging.warning("Invalid config message entry (missing %s): %s", ex.args, entry)

    message = {
        'node_config': node,
        'channel_config': channels,
    }
    return message


def decode_data_message(msg, payload):
    # TODO Decode shortcuts
    entries = cbor2.loads(payload)
    logging.debug("Decoded data entries: %s", entries)

    node_id = make_ttn_node_id(msg)
    msg_id = make_msg_id(node_id, msg)
    timestamp = msg['metadata']['time']

    config = mongodb.config.find_one(
        {'node_id': node_id, "timestamp": {"$lt": 'timestamp'}},
        sort=[("timestamp", pymongo.DESCENDING)]
    )
    logging.debug("Found relevant config: %s", config)

    channels = decode_data_entries(entries, config)
    logging.debug("Decoded data: %s", channels)

    # HACK: Elasticsearch breaks if a field is sometimes a timestamp and
    # sometimes the empty string, so remove empty time fields for now...
    for gw_data in msg.get('metadata', {}).get('gateways', []):
        if 'time' in gw_data and not gw_data['time']:
            gw_data.pop('time')

    data = {
        '_id': msg_id,
        'node_id': node_id,
        'timestamp': timestamp,
        'config_id': config['_id'] if config else None,
        'channels': channels,
        # TODO: Should these be a reference?
        'sources': {
            'ttn': msg,
            'config': config,
        }
    }
    logging.debug("Decoded data: %s", data)

    mongodb.data.update_one({'_id': msg_id}, {'$set': data}, upsert=True)
    data.pop('_id') # ES does not allow id inside the data
    es.index(index="data", id=msg_id, body=data)

    for chan_id, data in channels.items():
        meas_id = make_meas_id(msg_id, chan_id)
        chan_data = {
            '_id': meas_id,
            'node_id': node_id,
            'timestamp': msg['metadata']['time'],
            'config_id': config['_id'] if config else None,
            'channel_id': chan_id,
            'data': data,
            # TODO: Should these be a reference?
            'sources': {
                'ttn': msg,
                'config': config,
            }
        }
        logging.debug("Decoded single data: %s", chan_data)
        mongodb.data_single.update_one({'_id': meas_id}, {'$set': chan_data}, upsert=True)
        chan_data.pop('_id') # ES does not allow id inside the data
        es.index(index="data_single", id=meas_id, body=chan_data)

    return data


def decode_data_entries(entries, config):
    channels = {}

    for entry in entries:
        chan_data = dict(entry)
        try:
            chan_id = chan_data['channel_id']
        except KeyError as ex:
            logging.warning("Invalid config message entry (missing %s): %s", ex.args, entry)

        if chan_id in channels:
            logging.warning("Duplicate channel %s in data message: %s", chan_id, entry)
            continue

        try:
            chan_config = config['channel_config'][str(chan_id)]
        except KeyError:
            logging.warning("Missing config for channel %s: %s", chan_id, entry)
            # Still pass the data along untouched
            data = chan_data
        else:
            data = decode_data_entry(chan_data, chan_config)

        name = data.get('quantity', str(chan_id))

        if name in channels:
            for num in itertools.count(start=2):
                new_name = "{}_{}".format(name, num)
                if new_name not in channels:
                    name = new_name
                    break

        channels[name] = data

    return channels

def decode_data_entry(chan_data, chan_config):
    # Make copies we can modify
    data = dict(chan_data)
    config = dict(chan_config)

    # Add any remaining config keys to the data
    data.update(config)
    return data

# TODO: Write script to convert below values to a reverse mapping usable in the
# C++ code.
CONFIG_PACKET_KEYS = {
    1: 'channel_id',
    2: 'quantity',
    3: 'unit',
    4: 'sensor',
    5: 'item_type',
}

CONFIG_PACKET_VALUES = {
    'quantity': {
        1: 'temperature',
        2: 'humidity',
        3: 'voltage',
    },
    'unit': {
        # TODO: How to note these? Perhaps just '°C'?
        1: 'degree_celcius',
        2: 'percent_rh',
        3: 'volt',
    },
    'sensor': {
        1: 'Si2701',
    },
    'item_type': {
        1: 'node',
        2: 'channel',
    },
}

def decode_cbor_obj(obj, keys, values):
    if not isinstance(obj, dict):
        logging.warning("Element to decode is not object: %s", obj)
        return obj

    out = {}
    for key, value in obj.items():
        if isinstance(key, int):
            try:
                key = keys[key]
            except KeyError:
                # TODO: Store warnings in output?
                logging.warning('Unknown integer key in packet: %s=%s', key, value)
        if isinstance(value, int):
            values_for_this_key = values.get(key, False)
            if values_for_this_key:
                try:
                    value = values_for_this_key[value]
                except KeyError:
                    # TODO: Store warnings in output?
                    logging.warning('Unknown integer value in packet: %s=%s', key, value)
        out[key] = value
    return out




def main():
    global mongodb, es

    kafka_broker = os.environ['KAFKA_BROKER']
    kafka_topic_in = os.environ['KAFKA_TOPIC_IN']
    kafka_topic_out = os.environ['KAFKA_TOPIC_OUT']

    logging.info('Connecting Kafka to %s', kafka_broker)
    client = pykafka.KafkaClient(hosts=kafka_broker)
    topic_in = client.topics[kafka_topic_in.encode()]
    topic_out = client.topics[kafka_topic_out.encode()]

    mongodb_url = os.environ['MONGODB_URL']
    logging.info('Connecting MongoDB to %s', mongodb_url)
    mongo = pymongo.MongoClient(mongodb_url)
    mongodb = mongo.mjs
    status = mongodb.command("serverStatus")
    logging.info('MongoDB serverStatus: %s', str(status))

    elastic_host = os.environ['ELASTIC_HOST']
    logging.info('Connecting Elasticsearch to %s', elastic_host)
    es = elasticsearch.Elasticsearch(elastic_host)

    logging.basicConfig(level=logging.DEBUG)
    with topic_out.get_sync_producer() as producer:
        consumer = topic_in.get_simple_consumer(
            consumer_group='decoder',
            auto_commit_enable=True,
            reset_offset_on_start=False,
            #auto_offset_reset=pykafka.common.OffsetType.EARLIEST,
            #auto_offset_reset=287,
        )
        for message in consumer:
            try:
                decoded = process_message(message)
                if decoded is not None:
                    producer.produce(decoded)
            # pylint: disable=broad-except
            except Exception as ex:
                logging.exception('Error processing message: %s', ex)

main()

# vim: set sw=4 sts=4 expandtab:
