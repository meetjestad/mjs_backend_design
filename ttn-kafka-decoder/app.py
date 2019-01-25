import logging
import os

import pykafka
import base64
import cbor2
import json

# TODO: Use the Kafka Streams API? Might be better supported in java?
# https://github.com/wintoncode/winton-kafka-streams
# Without that, issues such as failover, exactly-once processing and order
# guarantees between config packets and data packets are unsolved.

def process_message(message):
    try:
        msg_as_string = message.value.decode('utf8')
        logging.debug('Received message {}: {}'.format(message.offset, msg_as_string))
        msg_obj = json.loads(msg_as_string)
        payload = base64.b64decode(msg_obj.get('payload_raw', ''))
    except json.JSONDecodeError as e:
        logging.warn('Error parsing JSON payload')
        logging.warn(e)
        return None

    try:
        decoded = decode_message(msg_obj, payload)
    except Exception as e:
        logging.exception('Error processing packet')
        return None

    result = json.dumps(decoded)
    logging.debug('Returning message {}'.format(result))
    return result.encode('utf8')

def decode_message(msg, payload):
    port = msg["port"]
    if port == 1:
        decoded = decode_config_message(payload)
    elif port == 2:
        decoded = decode_data_message(payload)
    else:
        logging.warn('Ignoring message with unknown port: {}'.format(port))
        return

    logging.debug("Decoded message: {}".format(decoded))

    # TODO: Should we produce a single stream of (data?) messages? Or split
    # into measurements already? Should we enrich data with config here, or later?
    node_id = make_ttn_node_id(msg)
    msg_id = make_msg_id(node_id, msg)

    return {
        'id': msg_id,
        'node_id': node_id,
        'timestamp': msg['metadata']['time'],
        'data': decoded,
        # TODO: Should this be a reference?
        'original': msg,
    }

def make_ttn_node_id(msg):
    return 'ttn/{}/{}'.format(msg['app_id'], msg['dev_id'])

def make_msg_id(node_id, msg):
    return '{}/{}'.format(node_id, msg['metadata']['time'])

def decode_config_message(payload):
    #TODO
    packet = cbor2.loads(payload)
    if not isinstance(packet, list):
        logging.warn("Config packet is not list: {}".format(packet))
    def decode(obj):
        return decode_cbor_obj(obj, config_packet_keys, config_packet_values)
    return list(map(decode, packet))

def decode_data_message(payload):
    #TODO
    return cbor2.loads(payload)


# TODO: Write script to convert below values to a reverse mapping usable in the
# C++ code.
config_packet_keys = {
    1: 'channel_id',
    2: 'quantity',
    3: 'unit',
    4: 'sensor',
    5: 'item_type',
}

config_packet_values = {
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
        logging.warn("Element to decode is not object: {}".format(obj))
        return obj

    out = {}
    for key, value in obj.items():
        if isinstance(key, int):
            try:
                key = keys[key]
            except KeyError as e:
                # TODO: Store warnings in output?
                logging.warn('Unknown integer key in packet: {}={}'.format(key, value))
        if isinstance(value, int):
            values_for_this_key = values.get(key, False)
            if values_for_this_key:
                try:
                    value = values_for_this_key[value]
                except KeyError as e:
                    # TODO: Store warnings in output?
                    logging.warn('Unknown integer value in packet: {}={}'.format(key, value))
        out[key] = value
    return out



logging.basicConfig(level=logging.DEBUG)

kafka_broker = os.environ['KAFKA_BROKER']
kafka_topic_in = os.environ['KAFKA_TOPIC_IN']
kafka_topic_out = os.environ['KAFKA_TOPIC_OUT']

logging.info('Connecting Kafka to {}'.format(kafka_broker))
client = pykafka.KafkaClient(hosts=kafka_broker)
topic_in = client.topics[kafka_topic_in.encode()]
topic_out = client.topics[kafka_topic_out.encode()]

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
        except Exception as e:
            logging.exception('Error processing message')

# vim: set sw=4 sts=4 expandtab:
