#!/usr/bin/env python3
# vim:fileencoding=utf8
# pylint: disable=missing-docstring

import logging
import os
import json
import base64
import traceback

import paho.mqtt.client as mqtt
import bitstring
import cbor2

from confluent_kafka import Producer

CONFIG_PORT = 1
DATA_PORT = 2

# Copied from ttn-kafka-decoder, do not modify here
CONFIG_PACKET_KEYS = {
    1: 'channel_id',
    2: 'quantity',
    3: 'unit',
    4: 'sensor',
    5: 'item_type',
    6: 'measured',
    7: 'divider',
}

# Copied from ttn-kafka-decoder, do not modify here
CONFIG_PACKET_VALUES = {
    'quantity': {
        1: 'temperature',
        2: 'humidity',
        3: 'voltage',
        4: 'ambient_light',
        5: 'latitude',
        6: 'longitude',
        7: 'particulate_matter',
    },
    'unit': {
        # TODO: How to note these? Perhaps just '°C'?
        1: 'degree_celcius',
        2: 'percent_rh',
        3: 'volt',
        4: 'ug_per_cubic_meter',
        5: 'lux',
        6: 'degrees',
    },
    'sensor': {
        1: 'Si2701',
    },
    'item_type': {
        1: 'node',
        2: 'channel',
    },
}

CONFIG_PACKET_KEYS_INVERTED = {v: k for k, v in CONFIG_PACKET_KEYS.items()}
CONFIG_PACKET_VALUES_INVERTED = {outer_k: {v: k for k, v in outer_v.items()}
                                 for outer_k, outer_v in CONFIG_PACKET_VALUES.items()}

def encode_cbor_obj(obj, keys, values):
    if not isinstance(obj, dict):
        logging.warning("Element to encode is not object: %s", obj)
        return obj

    out = {}
    for key, value in obj.items():
        if isinstance(value, str):
            values_for_this_key = values.get(key, False)
            if values_for_this_key:
                try:
                    value = values_for_this_key[value]
                except KeyError:
                    pass
        if isinstance(key, str):
            try:
                key = keys[key]
            except KeyError:
                pass
        out[key] = value
    return out

# Copied from ttn-kafka-decoder
def make_ttn_node_id(msg):
    return 'ttn/{}/{}'.format(msg['app_id'], msg['dev_id'])




def on_connect(client, userdata, flags, rc):
    logging.info('Connected to host, subscribing to uplink messages')
    client.subscribe('+/devices/+/up')

def on_message(client, userdata, msg):
    logging.debug('Received message %s', str(msg.payload))

    try:
        msg_as_string = msg.payload.decode('utf8')
        msg_obj = json.loads(msg_as_string)
        payload = base64.b64decode(msg_obj.get('payload_raw', ''))
    # python2 uses ValueError and perhaps others, python3 uses JSONDecodeError
    # pylint: disable=broad-except
    except Exception as ex:
        logging.warning('Error parsing JSON payload')
        logging.warning(ex)
        return

    try:
        process_data(msg_obj, payload)
    # pylint: disable=broad-except
    except Exception as ex:
        logging.warning('Error processing packet')
        logging.warning(ex)
        traceback.print_tb(e.__traceback__)

# Maps station ids to the last frame counter seen
last_counter_seen = {}

def process_data(msg_obj, payload):
    stream = bitstring.ConstBitStream(bytes=payload)

    node_config = {'item_type': 'node'}
    config = [
        node_config,
        {
            'item_type': 'channel',
            'channel_id': 0,
            'quantity': 'latitude',
            'unit': 'degrees',
            'divider': 32768,
        },
        {
            'item_type': 'channel',
            'channel_id': 1,
            'quantity': 'longitude',
            'unit': 'degree',
            'divider': 32768,
        },
        {
            'item_type': 'channel',
            'channel_id': 2,
            'quantity': 'temperature',
            'unit': 'degree_celsius',
            'divider': 16,
        },
        {
            'item_type': 'channel',
            'channel_id': 3,
            'quantity': 'humidity',
            'unit': 'percent_rh',
            'divider': 16,
        },
    ]
    vcc_config = {
        'item_type': 'channel',
        'channel_id': 4,
        'quantity': 'voltage',
        'unit': 'volt',
        'measured': 'supply',
        'divider': 100,
        'offset': 1,
    }
    battery_config = {
        'item_type': 'channel',
        'channel_id': 5,
        'quantity': 'voltage',
        'unit': 'volt',
        'measured': 'battery',
        'divider': 50,
        'offset': 1,
    }
    lux_config = {
        'item_type': 'channel',
        'channel_id': 6,
        'quantity': 'ambient_light',
        'unit': 'lux',
    }
    pm25_config = {
        'item_type': 'channel',
        'channel_id': 7,
        'quantity': 'particulate_matter',
        'unit': 'ug_per_cubic_meter',
        'measured:size': 2.5,
    }
    pm10_config = {
        'item_type': 'channel',
        'channel_id': 8,
        'quantity': 'particulate_matter',
        'unit': 'ug_per_cubic_meter',
        'measured:size': 10,
    }

    data = []

    port = msg_obj["port"]
    length = len(payload)
    if port == 10:
        # Legacy packet
        if length < 9 or length > 11:
            logging.warning('Invalid packet received on port %s with length %s', port, length)
            return
    elif port == 11:
        # Packet without lux, with or without 1 byte battery measurement, with
        # or without 4-byte particulate matter
        if length < 11 or 12 < length < 15 or length > 16:
            logging.warning('Invalid packet received on port %s with length %s', port, length)
            return
    elif port == 12:
        # Packet with 2-byte lux, with or without 1 byte battery measurement, with or
        # without 4-byte particulate matter
        if length < 13 or 14 < length < 17 or length > 18:
            logging.warning('Invalid packet received on port %s with length %s', port, length)
            return
    else:
        logging.warning('Ignoring message with unknown port: %s', port)
        return

    if port != 10:
        node_config['firmware_version'] = stream.read('uint:8')

    # Latitude
    data.append({
        'channel_id': 0,
        'value': stream.read('int:24'),
    })

    # Longitude
    data.append({
        'channel_id': 1,
        'value': stream.read('int:24'),
    })

    # Temperature
    data.append({
        'channel_id': 2,
        'value': stream.read('int:12'),
    })

    # Humidity
    data.append({
        'channel_id': 3,
        'value': stream.read('int:12'),
    })

    if port >= 11 or len(stream) - stream.bitpos >= 8:
        config.append(vcc_config)
        data.append({
            'channel_id': 4,
            'value': stream.read('uint:8'),
        })

    if port == 12:
        config.append(lux_config)
        data.append({
            'channel_id': 6,
            'value': stream.read('uint:16'),
        })

    if len(stream) - stream.bitpos >= 32:
        config.append(pm25_config)
        data.append({
            'channel_id': 7,
            'value': stream.read('uint:16'),
        })
        config.append(pm10_config)
        data.append({
            'channel_id': 8,
            'value': stream.read('uint:16'),
        })

    if len(stream) - stream.bitpos >= 8:
        config.append(battery_config)
        data.append({
            'channel_id': 5,
            'value': stream.read('uint:8'),
        })


    node_id = make_ttn_node_id(msg_obj)
    msg_counter = msg_obj['counter']

    generate_config = False
    try:
        last_counter = last_counter_seen[node_id]
        # If the node was rebooted, simulate a new config
        if last_counter < msg_counter:
            generate_config = True
    except KeyError:
        generate_config = True

    last_counter_seen[node_id] = msg_counter

    if generate_config:
        logging.debug("Generated config payload (before shortening): %s", config)
        def encode(item):
            return encode_cbor_obj(item, CONFIG_PACKET_KEYS_INVERTED, CONFIG_PACKET_VALUES_INVERTED)
        config = list(map(encode, config))
        logging.debug("Generated config payload (after shortening): %s", config)
        produce_message(msg_obj, config, CONFIG_PORT)

    logging.debug("Generated data payload: %s", data)
    produce_message(msg_obj, data, DATA_PORT)

def produce_message(msg_obj, payload, port):
    msg_obj['port'] = port
    payload_cbor = cbor2.dumps(payload)
    msg_obj['payload_raw'] = base64.b64encode(payload_cbor).decode('utf8')

    msg_as_string = json.dumps(msg_obj)
    msg_as_bytes = msg_as_string.encode('utf8')

    logging.debug("Producing new message: %s", msg_as_string)
    kafka_producer.produce(kafka_topic, msg_as_bytes)

logging.basicConfig(level=logging.DEBUG)

def get_env_or_file(name, default=None):
    try:
        return os.environ[name]
    except KeyError:
        try:
            filename = os.environ[name + '_FILE']
            with open(filename) as env_file:
                return env_file.read()
        except KeyError:
            if default is None:
                raise KeyError(name)
            return default

def main():
    global kafka_producer, kafka_topic

    kafka_broker = os.environ['KAFKA_BROKER']
    kafka_topic = os.environ['KAFKA_TOPIC']
    app_id = os.environ.get('TTN_CONVERT_APP_ID')
    access_key = get_env_or_file('TTN_CONVERT_ACCESS_KEY')
    ttn_host = os.environ.get('TTN_HOST', 'eu.thethings.network')
    ca_cert_path = os.environ.get('TTN_CA_CERT_PATH', 'mqtt-ca.pem')
    ttn_port = 8883

    logging.info('Connecting Kafka to %s', kafka_broker)
    kafka_producer = Producer({'bootstrap.servers': kafka_broker})

    logging.info('Connecting MQTT to %s on port %s', ttn_host, ttn_port)
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(app_id, password=access_key)
    client.tls_set(ca_cert_path)
    client.connect(ttn_host, port=ttn_port)
    client.loop_forever()

main()

# vim: set sw=4 sts=4 expandtab:
