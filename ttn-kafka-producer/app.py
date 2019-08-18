#!/usr/bin/env python3
# vim:fileencoding=utf8
# pylint: disable=missing-docstring

import logging
import os
import paho.mqtt.client as mqtt

from confluent_kafka import Producer

def on_connect(client, userdata, flags, rc):
    logging.info('Connected to host, subscribing to uplink messages')
    client.subscribe('+/devices/+/up')

def on_message(client, userdata, msg):
    logging.debug('Received message %s', str(msg.payload))

    kafka_producer.produce(kafka_topic, msg.payload)



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
    app_id = os.environ.get('TTN_APP_ID')
    access_key = get_env_or_file('TTN_ACCESS_KEY')
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
