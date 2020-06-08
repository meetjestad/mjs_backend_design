#!/usr/bin/env python3
# vim:fileencoding=utf8
# pylint: disable=missing-docstring

from datetime import datetime, timezone
import logging
import os
from urllib.parse import urlparse

import paho.mqtt.client as mqtt
import redis


def get_env_or_file(name, default=None):
    try:
        return os.environ[name]
    except KeyError:
        try:
            filename = os.environ[name + "_FILE"]
            with open(filename) as env_file:
                return env_file.read()
        except KeyError:
            if default is None:
                raise KeyError(name)
            return default


def main():
    def on_connect(client, userdata, flags, rc):
        logging.info("Connected to host, subscribing to uplink messages")
        client.subscribe("+/devices/+/up")

    def on_message(client, userdata, msg):
        logging.debug("Received message %s", str(msg.payload))

        try:
            redis_server.xadd(redis_stream, {
                "payload": msg.payload,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
        except Exception as e:
            logging.error("Inserting into Redis failed")
            logging.error(e)

    logging.basicConfig(level=logging.DEBUG)

    redis_stream = os.environ["REDIS_STREAM"]
    app_id = os.environ.get("TTN_APP_ID")
    access_key = get_env_or_file("TTN_ACCESS_KEY")
    ttn_host = os.environ.get("TTN_HOST", "eu.thethings.network")
    ca_cert_path = os.environ.get("TTN_CA_CERT_PATH", "mqtt-ca.pem")
    ttn_port = 8883

    redis_url = urlparse(os.environ["REDIS_URL"])
    logging.info(
        "Connecting Redis to {} on port {}".format(redis_url.hostname, redis_url.port)
    )
    redis_server = redis.Redis(
        host=redis_url.hostname, port=redis_url.port, db=int(redis_url.path[1:] or 0)
    )

    logging.info("Connecting MQTT to {} on port {}".format(ttn_host, ttn_port))
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.username_pw_set(app_id, password=access_key)
    mqtt_client.tls_set(ca_cert_path)
    mqtt_client.connect(ttn_host, port=ttn_port)
    mqtt_client.loop_forever()


main()

# vim: set sw=4 sts=4 expandtab:
