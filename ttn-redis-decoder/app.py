#!/usr/bin/env python3
# vim:fileencoding=utf8
# pylint: disable=missing-docstring
import base64
import itertools
import json
import logging
import os
from datetime import datetime
from urllib.parse import urlparse

import cbor2
import elasticsearch
import redis
from iso8601 import parse_date
from pony import orm
from pony.orm import desc, max

database_url = urlparse(os.environ["DATABASE_URL"])
redis_url = urlparse(os.environ["REDIS_URL"])

db = orm.Database()
db.bind(
    provider=database_url.scheme,
    user=database_url.username,
    password=database_url.password,
    host=database_url.hostname,
    port=database_url.port,
    database=database_url.path[1:],
)

# Below, datetime types specify the sql_type explicitly, to ensure timezone
# information is stored along with the timestamps. See also
# https://github.com/ponyorm/pony/issues/434

class Config(db.Entity):
    message_id = orm.PrimaryKey(str)
    node_id = orm.Required(str)
    timestamp = orm.Required(datetime, sql_type='TIMESTAMP WITH TIME ZONE')

    entry_id = orm.Required(str)  # Redis entry id

    data = orm.Required(orm.Json)

    bundles = orm.Set("Bundle")
    measurements = orm.Set("Measurement")


class Bundle(db.Entity):
    config = orm.Required(Config)
    message_id = orm.PrimaryKey(str)
    node_id = orm.Required(str)
    timestamp = orm.Required(datetime, sql_type='TIMESTAMP WITH TIME ZONE')

    entry_id = orm.Required(str)  # Redis entry id

    data = orm.Required(orm.Json)

    measurements = orm.Set("Measurement")


class Measurement(db.Entity):
    bundle = orm.Required(Bundle)
    config = orm.Required(Config)

    node_id = orm.Required(str)
    timestamp = orm.Required(datetime, sql_type='TIMESTAMP WITH TIME ZONE')

    data = orm.Required(orm.Json)


db.generate_mapping(create_tables=True)


def process_message(entry_id, message):
    try:
        msg_as_string = message.decode("utf8")
        logging.debug("Received message %s: %s", entry_id, msg_as_string)
        msg_obj = json.loads(msg_as_string)
        payload = base64.b64decode(msg_obj.get("payload_raw", ""))
    except json.JSONDecodeError as ex:
        logging.warning("Error parsing JSON payload")
        logging.warning(ex)
        return None

    try:
        decoded = decode_message(entry_id, msg_obj, payload)
    # pylint: disable=broad-except
    except Exception as ex:
        logging.exception("Error processing packet: %s", ex)
        return None

    if decoded is None:
        return None


def decode_message(entry_id, msg, payload):
    port = msg["port"]
    if port == 1:
        return decode_config_message(entry_id, msg, payload)
    if port == 2:
        return decode_data_message(entry_id, msg, payload)
    logging.warning("Ignoring message with unknown port: %s", port)
    return None


def make_ttn_node_id(msg):
    return "ttn/{}/{}".format(msg["app_id"], msg["dev_id"])


def make_msg_id(node_id, msg):
    return "{}/{}".format(node_id, msg["metadata"]["time"])


def make_meas_id(msg_id, chan_id):
    return "{}/{}".format(msg_id, chan_id)


@orm.db_session
def decode_config_message(entry_id, msg, payload):
    entries = decode_config_packet(payload)
    logging.debug("Decoded config entries: %s", entries)
    config_entries = decode_config_entries(entries)

    node_id = make_ttn_node_id(msg)
    msg_id = make_msg_id(node_id, msg)

    # HACK: Elasticsearch breaks if a field is sometimes a timestamp and
    # sometimes the empty string, so remove empty time fields for now...
    for gw_data in msg.get("metadata", {}).get("gateways", []):
        if "time" in gw_data and not gw_data["time"]:
            gw_data.pop("time")

    config = Config(
        message_id=msg_id,
        node_id=node_id,
        entry_id=entry_id,
        timestamp=parse_date(msg["metadata"]["time"]),
        data=config_entries,
    )

    logging.debug("Decoded config: %s", config)

    if es:
        body = {
            "node_id": node_id,
            "timestamp": msg["metadata"]["time"],
            # TODO: Should this be a reference?
            "sources": {"ttn": msg},
        }
        body.update(config_entries)
        es.index(index="config", id=msg_id, body=body)

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
            item = data.pop("item_type")
            if item == "node":
                node.update(data)
            elif item == "channel":
                chan_id = data.pop("channel_id")
                if chan_id in channels:
                    logging.warning(
                        "Duplicate channel entry in config message: %s", entry
                    )
                else:
                    # Convert id to string, since mongo can only do string keys
                    channels[str(chan_id)] = data
            else:
                logging.warning("Unknown entry type in config message: %s", entry)
        except KeyError as ex:
            logging.warning(
                "Invalid config message entry (missing %s): %s", ex.args, entry
            )

    message = {"node_config": node, "channel_config": channels}
    return message


@orm.db_session
def decode_data_message(entry_id, msg, payload):
    # TODO Decode shortcuts
    entries = cbor2.loads(payload)
    logging.debug("Decoded data entries: %s", entries)

    node_id = make_ttn_node_id(msg)
    msg_id = make_msg_id(node_id, msg)
    timestamp = msg["metadata"]["time"]

    config = (
        Config.select(lambda c: c.node_id == node_id)
            .order_by(orm.desc(Config.timestamp))
            .first()
    )
    logging.debug("Found relevant config: %s", config)

    if not config:
        logging.warning("Found no relevant config, returning")
        return

    channels = decode_data_entries(entries, config)
    logging.debug("Decoded data: %s", channels)

    # HACK: Elasticsearch breaks if a field is sometimes a timestamp and
    # sometimes the empty string, so remove empty time fields for now...
    for gw_data in msg.get("metadata", {}).get("gateways", []):
        if "time" in gw_data and not gw_data["time"]:
            gw_data.pop("time")

    bundle = Bundle(
        config=config,
        message_id=msg_id,
        node_id=node_id,
        entry_id=entry_id,
        timestamp=parse_date(timestamp),
        data=channels,
    )
    orm.commit()

    logging.debug("Decoded data: %s", bundle)

    if es:
        body = {
            "node_id": node_id,
            "timestamp": timestamp,
            "config_id": config["_id"] if config else None,
            "channels": channels,
        }
        es.index(index="data", id=msg_id, body=body)

    for chan_id, data in channels.items():
        meas_id = make_meas_id(msg_id, chan_id)

        measurement = Measurement(
            config=config,
            bundle=bundle,
            node_id=node_id,
            timestamp=parse_date(timestamp),
            data=data,
        )

        logging.debug("Decoded single data: %s", measurement)

        if es:
            body = {
                "node_id": node_id,
                "timestamp": msg["metadata"]["time"],
                "config_id": config["_id"] if config else None,
                "channel_id": chan_id,
                "data": data,
            }
            es.index(index="data_single", id=meas_id, body=body)

    return bundle


def decode_data_entries(entries, config: Config):
    channels = {}

    for entry in entries:
        chan_data = dict(entry)
        try:
            chan_id = chan_data["channel_id"]
        except KeyError as ex:
            logging.warning(
                "Invalid config message entry (missing %s): %s", ex.args, entry
            )
        else:
            if chan_id in channels:
                logging.warning(
                    "Duplicate channel %s in data message: %s", chan_id, entry
                )
                continue

            try:
                chan_config = config.data["channel_config"][str(chan_id)]
            except KeyError:
                logging.warning("Missing config for channel %s: %s", chan_id, entry)
                # Still pass the data along untouched
                data = chan_data
            else:
                data = decode_data_entry(chan_data, chan_config)

            name = data.get("quantity", str(chan_id))

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

    # TODO: Should we leave these? Or convert them somehow to preserve
    # information about granularity?
    divider = config.pop("divider", 1)
    offset = config.pop("offset", 0)

    data["value"] = data["value"] / divider + offset

    # Add any remaining config keys to the data
    data.update(config)
    return data


# TODO: Write script to convert below values to a reverse mapping usable in the
# C++ code.
CONFIG_PACKET_KEYS = {
    1: "channel_id",
    2: "quantity",
    3: "unit",
    4: "sensor",
    5: "item_type",
    6: "measured",
    7: "divider",
}

CONFIG_PACKET_VALUES = {
    "quantity": {
        1: "temperature",
        2: "humidity",
        3: "voltage",
        4: "ambient_light",
        5: "latitude",
        6: "longitude",
        7: "particulate_matter",
    },
    "unit": {
        # TODO: How to note these? Perhaps just '°C'?
        1: "degree_celcius",
        2: "percent_rh",
        3: "volt",
        4: "ug_per_cubic_meter",
        5: "lux",
        6: "degrees",
    },
    "sensor": {1: "Si2701"},
    "item_type": {1: "node", 2: "channel"},
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
                logging.warning("Unknown integer key in packet: %s=%s", key, value)
        if isinstance(value, int):
            values_for_this_key = values.get(key, False)
            if values_for_this_key:
                try:
                    value = values_for_this_key[value]
                except KeyError:
                    # TODO: Store warnings in output?
                    logging.warning(
                        "Unknown integer value in packet: %s=%s", key, value
                    )
        out[key] = value
    return out


@orm.db_session
def get_last_entry_id():
    return max(b.entry_id for b in Bundle) or "$"


def main():
    global es

    logging.basicConfig(level=logging.DEBUG)

    redis_stream = os.environ["REDIS_STREAM"]

    logging.info(
        "Connecting Redis to {} on port {}".format(redis_url.hostname, redis_url.port)
    )
    redis_server = redis.Redis(
        host=redis_url.hostname, port=redis_url.port, db=int(redis_url.path[1:] or 0)
    )

    elastic_host = os.environ["ELASTIC_HOST"]
    if elastic_host:
        logging.info("Connecting Elasticsearch to %s", elastic_host)
        es = elasticsearch.Elasticsearch(elastic_host)
    else:
        es = None

    from_entry_id = get_last_entry_id()
    while True:
        for stream_name, messages in redis_server.xread(
                {redis_stream: from_entry_id}, block=60 * 1000
        ):
            for entry_id, message in messages:
                from_entry_id = entry_id.decode("utf-8")
                try:
                    process_message(entry_id.decode("utf-8"), message[b"payload"])
                # pylint: disable=broad-except
                except Exception as ex:
                    logging.exception("Error processing message: %s", ex)


main()

# vim: set sw=4 sts=4 expandtab:
