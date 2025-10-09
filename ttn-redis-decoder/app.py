#!/usr/bin/env python3
# vim:fileencoding=utf8
# pylint: disable=missing-docstring
import base64
import itertools
import json
import logging
import os
import uuid
from urllib.parse import urlparse

import cbor2
import redis
from iso8601 import parse_date
from pony import orm

import db
import consys

def delete_if_exists(entity, **kwargs):
    # This runs a DELETE query without creating an instance. This bypasses the
    # cache, which could be problematic if the instance would already have been
    # loaded into the cache, but in practice there were actually problems with
    # calling delete() on an instance not deleting it from the cache (so a
    # subsequent insert would fail).
    num = entity.select().where(**kwargs).delete(bulk=True)
    if num:
        logging.info("Deleted previous %s %s", entity.__name__, kwargs)


@orm.db_session
def process_message(entry_id, message):
    payload = message[b'payload']
    timestamp = parse_date(message[b'timestamp'].decode('utf8'))
    # First thing, secure the message in the rawest form
    delete_if_exists(db.RawMessage, src="ttn", src_id=entry_id)
    raw_msg = db.RawMessage(
        src="ttn",
        # TTN does not assign ids, so use the id assigned by redis then
        # TODO: Use unique_id from TTN? Shown in TTN console, but not in
        # MQTT JSON... Also use it for msg_id.
        src_id=entry_id,
        received_from_src=timestamp,
        raw=payload,
    )
    orm.commit()

    # Then, actually decode the message
    try:
        msg_as_string = payload.decode("utf8")
        logging.debug("Received message %s: %s", entry_id, msg_as_string)
        msg_obj = json.loads(msg_as_string)
        payload = base64.b64decode(msg_obj.get('uplink_message').get('frm_payload', ''))
    except json.JSONDecodeError as ex:
        logging.warning("Error parsing JSON payload")
        logging.warning(ex)
        return

    # Store the "decoded" JSON version, which is a bit more readable for debugging
    raw_msg.decoded = msg_obj
    orm.commit()

    try:
        decode_message(raw_msg, msg_obj, payload)
    # pylint: disable=broad-except
    except Exception as ex:
        logging.exception("Error processing packet: %s", ex)
        return


def decode_message(raw_msg, msg, payload):
    port = msg["uplink_message"].get("f_port", 0)
    if port == 1:
        obj = decode_config_message(raw_msg, msg, payload)
        process_config_message(obj)
    elif port == 2:
        obj = decode_data_message(raw_msg, msg, payload)
        process_data_message(obj)
    else:
        logging.warning("Ignoring message with unknown port: %s", port)
    return None


def process_config_message(obj: db.Config):
    system_urn = "{}:system:{}".format(output.urn_root, obj.node_id)

    procedure_uid = uuid.uuid4().urn

    procedure_fields = ""

    for chan_id, channel in obj.data["channel_config"].items():
        quantity_url = channel["quantity"]
        # TODO: Explicitly specify name in node?
        name = os.path.basename(urlparse(quantity_url).path)
        uom = channel["unit"]
        procedure_fields += f"""
        <swe:field name="{name}">
           <swe:Quantity definition="{quantity_url}">
              <swe:uom code="{uom}"/>
           </swe:Quantity>
        </swe:field>
        """

    # TODO: This seems to give a non-descript 400 error
    procedure = f"""<?xml version="1.0" encoding="UTF-8"?>
    <sml:PhysicalSystem gml:id="MY_WEATHER_STATION"
       xmlns:sml="http://www.opengis.net/sensorml/2.0"
       xmlns:swe="http://www.opengis.net/swe/2.0"
       xmlns:gml="http://www.opengis.net/gml/3.2"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xmlns:xlink="http://www.w3.org/1999/xlink"
       xsi:schemaLocation="http://www.opengis.net/sensorml/2.0 http://schemas.opengis.net/sensorml/2.0/sensorML.xsd">
       <!-- ================================================= -->
       <!--                  System Description               -->
       <!-- ================================================= -->
       <!-- <gml:description>TODO</gml:description> -->
       <gml:identifier codeSpace="uniqueID">{procedure_uid}</gml:identifier>
       <gml:name>TODO</gml:name>

        <!-- TODO: Examples define observed properties as inputs, do we need that? -->
        <!-- TODO: Revisit all definition attributes, want ontologies to use? -->

       <sml:identification>
         <sml:IdentifierList>
            <sml:identifier>
              <sml:Term definition="http://www.opengis.net/def/ogc/PlatformType">
                <sml:label>Platform Type</sml:label>
                  <!-- TODO: Unhardcode -->
                  <sml:value>mjs2020</sml:value>
                </sml:Term>
            </sml:identifier>
         </sml:IdentifierList>
       </sml:identification>


       <sml:outputs>
          <sml:OutputList>
             <sml:output name="data">
                <swe:DataRecord>
                   {procedure_fields}
                </swe:DataRecord>
             </sml:output>
          </sml:OutputList>
       </sml:outputs>
    </sml:PhysicalSystem>
    """

    procedure_path = output.create_procedure(content=procedure, content_type="application/sml+xml")

    system = {
        "type": "PhysicalSystem",
        # "id": "abcd", # Ignored by OSH?
        "definition": "http://www.w3.org/ns/sosa/Sensor",
        "uniqueId": system_urn,
        "label": obj.node_id,
        "description": "TODO",
        "typeOf": {
            "href": output.url + procedure_path,
            "uid": procedure_uid,
            "type": "application/sml+json",
        },
    }

    system_path = output.create_system(content=system, content_type="application/sml+json")

    fields = []
    field_names = {}
    for chan_id, channel in obj.data["channel_config"].items():
        quantity_url = channel["quantity"]
        # TODO: Explicitly specify name in node?
        name = os.path.basename(urlparse(quantity_url).path)
        fields.append({
            "type": "Quantity",
            "name": name,
            "definition": quantity_url,
            "label": "TODO",
            "description": "TODO",
            # TODO: Units can also be a href
            "uom": {
                "code": channel["unit"],
            },
        })
        field_names[chan_id] = name

    datastream = {
        "name": obj.node_id,
        "description": "TODO",
        # "ultimateFeatureOfInterest@link": {
        #     "href": "https://data.example.org/api/collections/buildings/items/754",
        #     "title": "My House"
        # },
        # "samplingFeature@link": {
        #     "href": "https://data.example.org/api/samplingFeatures/4478",
        #     "title": "Thermometer Sampling Point"
        # },
        "outputName": "data",
        # Note: Schema property is write-only, so queries must use the
        # /schema nested endpoint.
        "schema": {
            "obsFormat": "application/om+json",
            "resultTimeSchema": {
                "name": "time",
                "type": "Time",
                "definition": "http://www.opengis.net/def/property/OGC/0/SamplingTime",
                "referenceFrame": "http://www.opengis.net/def/trs/BIPM/0/UTC",
                "uom": {
                    "href": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"
                }
            },
            "resultSchema": {
                "type": "DataRecord",
                "fields": fields,
            }
        }
    }

    datastream_path = output.create_datastream(system_path, content=datastream, content_type="application/json")

    prefix = "/datastreams/"
    assert datastream_path.startswith(prefix)
    datastream_id = datastream_path[len(prefix):]

    obj.datastream_id = datastream_id
    obj.field_names = field_names
    # TODO: This is out of place, might commit other stuff, etc.
    orm.commit()


def process_data_message(obj: db.Bundle):
    observation = {
        "resultTime": obj.timestamp.isoformat(),
        "phenomenonTime": obj.timestamp.isoformat(),
        "result": {},
    }

    for channel in obj.data.values():
        name = obj.config.field_names[str(channel['channel_id'])]
        observation['result'][name] = channel["value"]
    # TODO: Check if all fields are present? OSH rejects the
    # observation otherwise

    output.create_observation(
        f"/datastreams/{obj.config.datastream_id}",
        content=observation, content_type="application/om+json",
    )


def make_ttn_node_id(msg):
    return "ttn/{}/{}".format(
        msg["end_device_ids"]["application_ids"]["application_id"],
        msg["end_device_ids"]["device_id"])


def make_msg_id(node_id, msg):
    return "{}/{}".format(node_id, msg["received_at"])


def make_meas_id(msg_id, chan_id):
    return "{}/{}".format(msg_id, chan_id)


def decode_config_message(raw_msg, msg, payload):
    entries = decode_packet(payload, CONFIG_PACKET_KEYS, CONFIG_PACKET_VALUES)
    logging.debug("Decoded config entries: %s", entries)
    config_entries = decode_config_entries(entries)

    node_id = make_ttn_node_id(msg)
    msg_id = make_msg_id(node_id, msg)

    delete_if_exists(db.Config, message_id=msg_id)
    config = db.Config(
        message_id=msg_id,
        node_id=node_id,
        timestamp=parse_date(msg["received_at"]),
        data=config_entries,
        src=raw_msg,
    )

    logging.debug("Decoded config: %s", config)

    return config


def decode_packet(payload, keys, values):
    packet = cbor2.loads(payload)
    if not isinstance(packet, list):
        logging.warning("Config packet is not list: %s", packet)

    def decode(obj):
        return decode_cbor_obj(obj, keys, values)

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


def decode_data_message(raw_msg, msg, payload):
    # TODO Decode shortcuts
    entries = decode_packet(payload, DATA_PACKET_KEYS, DATA_PACKET_VALUES)
    logging.debug("Decoded data entries: %s", entries)

    node_id = make_ttn_node_id(msg)
    msg_id = make_msg_id(node_id, msg)
    timestamp = parse_date(msg["received_at"])

    config = (
        db.Config.select(lambda c: c.node_id == node_id)
        .where(lambda c: c.timestamp <= timestamp)
        .order_by(orm.desc(db.Config.timestamp))
        .first()
    )
    logging.debug("Found relevant config: %s: %s", config, config.to_dict())

    if not config:
        logging.warning("Found no relevant config, returning")
        return

    channels = decode_data_entries(entries, config)
    logging.debug("Decoded data: %s", channels)

    delete_if_exists(db.Bundle, message_id=msg_id)
    bundle = db.Bundle(
        config=config,
        message_id=msg_id,
        node_id=node_id,
        timestamp=timestamp,
        data=channels,
        src=raw_msg,
    )
    orm.commit()

    logging.debug("Decoded data: %s: %s", bundle, bundle.to_dict())

    for name, data in channels.items():
        chan_id = data["channel_id"]
        meas_id = make_meas_id(msg_id, chan_id)

        delete_if_exists(db.Measurement, meas_id=meas_id)
        measurement = db.Measurement(
            meas_id=meas_id,
            config=config,
            bundle=bundle,
            node_id=node_id,
            channel_id=chan_id,
            timestamp=timestamp,
            data=data,
        )

        logging.debug("Decoded single data: %s", measurement)

    return bundle


def decode_data_entries(entries, config: db.Config):
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

    def decode(value):
        return value / divider + offset

    if isinstance(data["value"], list):
        data["value"] = [decode(v) for v in data["value"]]
    else:
        data["value"] = decode(data["value"])

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
        1: "https://qudt.org/vocab/quantitykind/Temperature",
        2: "https://qudt.org/vocab/quantitykind/RelativeHumidity",
        3: "https://qudt.org/vocab/quantitykind/Voltage",
        4: "https://qudt.org/vocab/quantitykind/Illuminance",
        # TODO: Not present in QUDT?
        5: "particulate_matter",
        6: "position",
    },
    "unit": {
        1: "Cel",
        # TODO: Sketch code defines this as PercentRelativeHumidity, but
        # OSH does not accept %{rh}
        2: "%",
        3: "V",
        4: "ug/m3",
        5: "lx",
        6: "deg",
    },
    "sensor": {1: "Si2701"},
    "item_type": {1: "node", 2: "channel"},
}

DATA_PACKET_KEYS = {
    1: "channel_id",
    2: "value",
}

DATA_PACKET_VALUES = {
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


def main():
    logging.basicConfig(level=logging.DEBUG)

    database_url = urlparse(os.environ["DATABASE_URL"])
    redis_url = urlparse(os.environ["REDIS_URL"])

    redis_stream = os.environ["REDIS_STREAM"]

    logging.info(
        "Connecting Redis to {} on port {}".format(redis_url.hostname, redis_url.port)
    )
    redis_server = redis.Redis(
        host=redis_url.hostname, port=redis_url.port, db=int(redis_url.path[1:] or 0)
    )

    global output
    output = consys.ConnectedSystems(os.environ["CONSYS_URL"])

    db.init(database_url)

    messages_from = "0"
    while True:
        for stream_name, messages in redis_server.xread(
                {redis_stream: messages_from}, block=60 * 1000
        ):
            for entry_id, message in messages:
                messages_from = entry_id.decode("utf-8")
                try:
                    process_message(entry_id.decode("utf-8"), message)
                    # When successful, remove from the stream
                    redis_server.xdel(stream_name, entry_id)
                # pylint: disable=broad-except
                except Exception as ex:
                    logging.exception("Error processing message: %s", ex)


if __name__ == "__main__":
    main()

# vim: set sw=4 sts=4 expandtab:
