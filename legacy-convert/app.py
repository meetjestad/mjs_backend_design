#!/usr/bin/env python3
# vim:fileencoding=utf8
# pylint: disable=missing-docstring
import base64
import copy
import json
import logging
import os
import signal
import sys
from urllib.parse import urlparse
import uuid

import bitstring
import deepdiff
import redis
from iso8601 import parse_date
from pony import orm

import db
import sensorthings
from sensorthings import QOperator, QOp, QLiteral, QField

database_url = urlparse(os.environ["DATABASE_URL"])
redis_url = urlparse(os.environ["REDIS_URL"])
redis_stream_in = os.environ["REDIS_STREAM_IN"]
redis_consumer_group = os.environ["REDIS_CONSUMER_GROUP"]

db.init(database_url)


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
def process_message(sta, entry_id, message):
    ttn_msg = message['raw']
    topic = message['src_stream']
    timestamp = message['received_from_src']

    try:
        logging.debug("Received message %s: %s", entry_id, ttn_msg)
        msg_obj = json.loads(ttn_msg)
    except json.JSONDecodeError as ex:
        # TODO: Signal somewhere
        logging.exception("Error parsing JSON payload", ex)
        return

    if not topic.endswith("/up"):
        logging.info("Not uplink, skipping")
        return

    device_id = msg_obj["end_device_ids"]["device_id"]
    payload = base64.b64decode(msg_obj.get('uplink_message', {}).get('frm_payload', ''))
    port = msg_obj.get('uplink_message', {}).get("f_port", 0)

    try:
        decode_uplink(sta, msg_obj, device_id, port, payload)
    except Exception:
        logging.error("Failed to process message from %s at %s: %s", device_id, timestamp, msg_obj)
        raise


def decode_uplink(sta, msg_obj, device_id, port, payload):
    stream = bitstring.ConstBitStream(bytes=payload)

    l = len(payload)
    have_supply = False
    have_battery = False
    have_firmware = False
    have_lux = False
    have_pm = False
    have_extra = False
    lux_scale_bits = 0
    if port == 10:
        # Legacy packet without firmware_version, with or without supply
        # and battery
        if l == 9:
            pass
        elif l == 10:
            have_supply = True
        elif l == 11:
            have_supply = True
            have_battery = True
        else:
            logging.warning('Invalid packet received on port {} with length {}'.format(port, l))
            return
    elif port == 11:
        # Packet without lux, with or without 1 byte battery measurement, with
        # or without 4-byte particulate matter
        have_firmware = True
        have_supply = True
        if l == 11:
            pass
        elif l == 12:
            have_battery = True
        elif l == 15:
            have_pm = True
        elif l == 16:
            have_battery = True
            have_pm = True
        else:
            logging.warning('Invalid packet received on port {} with length {}'.format(port, l))
            return
    elif port == 12:
        # Packet with 2-byte lux, with or without 1 byte battery measurement, with or
        # without 4-byte particulate matter
        have_firmware = True
        have_supply = True
        have_lux = True
        if l == 13:
            pass
        elif l == 14:
            have_battery = True
        elif l == 17:
            have_pm = True
        elif l == 18:
            have_battery = True
            have_pm = True
        else:
            logging.warning('Invalid packet received on port {} with length {}'.format(port, l))
            return
    elif port == 13:
        # Packet starting with a flag byte that indicates which of the
        # optional values are present.
        have_firmware = True
        have_supply = True
        have_lux = True
        have_lux = stream.read('bool')
        have_pm = stream.read('bool')
        have_battery = stream.read('bool')
        # 4 bits unused
        stream.read('uint:4')
        have_extra = stream.read('bool')
        # In this packet, the lux is scaled to allow larger values
        lux_scale_bits = 2
    else:
        logging.warning('Ignoring message with unknown port: {}'.format(port))
        return

    data = {}

    if have_firmware:
        data['firmware_version'] = stream.read('uint:8')

    data['latitude'] = stream.read('int:24') / 32768.0
    data['longitude'] = stream.read('int:24') / 32768.0
    data['temperature'] = stream.read('int:12') / 16.0
    data['humidity'] = stream.read('int:12') / 16.0

    if have_supply:
        data['supply'] = 1 + stream.read('uint:8') / 100.0

    if have_lux:
        data['lux'] = stream.read('uint:16') << lux_scale_bits

    if have_pm:
        data['pm2_5'] = stream.read('uint:16')
        data['pm10'] = stream.read('uint:16')

    if have_battery:
        data['battery'] = 1 + stream.read('uint:8') / 50.0

    if have_extra:
        # Extra values are ecoded as pairs of size and value, where size
        # is always 6 bits and the value is size+1 bits long.
        extra_value = []
        while stream.bitpos < len(stream):
            if len(stream) - stream.bitpos < 5:
                # This can happen due to rounding to whole bytes
                break
            # Add 1 to allow 1-32 bits rather than 0-31
            bits = stream.read('uint:5') + 1
            if len(stream) - stream.bitpos < bits:
                # This can happen due to rounding to whole bytes, in
                # which case the bits should be all-ones
                break
            value = stream.read(bits).uint
            extra_value.append(value)

        data["extra"] = extra_value
    else:
        data["extra"] = []

    if data["extra"]:
        process_extra(data)

    logging.debug("Decoded: %s", data)

    # TODO: Maybe only check when framecount lowered or a new session
    # was started?
    check_metadata = True

    thing = get_or_create_thing(sta, msg_obj, data, check_metadata)

    logging.debug("Found: %s", thing)

    create_observations(sta, thing, msg_obj, data)


def process_extra(device_id, data):
    # keep original for whatever reason
    extra = list(data.get("extra", []))
    firmware = data.get("firmware_version", None)

    def try_sps30(extra, data):
        # read SPS30(9 values), compatble with firmware v5, v6, v7.
        if len(extra) < 9:
            return

        if "pm2_5" not in data and all(x == 0 for x in extra[0:9]):
            # If no valid data was received from the sensor, the firmware omits the PM flag and values, but still
            # sends zeroes in the extra field.
            del extra[0:9]
            return

        # validate: compare already set pm2_5 and pm10,
        # they are not in float but in int, this might be +1 due to float to int casting on arduino.
        if data["pm2_5"] != int(extra[1] / 10) and data["pm2_5"] + 1 != int(extra[1] / 10):
            logging.warning(
                "try_sps30(): SPS30 Validation FAILED, pm2_5 value doesn't match! data: %s",
                data,
            )
            return

        if data["pm10"] != int(extra[3] / 10) and data["pm10"] + 1 != int(extra[3] / 10):
            logging.warning(
                "try_sps30(): SPS30 Validation FAILED, pm10 value doesn't match! data: %s",
                data,
            )
            return

        data["pm1"] = extra.pop() / 10
        data["pm2_5"] = extra.pop() / 10
        data["pm4"] = extra.pop() / 10
        data["pm10"] = extra.pop() / 10

        data["pn1"] = extra.pop() / 10
        data["pn2_5"] = extra.pop() / 10
        data["pn4"] = extra.pop() / 10
        data["pn10"] = extra.pop() / 10

        # typical particle size in micrometer.
        data["tps"] = extra.pop()

        data["is_sps30"] = True

    def try_vsolar(extra, data):
        # read Vsolar in mv, compatible with firmware v5, v6, v7.
        if len(extra) >= 1:
            data["vsolar"] = extra.pop() / 1000

    # firmware 5,6,7: extra = [ SPS30(9)? + VSOLAR(1)? ]
    # https://github.com/meetjestad/mjs_firmware/blob/v7/mjs_firmware.ino#L693-L714
    # https://github.com/meetjestad/mjs_firmware/blob/v6/mjs_firmware.ino#L693-L714
    # https://github.com/meetjestad/mjs_firmware/blob/v5/mjs_firmware.ino#L693-L714
    # extra may contain:
    # - 9 values for SPS30
    # - 1 value for vsolar
    if firmware in [5,6,7]:
        logging.debug("Extra firmware: %s (SPS30(9)? + VSOLAR(1)?): %s", firmware, extra)
        try_sps30(extra, data)
        try_vsolar(extra, data)
        if extra:
            logging.warning(
                "Extra unexpected unparsed data in extra: %s, firmware: %s",
                extra,
                firmware,
            )
    elif extra:
        logging.warning("%s: extra fields not parsed, unknown firmware: %s, extra %s", device_id, firmware, extra)


def create_observations(sta, thing, msg_obj, data):
    time = parse_date(msg_obj["received_at"])

    # lookup: map ObservedProperty/name to data dict_key
    lookup = {
        "Temperature": "temperature",
        "Humidity": "humidity",
        "Particulate matter PM1 density": "pm1",
        "Particulate matter PM2.5 density": "pm2_5",
        "Particulate matter PM4 density": "pm4",
        "Particulate matter PM10 density": "pm10",
        "Particulate matter PM1 count": "pn1",
        "Particulate matter PM2.5 count": "pn2_5",
        "Particulate matter PM4 count": "pn4",
        "Particulate matter PM10 count": "pn10",
        "Particulate matter typical particle size": "tps",
        "Illuminance": "lux",
        "Battery voltage": "battery",
        "Supply voltage": "supply",
        "Solar voltage": "vsolar",
    }

    for ds in thing["MultiDatastreams"]:
        values = []
        for prop in ds["ObservedProperties"]:
            data_name = lookup[prop["name"]]
            values.append(data[data_name])

        observation = {
            "result": values,
            "phenomenonTime": time.isoformat(),
            "resultTime": time.isoformat(),
            # TODO: Better go via the thing location, and/or GPS datastream,
            # but for now just store whatever location is in the data packet
            # directly.
            "FeatureOfInterest": get_or_create_feature_of_interest(sta, lat=data["latitude"], lon=data["longitude"]),
        }

        sta.create_observation('MultiDatastreams', ds["@iot.id"], observation)

    for ds in thing["Datastreams"]:
        prop = ds["ObservedProperty"]
        data_name = lookup[prop["name"]]
        try:
            value = data[data_name]
        except KeyError:
            logging.warning("Missing data for datastream: %s / %s", prop["name"], data_name)
            continue

        observation = {
            "result": value,
            "phenomenonTime": time.isoformat(),
            "resultTime": time.isoformat(),
            # TODO: Better go via the thing location, and/or GPS datastream,
            # but for now just store whatever location is in the data packet
            # directly.
            "FeatureOfInterest": get_or_create_feature_of_interest(sta, lat=data["latitude"], lon=data["longitude"]),
        }

        sta.create_observation('Datastreams', ds["@iot.id"], observation)


location_foi_cache = {}


def get_or_create_location_or_foi(sta, lat, lon, is_location):
    cache_key = (lat, lon, is_location)

    try:
        return location_foi_cache[cache_key]
    except KeyError:
        # Display (and round to) 6 digits of precision, which is
        # around 60*1852/1e6 = 11cm. We transmit in 15-bits fixed
        # point, so we've already rounded to 60*1852/2**15 = 339 cm
        # in transit.
        # 6 digits should also be short enough to not run into float
        # precision issues.
        # TODO: Maybe use Decimal instead of floats (at the place where
        # the packet is parsed already)?
        GPS_DIGITS = 6

        # Location and FeatureOfInterest have nearly the same schema
        key = "location" if is_location else "feature"
        path = "/Locations" if is_location else "/FeaturesOfInterest"

        # This uses the name to retrieve an existing object for the same
        # position. This is somewhat ugly, but does neatly sidestep
        # issues with floating point inequality issues, or deciding what
        # the threshold for equality using st_distance should be.
        #
        # TODO: Have some better way to merge multiple (nearby)
        # locations as well. Maybe also only merge locations / FOI per
        # thing?
        if lat and lon:
            name = f"{lat:.{GPS_DIGITS}f} / {lon:.{GPS_DIGITS}f}"
        else:
            name = "Unknown location"

        objs = sta.get_objects_filtered(
            path,
            filter=QOp(QField('name'), QOperator.Eq, QLiteral(name)),
        )

        if not objs:
            # Not found in cache and not found in db, create
            if lat and lon:
                geometry = {
                    "type": "Point",
                    "coordinates": [round(lon, GPS_DIGITS), round(lat, GPS_DIGITS)],
                }
            else:
                geometry = None

            data = {
                "name": name,
                "description": "",
                "encodingType": "application/geo+json",
                key: {
                    "type": "Feature",
                    "geometry": geometry,
                },
            }
            new_path = sta.create_object(path, content=data)
            obj = sta.get(path=new_path).json()
        elif len(objs) > 1:
            logging.warning(f"{name}: Multiple {path[1:]} with same name, using first one")
            obj = objs[0]
        else:
            obj = objs[0]

        result = {"@iot.id": obj["@iot.id"]}
        location_foi_cache[cache_key] = result
        return result


def get_or_create_location(sta, lat, lon):
    return get_or_create_location_or_foi(sta, lat=lat, lon=lon, is_location=True)


def get_or_create_feature_of_interest(sta, lat, lon):
    return get_or_create_location_or_foi(sta, lat=lat, lon=lon, is_location=False)


observed_property_cache = {}


def get_or_create_observed_property(sta, props):
    # Dicts are not hashable, so convert into a frozenset
    # https://stackoverflow.com/a/1600806/740048
    cache_key = frozenset(props.items())

    try:
        return observed_property_cache[cache_key]
    except KeyError:
        # TODO: This might select an existing object that has
        # *additional* properties not asked for, but maybe that is ok?
        # e.g. when (later) a station submits only a definition and not
        # a name, this will reuse a property with whatever name if it
        # already exists, which might be good?
        filter = None
        for key, value in props.items():
            op = QOp(QField(key), QOperator.Eq, QLiteral(value))
            if filter is None:
                filter = op
            else:
                filter = QOp(filter, QOperator.And, op)

        objs = sta.get_objects_filtered('/ObservedProperties', filter=filter)

        if not objs:
            new_path = sta.create_object('/ObservedProperties', content=props)
            obj = sta.get(path=new_path).json()
        elif len(objs) > 1:
            logging.warning(f"Multiple ObservedProperties with same values ({props}), using first one")
            obj = objs[0]
        else:
            obj = objs[0]

        result = {"@iot.id": obj["@iot.id"]}
        observed_property_cache[cache_key] = result
        return result

thing_cache = {}

def get_or_create_thing(sta, msg_obj, data, check_metadata):
    unique_id = make_thing_id(msg_obj)
    cache_key = unique_id

    try:
        thing = thing_cache[cache_key]
    except KeyError:
        expand = (
            "MultiDatastreams,MultiDatastreams/Sensor,MultiDatastreams/ObservedProperties,"
            + "Datastreams,Datastreams/Sensor,Datastreams/ObservedProperty"
        )

        objs = sta.get_objects_filtered(
            '/Things',
            filter=QOp(
                QOp(QField('properties/metadata/uniqueId'), QOperator.Eq, QLiteral(unique_id)),
                QOperator.And,
                QOp(QField('properties/metadata/validTime/1'), QOperator.Eq, QLiteral('now')),
            ),
            expand=expand,
        )

        if not objs:
            thing = None
        elif len(objs) > 1:
            logging.warning(f"{unique_id}: Multiple things with same uniqueId and open validTime found, using first one")
            thing = objs[0]
        else:
            thing = objs[0]

    new_thing = None
    if thing is None or check_metadata:
        new_thing = describe_thing(sta, unique_id, msg_obj, data)

    if thing is None:
        logging.info(f"{unique_id}: No Thing found")
    elif check_metadata:
        changes = thing_changed(thing, new_thing)
        if changes:
            logging.info(f"{unique_id}: Found Thing({thing['@iot.id']}), metadata changed, changes: {changes}")
        else:
            logging.debug(f"{unique_id}: Found Thing({thing['@iot.id']}), metadata unchanged")
            new_thing = None
    else:
        logging.debug(f"{unique_id}: Found Thing({thing['@iot.id']}), not checking metadata")

    if new_thing:
        new_path = sta.create_thing(new_thing)
        created = sta.get(path=new_path, params={"$expand": expand}).json()
        logging.info(f"{unique_id}: Created new Thing({created['@iot.id']})")
        # Update validTime en on old thing
        if thing:
            new_properties = copy.deepcopy(thing["properties"])
            new_properties["metadata"]["validTime"][1] = new_thing["properties"]["metadata"]["validTime"][0]
            sta.patch_thing(thing["@iot.id"], {"properties": new_properties})
        return created

    return thing


def thing_changed(old, new):
    # TODO: Also compare datastreams, sensors, observedproperties
    # http://localhost:8888/FROST-Server/v1.1/Things?$expand=MultiDatastreams,MultiDatastreams/Sensor,MultiDatastreams/ObservedProperties&$top=1
    return deepdiff.DeepDiff(
        old["properties"],
        new["properties"],
        exclude_paths=["root['metadata']['validTime']"],
    )


def describe_thing(sta, unique_id, msg_obj, data):
    """ Generate metadata for a thing based on a decoded data message. """

    name = msg_obj["end_device_ids"]["device_id"]
    desc = ""
    valid_from = parse_date(msg_obj["received_at"])
    station_num = int(msg_obj["end_device_ids"]["dev_eui"], 16)

    metadata = {
        "type": "PhysicalSystem",
        "definition": "http://www.w3.org/ns/sosa/System",
        "uniqueId": unique_id,
        "label": name,
        "validTime": [
            valid_from.isoformat(),
            "now"
        ],
        "identifiers": [
            {
                "definition": "http://sensorml.com/ont/swe/property/Manufacturer",
                "label": "Manufacturer Name",
                "value": "Meet je stad"
            },
        ],
        "characteristics": [
            {
                "definition": "http://sensorml.com/ont/swe/property/FirmwareVersion",
                "label": "Firmware version",
                "value": data['firmware_version'],
            },
        ],
    }

    is_mjs2020 = False
    if station_num > 0 and station_num < 2000:
        model = "MJS2016"
    elif station_num > 2000 and station_num < 3000:
        model = "MJS2020"
    else:
        model = None

    if model is not None:
        metadata["identifiers"].append({
            "definition": "http://sensorml.com/ont/swe/property/ModelNumber",
            "label": "Model Number",
            "value": model,
        })

    datastreams = []

    si7021 = {
        "name": "Si7021",
        "description": "Silicon Labs Si7021 temperature and humidity sensor",
        "encodingType": "application/vnd.ogc.sml+json",
        "metadata": {
            "type": "PhysicalComponent",
            "definition": "http://www.w3.org/ns/sosa/Sensor",
            "identifiers": [
                {
                    "definition": "http://sensorml.com/ont/swe/property/Manufacturer",
                    "label": "Manufacturer Name",
                    "value": "Silicon Labs"
                },
                {
                    "definition": "http://sensorml.com/ont/swe/property/ModelNumber",
                    "label": "Model Number",
                    "value": "Si7021"
                    # TODO: Could also be HTU21D
                },
            ],
            "documents": [
                {
                    "role": "http://dbpedia.org/resource/Datasheet",
                    "name": "Datasheet",
                    "link": {
                        "href": "https://www.silabs.com/documents/public/data-sheets/Si7021-A20.pdf",
                        "type": "application/pdf",
                    },
                },
            ],
        },
    }

    datastreams.append({
        "name": "Si7021 temperature",
        "description": "",
        "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
        "unitOfMeasurement": {
            "name": "degree Celcius",
            "symbol": "°C",
            "definition": "ucum:Cel",
        },
        "Sensor": si7021,
        "ObservedProperty": get_or_create_observed_property(sta, {
            "name": "Temperature",
            "definition": "http://qudt.org/vocab/quantitykind/Temperature",
            "description": "Temperature"
        }),
    })

    datastreams.append({
        "name": "Si7021 humidity",
        "description": "",
        "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
        "unitOfMeasurement": {
            "name": "percent",
            "symbol": "%",
            "definition": "ucum:%"
        },
        "Sensor": si7021,
        "ObservedProperty": get_or_create_observed_property(sta, {
            "name": "Humidity",
            "definition": "http://qudt.org/vocab/quantitykind/RelativeHumidity",
            "description": "Humidity"
        }),
    })

    pm_sensor = {
        "name": "Particulate matter sensor",
        "description": "Probably SDS11 or SPS30",
        "encodingType": "application/vnd.ogc.sml+json",
        "metadata": {
            "type": "PhysicalComponent",
            "definition": "http://www.w3.org/ns/sosa/Sensor",
        },
    }

    # make pm_sensor more specific for is_sps30
    if "is_sps30" in data:
        pm_sensor["name"] = "SPS30"
        pm_sensor["description"] = "Sensirion SPS30 Particulate matter sensor"
        pm_sensor["metadata"]["identifiers"] = [
            {
                "definition": "http://sensorml.com/ont/swe/property/Manufacturer",
                "label": "Manufacturer Name",
                "value": "Silicon Labs",
            },
            {
                "definition": "http://sensorml.com/ont/swe/property/ModelNumber",
                "label": "Model Number",
                "value": "SPS30",
            },
        ]
        pm_sensor["metadata"]["documents"] = [
            {
                "role": "http://dbpedia.org/resource/Datasheet",
                "name": "Datasheet",
                "link": {
                    "href": "https://sensirion.com/media/documents/8600FF88/64A3B8D6/Sensirion_PM_Sensors_Datasheet_SPS30.pdf",
                    "type": "application/pdf",
                },
            },
        ],

    def datastream_pm_density(size):
        return {
            "name": f"Particulate matter PM{size} density",
            "description": "",
            "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            "unitOfMeasurement": {
                "name": "microgram per cubic meter",
                "symbol": "μg/m3",
                "definition": "ucum:ug.m-3",
            },
            "Sensor": pm_sensor,
            "ObservedProperty": get_or_create_observed_property(
                sta,
                {
                    "name": f"Particulate matter PM{size} density",
                    "definition": f"https://qudt.org/vocab/quantitykind/MassDensity#pm_size={size}",
                    "description": f"Particulate matter density in ambient air, particle size < {size}μm",
                },
            ),
        }

    if "pm1" in data:
        datastreams.append(datastream_pm_density(1))
    if "pm2_5" in data:
        datastreams.append(datastream_pm_density(2.5))
    if "pm4" in data:
        datastreams.append(datastream_pm_density(4))
    if "pm10" in data:
        datastreams.append(datastream_pm_density(10))

    def datastream_pm_count(size):
        return {
            "name": f"Particulate matter PM{size} count",
            "description": "",
            "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            "unitOfMeasurement": {
                "name": "particle count per cubic centimeter",
                "symbol": "1/cm3",
                "definition": "ucum:cm-3",
            },
            "Sensor": pm_sensor,
            "ObservedProperty": get_or_create_observed_property(
                sta,
                {
                    "name": f"Particulate matter PM{size} count",
                    "definition": f"https://qudt.org/vocab/quantitykind/Count#pm_size={size}",
                    "description": f"Particulate matter count in ambient air, particle size < {size}μm",
                },
            ),
        }

    if "pn1" in data:
        datastreams.append(datastream_pm_count(1))
    if "pn2_5" in data:
        datastreams.append(datastream_pm_count(2.5))
    if "pn4" in data:
        datastreams.append(datastream_pm_count(4))
    if "pn10" in data:
        datastreams.append(datastream_pm_count(10))

    if "tps" in data:
        datastreams.append({
            "name": "Particulate matter typical particle size",
            "description": "",
            "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            "unitOfMeasurement": {
                "name": "micrometer",
                "symbol": "um",
                "definition": "ucum:um"
            },
            "Sensor": pm_sensor,
            "ObservedProperty": get_or_create_observed_property(sta, {
                "name": "Particulate matter typical particle size",
                "definition": "http://qudt.org/vocab/quantitykind/Diameter",
                "description": "Particulate matter typical particle size in ambient air"
            }),
        })

    # lux - illuminance:
    # https://meetjestad.net/en/Experiment_-_Light_sensor_comparison_at_Geophysics_Institute_Bergen
    # https://meetjestad.net/static/graphs/light/
    bpw34 = {
        "name": "BPW34",
        "description": "Vishay BPW34 Photodiode",
        "encodingType": "application/vnd.ogc.sml+json",
        "metadata": {
            "type": "PhysicalComponent",
            "definition": "http://www.w3.org/ns/sosa/Sensor",
            "identifiers": [
                {
                    "definition": "http://sensorml.com/ont/swe/property/Manufacturer",
                    "label": "Manufacturer Name",
                    "value": "Vishay"
                },
                {
                    "definition": "http://sensorml.com/ont/swe/property/ModelNumber",
                    "label": "Model Number",
                    "value": "BPW34"
                },
            ],
            "documents": [
                {
                    "role": "http://dbpedia.org/resource/Datasheet",
                    "name": "Datasheet",
                    "link": {
                        "href": "https://www.vishay.com/docs/81521/bpw34.pdf",
                        "type": "application/pdf",
                    },
                },
            ],
        },
    }

    if "lux" in data:
        datastreams.append({
            "name": "BPW34 illuminance",
            "description": "",
            "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            "unitOfMeasurement": {
                "name": "lux",
                "symbol": "lx",
                "definition": "ucum:lx"
            },
            "Sensor": bpw34,
            "ObservedProperty": get_or_create_observed_property(sta, {
                "name": "Illuminance",
                "definition": "http://qudt.org/vocab/quantitykind/LuminousFluxPerArea",
                "description": "Luminous Flux per Area"
            }),
        })

    mjs_arduino = {
        "name": "Internal ADC",
        "description": "Internal ADC",
        "encodingType": "application/vnd.ogc.sml+json",
        "metadata": {},
    }

    if "battery" in data:
        datastreams.append({
            "name": "Battery voltage",
            "description": "",
            "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            "unitOfMeasurement": {
                "name": "volt",
                "symbol": "V",
                "definition": "ucum:V",
            },
            "Sensor": mjs_arduino,
            "ObservedProperty": get_or_create_observed_property(
                sta,
                {
                    "name": "Battery voltage",
                    "definition": "http://qudt.org/vocab/quantitykind/Voltage#source=battery",
                    "description": "Battery Voltage",
                },
            ),
        })

    if "supply" in data:
        datastreams.append({
            "name": "Supply voltage",
            "description": "",
            "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            "unitOfMeasurement": {
                "name": "volt",
                "symbol": "V",
                "definition": "ucum:V",
            },
            "Sensor": mjs_arduino,
            "ObservedProperty": get_or_create_observed_property(
                sta,
                {
                    "name": "Supply voltage",
                    "definition": "http://qudt.org/vocab/quantitykind/Voltage#source=supply",
                    "description": "Supply Voltage",
                },
            ),
        })

    if "vsolar" in data:
        datastreams.append({
            "name": "Solar voltage",
            "description": "",
            "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            "unitOfMeasurement": {
                "name": "volt",
                "symbol": "V",
                "definition": "ucum:V",
            },
            "Sensor": mjs_arduino,
            "ObservedProperty": get_or_create_observed_property(
                sta,
                {
                    "name": "Solar voltage",
                    "definition": "http://qudt.org/vocab/quantitykind/Voltage#source=solar",
                    "description": "Solar Voltage",
                },
            ),
        })

    # TODO: location

    # TODO: Reuse existing Sensor objects if possible?

    for ds in datastreams:
        if "metadata" in ds.get("Sensor", {}):
            ds["Sensor"]["metadata"]["label"] = ds["Sensor"]["description"]
            ds["Sensor"]["metadata"]["uniqueId"] = f"urn:uuid:{uuid.uuid4()}"

    # TODO: Update location when it changes?

    return {
        "name": name,
        "description": desc,
        "properties": {
            "encodingType": "application/sml+json",
            "metadata": metadata,
        },
        "Datastreams": datastreams,
        "Locations": [
            get_or_create_location(sta, lat=data["latitude"], lon=data["longitude"])
        ],
    }


def make_thing_id(msg_obj):
    return "urn:fdc:meetjestad.nl:2024:thing/ttn/{}/{}".format(
        msg_obj["end_device_ids"]["application_ids"]["application_id"],
        msg_obj["end_device_ids"]["device_id"])


def make_msg_id(node_id, msg):
    return "{}/{}".format(node_id, msg["received_at"])


def make_meas_id(msg_id, chan_id):
    return "{}/{}".format(msg_id, chan_id)


def main():
    def terminate(sig, *args):
        print(f"Received signal {sig}, terminating", flush=True)
        sys.exit(0)
    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)

    logging.basicConfig(level=logging.INFO, force=True)

    logging.info(
        "Connecting Redis to {} on port {}".format(redis_url.hostname, redis_url.port)
    )

    sta = sensorthings.SensorThings(
        url=os.environ["SENSORTHINGS_URL"],
        username=os.environ.get("SENSORTHINGS_USER", None),
        password=os.environ.get("SENSORTHINGS_PASSWORD", None),
    )

    redis_server = redis.Redis(
        host=redis_url.hostname, port=redis_url.port, db=int(redis_url.path[1:] or 0), decode_responses=True,
    )

    # We user a consumer group, not with the intention of having
    # multiple consumers (since we need messages to be in-order
    # usually), but since that allows explicit acking and deleting only
    # acked messages.
    if redis_server.exists(redis_stream_in):
        groups = redis_server.xinfo_groups(name=redis_stream_in)
    else:
        logging.info(f"Stream {redis_stream_in} does not exist yet, will be created along with consumer group")
        groups = []
    if not [g for g in groups if g['name'] in redis_consumer_group]:
        logging.info(f"Creating consumer group {redis_consumer_group} for stream {redis_stream_in}")
        redis_server.xgroup_create(name=redis_stream_in, groupname=redis_consumer_group, id="0", mkstream=True)

    # Since we expect only one consumer, use the group name as the consumer name
    redis_consumer_name = redis_consumer_group
    logging.info(f"Using consumer {redis_consumer_name} in group {redis_consumer_group} for stream {redis_stream_in}")

    while True:
        # The special > id means "messages not seen by any consumer yet
        message_from = '>'
        for stream_name, messages in redis_server.xreadgroup(
            groupname=redis_consumer_group,
            consumername=redis_consumer_name,
            block=60 * 1000,
            streams={redis_stream_in: message_from},
        ):
            for entry_id, message in messages:
                logging.debug("Received message on stream %s: %s", stream_name, message)
                try:
                    process_message(sta, entry_id, message)
                    redis_server.xack(stream_name, redis_consumer_group, entry_id)
                # pylint: disable=broad-except
                except Exception as ex:
                    # TODO: Any messages not acked linger in the stream
                    # forever. We should report these errors and have a
                    # way to reprocess pending messages after the
                    # underlying error was fixed?
                    logging.exception("Error processing message: %s", ex)


if __name__ == "__main__":
    main()

# vim: set sw=4 sts=4 expandtab:
