import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from iso8601 import parse_date
import legacy_meta_lookups as meta_data

MJS_NODE_METADATA_TZ   = 'Europe/Amsterdam'



def parse_si7021(extra, data, node_id, timestamp, ttn_data):
    '''parse extra[] values for si7021, this case there are no extra fields. so simply pass.'''
    pass

def parse_vsolar(extra, data, node_id, timestamp, ttn_data):
    '''parse extra[] values for vsolar. (1 value in mV.)'''
    data["vsolar"] = extra.pop() / 1000


def parse_1xpinotechsw10_1xntc10k(extra, data, node_id, timestamp, ttn_data):
    '''parse extra[] values for 1xpinotechsw10_1xntc10k (2 values).
    "greenroof" sensor with calibration.

    value : (raw waarde * a) +b
    '''
    fields = meta_data.table_roof_format.lookup(node_id, timestamp)
    try:
        calibration = meta_data.table_roof_calib.lookup(node_id, timestamp)
    except KeyError:
        logging.warning("Need-Action: add calibration roof for node_id '%s' on timestamp '%s'. (need-replay)", node_id, timestamp)
        logging.warning("Need-Action: need-replay db_id='%s'", ttn_data.db_id)
        #  calibration fails
        calibration = None

    results = {}

    # calibrate logic:
    for xx in ['roofM', 'roofT']:
        # get raw value, fields knows the index of values roofM, roofT
        raw_value = extra[ int(fields[xx]) ]
        results[xx] = {'raw': raw_value}

        outcome = None
        calib_chars = None

        # calibration data:
        if calibration:

            cal_a = calibration['values'][xx]['a']
            cal_b = calibration['values'][xx]['b']

            # calibration characteristics
            calib_chars = {"name":"calibration",
                 "label":"Calibration parameters",
                  "values": []}
            # add values as name=key, value=value pairs:
            # dress up calib_chars['values'][] = dict(name=key, value=value)
            [calib_chars['values'].append({'name': k, 'value':v})  for k,v in calibration['values'][xx].items()]
            if 'date' in calibration:
                calib_chars['values'].append({'name': 'calibrationDate', 'label': 'Calibration Date', 'value': calibration['date']})

            if 'comment' in calibration:
                calib_chars['values'].append({'name': 'calibrationComment', 'label': 'Calibration Comment', 'value': calibration['comment']})

            if 'name' in calibration:
                calib_chars['values'].append({'name': 'calibrationName', 'label': 'Calibration Name', 'value': calibration['name']})

                # "characteristics": [
                #   {
                #     "name": "calibration",
                #     "label": "Calibration parameters",
                #     "values": [
                #       {
                #         "name": "temperatureOffset",
                #         "label": "Temperature Offset",
                #         "value": 0.2,
                #         "uom": "Cel"
                #       },
                #       {
                #         "name": "humidityOffset",
                #         "label": "Humidity Offset",
                #         "value": -1.5,
                #         "uom": "%"
                #       },
                #       {
                #         "name": "calibrationDate",
                #         "label": "Calibration Date",
                #         "value": "2025-01-15"
                #       }
                #     ]


            # calculate outcome
            if cal_a is not None and cal_b is not None:
                outcome = raw_value * cal_a + cal_b
                logging.info(f"Calibrate: roof {node_id} {xx}: {outcome} = {raw_value} * {cal_a} + {cal_b}")
            else:
                logging.warning("Need-Action: fix calibration roof, '%s' invalid 'a' or 'b' value(s) for node_id '%s' on timestamp '%s'. (need-replay)", xx, node_id, timestamp)
                logging.warning("Need-Action: need-replay db_id='%s'", ttn_data.db_id)
        # set
        results[xx] = {'raw':raw_value, 'value': outcome }
        results[xx]['calib_chars'] = calib_chars

    # set sensor type:
    results['roofM']['sensor'] = 'moist'
    results['roofT']['sensor'] = 'temp'


    # for now just add results list:
    data["roof_soil_moist"] = results['roofM']
    data["roof_soil_temp"] = results['roofT']


def parse_2xpinotechsw10_2xntc10k(extra, data, node_id, timestamp, ttn_data):
    '''parse extra[] values for 2xpinotechsw10_2xntc10k (4 values).
    "groundsoil" sensor with calibration.

    value : (raw waarde * a) +b
    '''
    fields = meta_data.table_soil_format.lookup(node_id, timestamp)

    try:
        calibration = meta_data.table_soil_calib.lookup(node_id, timestamp)

        # if calibration fails, the rest is unknown too (avoid getting default values.)
        soil_depths = meta_data.table_soil_depth.lookup(node_id, timestamp)
        soil_type = meta_data.table_soil_type.lookup(node_id, timestamp)
    except KeyError:
        logging.warning("Need-Action: add calibration soil for node_id '%s' on timestamp '%s'. (need-replay)", node_id, timestamp)
        logging.warning("Need-Action: need-replay db_id='%s'", ttn_data.db_id)
        calibration=False
        soil_depths = [10,40,10,40] # default values
        # soil_depths = [None, None, None, None] # default values
        soil_type = None




    calcs = {}
    # calibrate logic:
    for index, xx in enumerate(['soilM1', 'soilT1', 'soilM2', 'soilT2']):
        # get raw value, fields knows the index of values 'soilM1', 'soilT1', 'soilM2', 'soilT2'
        raw_value = extra[ int(fields[xx]) ]
        outcome = None
        calib_info = None # TODO , not deeded? remove also next calib_info
        calib_chars = None

        # calibration data:
        if calibration:
            cal_a = calibration['values'][xx]['a']
            cal_b = calibration['values'][xx]['b']

            # calibration characteristics
            calib_chars = {"name":"calibration",
                 "label":"Calibration parameters",
                  "values": []}
            # add values as name=key, value=value pairs:
            # dress up calib_chars['values'][] = dict(name=key, value=value)
            [calib_chars['values'].append({'name': k, 'value':v})  for k,v in calibration['values'][xx].items()]
            if 'date' in calibration:
                calib_chars['values'].append({'name': 'calibrationDate', 'label': 'Calibration Date', 'value': calibration['date']})

            if 'comment' in calibration:
                calib_chars['values'].append({'name': 'calibrationComment', 'label': 'Calibration Comment', 'value': calibration['comment']})

            if 'name' in calibration:
                calib_chars['values'].append({'name': 'calibrationName', 'label': 'Calibration Name', 'value': calibration['name']})

                # "characteristics": [
                #   {
                #     "name": "calibration",
                #     "label": "Calibration parameters",
                #     "values": [
                #       {
                #         "name": "temperatureOffset",
                #         "label": "Temperature Offset",
                #         "value": 0.2,
                #         "uom": "Cel"
                #       },
                #       {
                #         "name": "humidityOffset",
                #         "label": "Humidity Offset",
                #         "value": -1.5,
                #         "uom": "%"
                #       },
                #       {
                #         "name": "calibrationDate",
                #         "label": "Calibration Date",
                #         "value": "2025-01-15"
                #       }
                #     ]



            # calculate outcome
            if cal_a is not None and cal_b is not None:
                outcome = raw_value * cal_a + cal_b
                logging.info(f"Calibrate: soil {node_id} {xx}: {outcome} = {raw_value} * {cal_a} + {cal_b}")
            else:
                logging.warning("Need-Action: fix calibration soil, '%s' invalid 'a' or 'b' value(s) for node_id '%s' on timestamp '%s'. (need-replay)", xx, node_id, timestamp)
                logging.warning("Need-Action: need-replay db_id='%s'", ttn_data.db_id)


        if xx[:5] == 'soilM':
            sensor = 'moist'
        elif xx[:5] == 'soilT':
            sensor = 'temp'
        else:
            sensor = None

        calcs[xx] = {'raw':raw_value, 'value': outcome, 'depth': soil_depths[index], 'sensor': sensor, 'calib_info': calib_info }
        calcs[xx]['calib_chars'] = calib_chars
        # logging.info(f"SOIL: {node_id} soil_data[{xx}]: {calcs[xx]}")

        # soil_temp_d10 or soil_moist_d40 for example:
        if sensor and soil_depths[index]:
            data_key = f"soil_{sensor}_d{soil_depths[index]}"
            data[data_key] = calcs[xx]
            logging.info(f"SOIL: now avaiable as data['{data_key}'] == {xx} for node_id '{node_id}'")
        else:
            logging.warning(f"Need-Action: Missing soil depth or sensor type for '{node_id}', '{xx}' on '{timestamp}'. cannot create data key.")
            logging.warning("Need-Action: need-replay db_id='%s'", ttn_data.db_id)


    logging.debug(f"SOIL: {calcs}")
    # for now just add results list:
    data["soil_type"] = soil_type
    # data["soil_data"] = calcs #TODO remove this one

    # idea: soil_data dict
    #   soil_data[{n}] {'depth':..., 'temp':..., 'moist':....,}
    #   exmaple: soil_data['soilM1'] = {'raw': 620, 'value': 24.78, 'depth': '10', 'sensor': 'moist'}



def parse_hc_sr04_tt(extra, data, node_id, timestamp, ttn_data):
    # TODO: calibrate and more
    data["todo_hc_sr04_tt"] = extra

def parse_rcwl_1601(extra, data, node_id, timestamp, ttn_data):
    # TODO: calibrate and more
    data["todo_rcwl_1601"] = extra


def parse_sensirion_sps30(extra, data, node_id, timestamp, ttn_data):
    if extra == [0, 0, 0, 0, 0, 0, 0, 0, 0]:
        logging.info("parse_sensirion_sps30(): SPS30 ignoring data, values are all zeroes.")
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


# register parser is parsers lookup table:
parsers = {}
parsers['si7021'] = parse_si7021
parsers['sensirion_sps30'] = parse_sensirion_sps30
parsers['vsolar'] = parse_vsolar
parsers['1xpinotechsw10_1xntc10k'] = parse_1xpinotechsw10_1xntc10k
parsers['2xpinotechsw10_2xntc10k'] = parse_2xpinotechsw10_2xntc10k
parsers['hc-sr04_tt'] = parse_hc_sr04_tt
parsers['rcwl-1601'] = parse_rcwl_1601


def parse_extra(ttn_data, data):
    # mjs station id (from "meetstation-123"):
    node_id = str(ttn_data.station_num)
    # timestamp as "YYYYMMDD HHMMSS"
    timestamp = parse_date(ttn_data.received_at)
    timestamp = datetime.strftime(timestamp.astimezone(ZoneInfo(MJS_NODE_METADATA_TZ)), "%Y%m%d %H%M%S")

    # create list of sensors, in exact order of apearance in 'extra'
    sensors = meta_data.table_sensor_order.lookup(node_id, timestamp)
    extra_size = 0
    for sensor in sensors:
        extra_size += meta_data.dict_sensor_extra_len[sensor] # total 'extra_size'

    # validate extra size:
    if (extra_size != len(data['extra'])):
        # log message and return instead of raising exception
        mesg = f"MetaDataError: Unexpected length of 'extra', for '{node_id}' on '{timestamp}', expected {extra_size}, got {len(data['extra'])} for sensors: {sensors}"
        logging.warning(mesg)
        # raise Exception(f"Unexpected length of 'extra', for {node_id}, expected {extra_size}, got {len(data['extra'])} for sensors: {sensors}")
        return

    # dispatch parsing extra data:
    for sensor in sensors:
        parse_cls = parsers.get(sensor, None)
        extra_size = meta_data.dict_sensor_extra_len[sensor]

        # pop extra_size fields from extra list:
        extra = data['extra'][0:extra_size]
        del(data['extra'][0:extra_size])

        if parse_cls:
            logging.debug(f"parse_extra: {node_id} - {parse_cls}({extra}:{extra_size}) ")
            parse_cls(extra, data, node_id, timestamp, ttn_data)
        else:
            logging.warning(f"FAILED no parser found for: {node_id} - {parse_cls}({extra}:{extra_size})")


