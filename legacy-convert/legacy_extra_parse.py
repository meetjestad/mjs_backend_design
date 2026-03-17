import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from iso8601 import parse_date
import legacy_meta_lookups as meta_data

MJS_NODE_METADATA_TZ   = 'Europe/Amsterdam'



def parse_si7021(extra, data, node_id, timestamp):
    '''parse extra[] values for si7021, this case there are no extra fields. so simply pass.'''
    pass

def parse_vsolar(extra, data, node_id, timestamp):
    '''parse extra[] values for vsolar. (1 value in mV.)'''
    data["vsolar"] = extra.pop() / 1000


def parse_1xpinotechsw10_1xntc10k(extra, data, node_id, timestamp):
    '''parse extra[] values for 1xpinotechsw10_1xntc10k (2 values).
    "greenroof" sensor with calibration.

    value : (raw waarde * a) +b
    '''
    fields = meta_data.table_roof_format.lookup(node_id, timestamp)
    calibration = meta_data.table_roof_calib.lookup(node_id, timestamp)
    results = {}

    # calibrate logic:
    for xx in ['roofM1', 'roofT1']:
        # get raw value, fields knows the index of values roofM, roofT
        raw_value = extra[ int(fields[xx]) ]
        results[xx] = {'raw': raw_value}

        # calibration data:
        cal_a = calibration['values'][xx]['a']
        cal_b = calibration['values'][xx]['b']

        # calculate outcome
        if cal_a is not None and cal_b is not None:
            results[xx]['value'] = raw_value * cal_a + cal_b
            logging.info(f"Calibrate: {node_id} {xx}: {results[xx]['value']} = {raw_value} * {cal_a} + {cal_b}")
        else:
            logging.warning(f"MetaDataError: Calibriation value missing for '%s', '%s' on '%s'.", node_id, xx, timestamp)

    # for now just add results list:
    data["roof_soil_moist"] = results['roofM1']
    data["roof_soil_temp"] = results['roofT1']


def parse_2xpinotechsw10_2xntc10k(extra, data, node_id, timestamp):
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
        logging.warning("MetaDataError: No soil calibration found for node_id '%s' on timestamp '%s'.", node_id, timestamp)
        calibration=False
        soil_depths = [None,None,None,None]
        soil_type = None




    calcs = {}
    # calibrate logic:
    for index, xx in enumerate(['soilM1', 'soilT1', 'soilM2', 'soilT2']):
        # get raw value, fields knows the index of values 'soilM1', 'soilT1', 'soilM2', 'soilT2'
        raw_value = extra[ int(fields[xx]) ]
        outcome = None

        # calibration data:
        if calibration:
            cal_a = calibration['values'][xx]['a']
            cal_b = calibration['values'][xx]['b']

            # calculate outcome
            if cal_a is not None and cal_b is not None:
                outcome = raw_value * cal_a + cal_b
                logging.info(f"Calibrate: {node_id} {xx}: {outcome} = {raw_value} * {cal_a} + {cal_b}")
            else:
                logging.warning("MetaDataError: Calibriation value missing for '%s', '%s' on '%s'.", node_id, xx, timestamp)

        if xx[:5] == 'soilM':
            sensor = 'moist'
        else:
            sensor = 'temp'

        calcs[xx] = {'raw':raw_value, 'value': outcome, 'depth': soil_depths[index], 'sensor': sensor }


    logging.debug(f"SOIL: {calcs}")
    # for now just add results list:
    data["soil_type"] = soil_type
    data["soil_data"] = calcs

    # ideas: soil_data dict vs many key+values
    #   soil_data[{n}] {'depth':..., 'temp':..., 'moist':....,}
    #   exmaple: soil_data['soilM1'] = {'raw': 620, 'value': 24.78, 'depth': '10', 'sensor': 'moist'}
    # of:
    #   data[ soil_d{depth}_temp  ] = value
    #   data[ soil_d{depth}_moist ] = value
    #

    data["soil_d10_moist"] = calcs['soilM1'].get('value', None)
    data["soil_d10_temp"] = calcs['soilT1'].get('value', None)
    data["soil_d40_moist"] = calcs['soilM2'].get('value', None)
    data["soil_d40_temp"] = calcs['soilT2'].get('value', None)



def parse_hc_sr04_tt(extra, data, node_id, timestamp):
    # TODO: calibrate and more
    data["todo_hc_sr04_tt"] = extra

def parse_rcwl_1601(extra, data, node_id, timestamp):
    # TODO: calibrate and more
    data["todo_rcwl_1601"] = extra


def parse_sensirion_sps30(extra, data, node_id, timestamp):
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


def parse_extra(msg_obj, data):
    # mjs station id (from "meetstation-123"):
    node_id = msg_obj["end_device_ids"]["device_id"].split('-')[1]
    # timestamp as "YYYYMMDD HHMMSS"
    timestamp = parse_date(msg_obj["received_at"])
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
            parse_cls(extra, data, node_id, timestamp)
        else:
            logging.warning(f"FAILED no parser found for: {node_id} - {parse_cls}({extra}:{extra_size})")


