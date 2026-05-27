"""Lookup table and functions for 'node_metadata.json':

first we read the json and collect and fix all info we need in some dictionaries wrapped in an ImmutableLookupTable/LookupTable:
this object has 2 dicts:
 dict('node_id' = OrderedDict( 'timestamp' = { .. data .. } ))

using the add(), condens() and lookup() methods it creates a mem and cpu  efficient (enough ;) ) lookup table.


table_sensor_order.lookup('node_id', 'timestamp'):
    lookup the order of sensor values in the extra fields.

table_soil_format.lookup('node_id', 'timestamp'):
    lookup the dict containing indexes of extra fields by sensor_value_name.
    example: {'soilM1': '0', 'soilT1': '1', 'soilM2': '2', 'soilT2': '3'}

table_roof_format.lookup('node_id', 'timestamp'):
    lookup the dict containing indexes of extra fields by sensor_value_name.
    example: {'roofM': '0', 'roofT': '1'}

# lookup calibration values:
table_soil_calib.lookup('node_id', 'timestamp'):

table_roof_calib.lookup('node_id', 'timestamp'):

# lookup other soil meta data:
table_soil_depth.lookup('node_id', 'timestamp'):

table_soil_type.lookup('node_id', 'timestamp'):


"""
from collections import OrderedDict
import logging
import json


# globals: use loadTAbles() to fill
table_sensor_order = None
table_soil_format = None
table_roof_format = None
table_soil_calib = None
table_roof_calib = None
dict_sensor_extra_len = None


# import lookup_extra

class LookupTable:
    """
    Lookup table for legacy metadata per Node and timestamp.

    dict[ node_id ][ valid_from ] = { .... }
    """
    def __init__(self, default=None, table_name='unknown_table'):
        # self.table = defaultdict(OrderedDict)
        self.table = dict()
        self.default = default

        # used for export
        self.table_name = table_name

    def __repr__(self):
        return f"{type(self).__name__}(<{self.table_name}>)"

    def add(self, node_id, timestamp, data):
        if (timestamp == '' and data == self.default):
            # logging.debug("ignoring data, same as default: %s, %s", node_id, data)
            return

        if node_id not in self.table:
            self.table[node_id] = OrderedDict()

        # append data:
        self.table[node_id][timestamp] = data

        # order: latest timestamp first:
        for key in sorted(self.table[node_id].keys(), reverse=True):
            self.table[node_id].move_to_end(key)


    def lookup(self, node_id, now):
        if node_id in self.table:
            for timestamp in self.table[node_id].keys():
                if timestamp <= now:
                    logging.debug("%s found data: %s,%s for %s", self, node_id, timestamp, now)
                    return self.table[node_id][timestamp]

        if self.default is not None:
            logging.debug("%s: nothing found returning default for %s, %s", self, node_id, now)
            return self.default

        else:
            raise KeyError(f"{self}: nothing found for {node_id}, {now}.")

    def condens(self):
        '''remove duplicate data entries with upfollowing timestamps'''
        for node_id,table in list(self.table.items()):
            # first entry equal to default:
            first_time = list(table.keys())[0]
            if table[first_time] == self.default:
                logging.debug(f"condens(): first_time entry same as default {node_id}, {first_time}, {table[first_time]}")
                del(table[first_time])


            # remove duplicate entries :
            previous = [None, None]
            for timestamp, data in table.items():
                # compare previous
                if [node_id, data] == previous:
                    logging.debug(f"condens(): removed duplicate data entry with new timestamp {node_id}, {timestamp}, {data}")
                    del(table[timestamp])

                # print(dict(node_id=node_id, timestamp=timestamp, data=data))
                previous = [node_id, data]

            if len(table) == 0:
                logging.debug(f"condens(): length become 0, nothing other than to default to keep for {node_id}.")
                del(self.table[node_id])


    def show(self, node_id):
        '''show all entries for an node_id'''
        if self.default is not None:
            print(f"{node_id}, <default>, {self.default}")
            print()

        for timestamp, data in self.table.get(node_id, {}).items():
            print(f"{node_id}, {timestamp}, {data}")
            print()




    def stats(self):
        # number of entries per node:
        #
        print(f"default: {self.default}")
        print(f"total entries: {len(self.table)}")

        print("nodes with more than 2 entries:")
        print([(n, len(items)) for n,items in  self.table.items() if len(items) >= 2 ])
        print("")


    def export(self):
        print(f"{self.table_name} = {type(self).__name__}(default={self.default})")
        for node_id,table in self.table.items():
            previous = [None, None]
            for timestamp, data in table.items():
                # print(dict(node_id=node_id, timestamp=timestamp, data=data))
                print(f"{self.table_name}.add('{node_id}', '{timestamp}', {data})")


class ImmutableLookupTable(LookupTable):
    """
    Since the 'data' is considered immutable, we don't keep duplicates.
    all unique 'data' entries are stored inside self.unique[].
    only a reference is kept in self.table.

    once populated, self.unique may be deleted.
    """
    def __init__(self, default=None, *args, **kwargs):
        super().__init__(default, *args, **kwargs)
        self.unique = [default]

    def add(self, node_id, timestamp, data):
        if data not in self.unique:
            self.unique.append(data)
            # logging.debug("add %s to self.data", data)
        else:
            # we use the object from data:
            data = self.unique[self.unique.index(data)]

        super().add(node_id, timestamp, data)

    def stats(self):
        super().stats()
        print(f"unique entries: {len(self.unique) - 1}") # -1 to not count default.



def makeTableSensorOrder(json_metadata,):
    # node_id , timestamp , default = tuple()
    default=tuple()
    table = ImmutableLookupTable(default, table_name='table_sensor_order')
    for node in json_metadata.get('nodes'):
        node_id = node.get('id', None)
        sensors = node.get('sensors', [])

        if type(sensors) is not list:
            logging.debug("ignoring sensors for node %s, it is not a list , dropping: %s", node_id, sensors)
            sensors = []

        for sensor in sensors:
            # get timestamp and data
            timestamp = sensor.get('date', "")
            data = sensor.get('order', default)

            # remove 'si7021', has extra_fields = 0, is not needed to know
            if 'si7021' in data:
                del(data[data.index('si7021')])

            # store it as tuple.
            table.add(node_id, timestamp, tuple(data))

    table.condens()
    global table_sensor_order
    table_sensor_order=table


def makeTableFormats(json_metadata):
    # node_id , timestamp , default = tuple()

    # 'sensor': '2xpinotechsw10_2xntc10k'
    soil_table = ImmutableLookupTable({'soilM1': '0', 'soilT1': '1', 'soilM2': '2', 'soilT2': '3'}, table_name='table_soil_format')
    # 'sensor': '1xpinotechsw10_1xntc10k'
    roof_table = ImmutableLookupTable({'roofM': '0', 'roofT': '1'},table_name='table_roof_format')

    for node in json_metadata.get('nodes'):
        node_id = node.get('id', None)
        formats = node.get('formats', [])

        if type(formats) is not list:
            logging.debug("ignoring formats for node %s, it is not a list , dropping: %s", node_id, formats)
            formats = []

        for field_order in formats:
            # get timestamp and data
            timestamp = field_order.get('date', "")
            # remove date from field_order:
            if 'date' in field_order:
                del(field_order['date'])
            data = field_order

            # store it as tuple.
            sensor = data.get('sensor', None)
            if 'sensor' in data:
                del(data['sensor'])

            if sensor == '2xpinotechsw10_2xntc10k':
                soil_table.add(node_id, timestamp, data)
            elif sensor == '1xpinotechsw10_1xntc10k':
                roof_table.add(node_id, timestamp, data)
            else:
                logging.debug("ignoring format for node %s, unknown sensor: %s", node_id, field_order)

    soil_table.condens()
    roof_table.condens()

    # make the table global:
    global table_soil_format
    table_soil_format=soil_table
    global table_roof_format
    table_roof_format=roof_table

def makeTableCalibrations(json_metadata):

    def fix_calib_value(value):
        '''FIX calibrations.values str/comma into float/None'''
        if type(value) is str:
            if value == '':
                # '' -> None
                return None
            else:
                # fix decimal comma sign -> dot -> float: '1,23' -> '1.23' -> 1.23
                return float(value.replace(',','.'))
        # nothing to fix:
        return value

    # 'sensor': '2xpinotechsw10_2xntc10k'
    soil_default = {'comment': 'no actual measurement, copied from station 2126 dd 30-12-2024', 'name': 'default', 'date': '20221127', 'values': {'soilM1': {'a': 0.049, 'bsen': 15.0, 'b': -5.1, 'alpha': 0.0, 'beta': 0.02, 'gamma': 0.02}, 'soilT1': {'a': 0.25, 'b': -20.0}, 'soilM2': {'a': 0.049, 'bsen': 7.9, 'b': -4.7, 'alpha': 0.0, 'beta': 0.02, 'gamma': 0.02}, 'soilT2': {'a': 0.25, 'b': -20.0}}}
    soil_table = ImmutableLookupTable(table_name='table_soil_calib', default=soil_default)
    # 'sensor': '1xpinotechsw10_1xntc10k'
    roof_table = ImmutableLookupTable(table_name='table_roof_calib')

    for node in json_metadata.get('nodes'):
        node_id = node.get('id', None)
        formats = node.get('calibrations', [])

        if type(formats) is not list:
            logging.debug("ignoring calibrations for node %s, it is not a list , dropping: %s", node_id, formats)
            formats = []

        for field_order in formats:
            # get timestamp and data
            timestamp = field_order.get('date', "")
            # # remove date from field_order:
            # if 'date' in field_order:
            #     del(field_order['date'])
            data = field_order

            # store it as tuple.
            sensor = data.get('sensor', None)
            if 'sensor' in data:
                del(data['sensor'])

            if 'comment' in data and data['comment'] == '':
                del(data['comment'])
                logging.debug("removed empty comment in calibration for node %s, timestamp %s", node_id, timestamp)
            if 'name' in data and data['name'] == '':
                del(data['name'])
                logging.debug("removed empty name in calibration for node %s, timestamp %s", node_id, timestamp)

            # FIX calibrations.values str/comma into float/None

            # for "soil|roof[MT][12]? ..."
            for item in data.get('values', {}).keys():
                # for "a|b|bsen|alpha|beta|gamma ..."
                for key, value in data['values'][item].items():
                    data['values'][item][key] = fix_calib_value(value)


            if sensor == '2xpinotechsw10_2xntc10k':
                soil_table.add(node_id, timestamp, data)
            elif sensor == '1xpinotechsw10_1xntc10k':
                # FIX calibrations.values roofM1 -> roofM
                if data.get('values', {}).get('roofM', False):
                    # merge roofM1 into roofM
                    data['values']['roofM'] = data['values']['roofM1'] | data['values'].get('roofM', {})
                    del(data['values']['roofM1'])

                # FIX calibrations.values roofT1 -> roofT
                if data.get('values', {}).get('roofT1', False):
                    # merge roofT1 into roofT
                    data['values']['roofT'] = data['values']['roofT1'] | data['values'].get('roofT', {})
                    del(data['values']['roofT1'])

                roof_table.add(node_id, timestamp, data)
            else:
                logging.debug("ignoring calibrations for node %s, unknown sensor: %s", node_id, field_order)

    soil_table.condens()
    roof_table.condens()

    global table_soil_calib
    table_soil_calib=soil_table
    global table_roof_calib
    table_roof_calib=roof_table

def makeTableSoilMeta(json_metadata):
    # 'sensor': '2xpinotechsw10_2xntc10k'

    global table_soil_depth
    table_soil_depth = ImmutableLookupTable(table_name='table_soil_depth', default=["10","40","10","40"])

    global table_soil_type
    table_soil_type = ImmutableLookupTable(table_name='table_soil_type', default="sand")

    for node in json_metadata.get('nodes'):
        node_id = node.get('id', None)
        soil = node.get('location', {}).get('soil',{})
        # print(f"DEBUG: {node_id}", json.dumps(soil, indent=2))

        if 'depths' in soil:
            depths = soil.get('depths')
            if len(depths) == 2:
                # make 4 values so it reflects [soilM1, soilT1, soilM2, soilM2]
                depths = [depths[0], depths[0], depths[1], depths[1]]

            # due to missing timestamps we use ""
            table_soil_depth.add(node_id, "",depths )

        if 'type' in soil:
            # due to missing timestamps we use ""
            table_soil_type.add(node_id, "",soil.get('type'))

    # table_soil_depth.condens()
    # table_soil_type.condens()

def loadTables(mjs_node_metadata_json='node_metadata.json'):
    '''
    # loadTables: create lookup tables from 'mjs_node_metadata_json'.

    hint: curl https://meetjestad.net/static/node_metadata.json -o node_metadata.json

    # use .lookup('sensor_id', 'timestamp') on the following tables:
    global table_sensor_order
    global table_soil_format
    global table_roof_format
    global table_soil_calib
    global table_roof_calib
    global table_soil_type
    global table_soil_depth

    # get extra field len for sensor:
    exmaple: dict_sensor_extra_len['vsolar'] # -> returns 1
    global dict_sensor_extra_len

    '''
    logging.info("load lookup tables form metajson '%s'.", mjs_node_metadata_json)

    # open json data
    node_metadata = json.loads(open(mjs_node_metadata_json).read())
    makeTableSensorOrder(node_metadata)
    makeTableFormats(node_metadata)
    makeTableCalibrations(node_metadata)
    makeTableSoilMeta(node_metadata)

    # sensor extra fields:
    global dict_sensor_extra_len
    # {'si7021': 0, '1xpinotechsw10_1xntc10k': 2, '2xpinotechsw10_2xntc10k': 4, 'vsolar': 1, 'sensirion_sps30': 9, 'hc-sr04_tt': 1, 'rcwl-1601': 1}
    dict_sensor_extra_len = dict([(x.get('name', 'not-set'),int(x.get('fields',{}).get('extra', 0))) for x in node_metadata.get('sensors')])

    # remove json data
    del(node_metadata)


class Node:
    def __init__(self, node_id, timestamp):
        self.node_id = str(node_id)
        self.timestamp = str(timestamp)

    def __repr__(self):
        return f"Node('{self.node_id}', '{self.timestamp}')"

    #
    # sensor_order
    #
    @property
    def sensor_order(self):
        if not hasattr(self, '_sensor_order'):
            self._sensor_order = table_sensor_order.lookup(self.node_id, self.timestamp)
        return self._sensor_order

    @property
    def total_extra_len(self):
        return sum([dict_sensor_extra_len[s] for s in self.sensor_order])

    #
    # soil:
    #
    @property
    def soil_format(self):
        return table_soil_format.lookup(self.node_id, self.timestamp)

    @property
    def soil_calib(self):
        return table_soil_calib.lookup(self.node_id, self.timestamp)

    @property
    def soil_type(self):
        return table_soil_type.lookup(self.node_id, self.timestamp)

    @property
    def soil_depth(self):
        return table_soil_depth.lookup(self.node_id, self.timestamp)

    #
    # roof:
    #
    @property
    def roof_format(self):
        return table_roof_format.lookup(self.node_id, self.timestamp)

    @property
    def roof_calib(self):
        return table_roof_calib.lookup(self.node_id, self.timestamp)



    def show(self):
        '''defubg info for this timestamp:'''
        print(f"{self}:")
        print(f"{self.sensor_order=}")
        print(f"{self.total_extra_len=}")

        if '2xpinotechsw10_2xntc10k' in self.sensor_order:
            print(f"{self.soil_format=}")
            print(f"{self.soil_calib=}")

        if '1xpinotechsw10_1xntc10k' in self.sensor_order:
            print(f"{self.roof_format=}")
            print(f"{self.roof_calib=}")

    def showall(self):
        """extended debug info for all timestamps"""
        print(f"{self}:")
        table_sensor_order.show(self.node_id)
        table_soil_format.show(self.node_id)
        table_soil_calib.show(self.node_id)
        table_soil_type.show(self.node_id)
        table_soil_depth.show(self.node_id)
        table_roof_format.show(self.node_id)
        table_roof_calib.show(self.node_id)

def getSensorLen(sensor):
    '''get number of extra field values for a sensor.
    reading from global dict: dict_sensor_extra_len.
    '''
    return dict_sensor_extra_len[sensor]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, force=True)
    loadTables()
    print("meta_loopup.py: ", __doc__)

else:
    loadTables()
