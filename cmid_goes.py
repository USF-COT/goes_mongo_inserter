import logging

import pyinotify
import threading

logger = logging.getLogger("CMID")

from datetime import date, time, datetime
from pytz import timezone

from pymongo import MongoClient

def parse_int(field_desc, part):
    return int(part)

def parse_float(field_desc, part):
    return float(part)

def parse_timestamp(field_desc, part):
    fmt = field_desc['format']
    tz = timezone(field_desc['timezone'])

    dt = datetime.strptime(part, fmt)
    return tz.localize(dt)

def parse_position(field_desc, part):
    gps_pos = [0 0]
    name = field_desc['name']
    if name in line_data:
        gps_pos = line_data[name]
    
    if field_desc['component'] == 'lng':
        gps_pos[0] = float(part)
    elif field_desc['component'] == 'lat':
        gps_pos[1] = float(part)

    return gps_pos

datatype_parsers = {'int': parse_int, 'float': parse_float, 'timestamp': parse_timestamp, 'position': parse_position}

class GOESFileParser(threading.Thread):
    path = ''
    config = {}
    connection = MongoClient('localhost', 27017)

    def __init__(self, path, config):
        threading.Thread.__init__(self)
        self.path = path
        self.config = config

    def run(self):
        logger.info("Config: %s" % (self.config))
        # Open, process, and insert data according to configuration
        with open(self.path, 'r') as f:
            # Chew up offset lines
            offset = self.config['line_offset']
            while offset > 0:
                f.readline()
                offset = offset - 1
                continue

            data = []
            for line in f:
                line_parts = line.split(',')
                if line_parts[0] in self.config['lines']:
                    line_data = {}
                    # Parse fields
                    for i, part in enumerate(line_parts[1:]):
                        if i < len(self.config['lines']):
                            field_desc = self.config['lines'][i]
                            data_key = field_desc['name']
                            if 'units' in field_desc:
                                data_key += "-%s" % (field_desc['units'],)

                            field_type = field_desc['type']
                            if field_type in datatype_parsers:
                                value = datatype_parsers[field_type](field_desc, part)
                                if value is not None:
                                    line_data[data_key] = value
                            else:
                                logger.error('Unknown type %s in configuration file line %s, field %s' % (field_type, line_parts[0], field_desc['name'])
                        else:
                            logger.error('Exceeded specified number of field descriptors.  Ignoring remaining fields')
                    # Store in data array
                    data.append(line_data)
                else:
                    logger.info("Ignoring line with unknown prefix in file %s: %s" % (self.path, line_parts[0]))

            # Connect to Mongo
            conn = MongoClient('localhost', 27017)
            db = conn['COMPS']
            collection = db[self.config['station']]
            collection.insert(data)

class GOESUpdateHandler(pyinotify.ProcessEvent):
    configs = {}

    def __init__(self, configs):
        pyinotify.ProcessEvent.__init__(self)
        self.configs = configs

    def process_file(self, event):
        logger.info("Processing: %s" % event.pathname)
        if event.name[0] is not '.': # Ignore hidden files
            if event.name in self.configs:
                parser = GOESFileParser(event.pathname, self.configs[event.name])
                parser.start()
            else:
                logger.info('File %s modified, but no parser configuration setup.  Ignoring...' % event.name)

    def process_IN_CREATE(self, event):
        self.process_file(event)

    def process_IN_CLOSE_WRITE(self, event):
        self.process_file(event)

    def process_IN_CLOSE_NOWRITE(self, event):
        self.process_file(event)
