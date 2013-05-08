import logging

import pyinotify
import threading
import os

logger = logging.getLogger("CMID")

from datetime import date, time, datetime
from pytz import timezone

from pymongo import MongoClient

def parse_int(line_data, field_desc, part):
    if 'ignore' in field_desc:
        ignore_vals = field_desc['ignore'].split(',')
        if part in ignore_vals:
            return None

    return int(part)

def parse_float(line_data, field_desc, part):
    if 'ignore' in field_desc:
        ignore_vals = field_desc['ignore'].split(',')
        if part in ignore_vals:
            return None

    return float(part)

def parse_text(line_data, field_desc, part):
    if 'ignore' in field_desc:
        ignore_vals = field_desc['ignore'].split(',')
        if part in ignore_vals:
            return None

    return part

def parse_timestamp(line_data, field_desc, part):
    fmt = field_desc['format']
    tz = timezone(field_desc['timezone'])

    dt = datetime.strptime(part, fmt)
    return tz.localize(dt)

def convert_merged_degrees_minutes(degrees_minutes):
    degrees = 0

    decimal_place = degrees_minutes.find('.')

    if decimal_place == -1:
        logger.error('Unable to parse GPS string: No decimal place present in string (%s)' % (degrees_minutes,))
        return -1

    deg_str = degrees_minutes[0:decimal_place-2]
    min_str = degrees_minutes[decimal_place-2:]
    
    try:
        degrees = float(deg_str)
        minutes = float(min_str)
        if degrees < 0:
            minutes *= -1

        degrees += minutes/60
        return degrees
    except:
        return -1 

def parse_point_degrees_minutes(line_data, field_desc, part):
    gps_pos = [0, 0]
    name = field_desc['name']
    if name in line_data:
        gps_pos = line_data[name]
    
    value = convert_merged_degrees_minutes(part)
    if field_desc['component'] == 'lng':
        gps_pos[0] = value * -1 # For some reason GOES returns a positive longitude when it should be negative
    elif field_desc['component'] == 'lat':
        gps_pos[1] = value

    return gps_pos

datatype_parsers = {'int': parse_int, 'float': parse_float, 'text': parse_text, 'timestamp': parse_timestamp, 'point_degrees_minutes': parse_point_degrees_minutes}

class GOESFileParser(threading.Thread):
    path = ''
    goes_id = ''
    config = {}
    connection = MongoClient('localhost', 27017)

    def __init__(self, path, goes_id, config):
        threading.Thread.__init__(self)
        self.goes_id = goes_id
        self.path = path
        self.config = config
    
    def parse_line(self, prefix, line_parts):
        line_data = {}

        line_config = self.config['lines'][prefix]

        # Parse fields
        for i, part in enumerate(line_parts[1:]):
            if i < len(line_config):
                field_desc = line_config[i]
                data_key = field_desc['name']
                if 'units' in field_desc:
                    data_key += "-%s" % (field_desc['units'],)

                field_type = field_desc['type']
                if field_type in datatype_parsers:
                    value = datatype_parsers[field_type](line_data, field_desc, part.strip())
                    if value is not None:
                        line_data[data_key] = value
                else:
                    logger.error('Unknown type %s in configuration file line %s, field %s' % (field_type, line_parts[0], field_desc['name']))
            else:
                logger.error('Exceeded specified number of field descriptors.  Ignoring remaining fields')

        return line_data

    def run(self):
        # Connect to Mongo
        conn = MongoClient('localhost', 27017)
        db = conn['COMPS']

        logger.info("Config: %s" % (self.config))
        fstat = os.stat(self.path)
        with open(self.path, 'r') as f:
            file_contents = f.read()
            collection = db[self.config['station']+'.files']
            file_object_id = collection.insert({'goes_id': self.goes_id, 'contents': file_contents, 'processed': datetime.utcnow(), 'modified': datetime.fromtimestamp(int(fstat.st_mtime))})

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
                prefix = None
                if line_parts[0] in self.config['lines']:
                    prefix = line_parts[0]
                elif 'NOHEADER' in self.config['lines']:
                    prefix = 'NOHEADER'
                else:
                    logger.info("Ignoring line with unknown prefix in file %s: %s" % (self.path, line_parts[0]))

                if prefix is not None:
                    line_data = self.parse_line(prefix=prefix, line_parts=line_parts)
                    
                    if len(line_data) > 0:
                        line_data['prefix'] = prefix
                        line_data['file_id'] = file_object_id
                        data.append(line_data)

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
                parser = GOESFileParser(event.pathname, event.name, self.configs[event.name])
                parser.start()
            else:
                logger.info('File %s modified, but no parser configuration setup.  Ignoring...' % event.name)

    def process_IN_CREATE(self, event):
        self.process_file(event)

    def process_IN_CLOSE_WRITE(self, event):
        self.process_file(event)

    def process_IN_CLOSE_NOWRITE(self, event):
        self.process_file(event)
