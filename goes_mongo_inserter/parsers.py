import logging

import pyinotify
import threading
import os

logger = logging.getLogger("GMI")

from datetime import datetime

from pymongo import MongoClient

from prefixed_lines_parser import parse_prefixed_lines
from prefixed_sections_parser import parse_prefixed_sections
from trdi_adcp_readers.readers import read_PD15_file
from trdi_adcp_parser import parse_trdi_PD0


def parse_goes_header(header):
    header_fields = {}

    header_fields['goes_id'] = header[:8]

    # Parse timestamp
    year = header[8:10]
    doy = header[10:13]
    hour = header[13:15]
    minute = header[15:17]
    second = header[17:19]
    date_string = '%s-%s %s:%s:%s' % (year, doy, hour, minute, second)
    header_fields['timestamp'] = datetime.strptime(date_string,
                                                   '%y-%j %H:%M:%S')

    header_fields['failure_code'] = header[19:20]
    header_fields['signal_strength'] = header[20:22]
    header_fields['frequency_offset'] = header[22:24]
    header_fields['modulation_index_1'] = header[24:25]
    header_fields['modulation_index_2'] = header[25:26]
    header_fields['data_quality'] = header[26:30]
    header_fields['csta'] = header[30:32]
    header_fields['number_of_bytes'] = header[32:37]

    return header_fields


def insert_goes_file(path, config, goes_id, mongo_collection):
    fstat = os.stat(path)
    file_object_id = None
    fields = {}
    file_contents = []
    with open(path, 'r') as f:
        file_contents = list(f)

    # Parse out header and initialize field dictionary to be inserted
    if len(file_contents) > 0:
        fields = parse_goes_header(file_contents[0])
        fields['contents'] = file_contents
        fields['processed'] = datetime.utcnow()
        fields['modified'] = datetime.fromtimestamp(int(fstat.st_mtime))

        file_object_id = mongo_collection.insert(fields)

    return (file_object_id, fields)


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

    def run(self):
        # Connect to Mongo
        conn = MongoClient('localhost', 27017)
        db = conn['COMPS']

        logger.info("Config: %s" % (self.config))

        file_collection = db[self.config['station']+'.files']
        file_object_id, fields = insert_goes_file(self.path,
                                                  self.config,
                                                  self.goes_id,
                                                  file_collection)

        if file_object_id is not None and fields['failure_code'] == 'G':
            if self.config['type'] == 'trdi_adcp_pd15':
                try:
                    pd0_data = read_PD15_file(self.path,
                                              self.config['line_offset'])
                except:
                    logger.error('Error reading pd0 data '
                                 'from %s' % (self.path,))

                parse_trdi_PD0(pd0_data, self.config,
                               file_object_id, db)
            elif self.config['type'] == 'prefixed_lines':
                parse_prefixed_lines(self.path, self.config,
                                     file_object_id, db)
            elif self.config['type'] == 'prefixed_sections':
                parse_prefixed_sections(self.path, self.config,
                                        file_object_id, db)
        elif file_object_id is not None and fields['failure_code'] == '?':
            logger.info('Bad goes transmission for file '
                        'with mongo id: %s' % (file_object_id))
        else:
            logger.error('Error inserting GOES file details. No data inserted')


class GOESUpdateHandler(pyinotify.ProcessEvent):
    configs = {}

    def __init__(self, configs):
        pyinotify.ProcessEvent.__init__(self)
        self.configs = configs

    def process_file(self, event):
        logger.info("Processing: %s" % event.pathname)
        if event.name[0] is not '.':  # Ignore hidden files
            if event.name in self.configs:
                parser = GOESFileParser(event.pathname, event.name,
                                        self.configs[event.name])
                parser.start()
            else:
                logger.info('File %s modified, but no parser '
                            'configuration setup.  Ignoring...' % event.name)

    def process_IN_CREATE(self, event):
        pass

    def process_IN_CLOSE_WRITE(self, event):
        self.process_file(event)

    def process_IN_CLOSE_NOWRITE(self, event):
        self.process_file(event)
