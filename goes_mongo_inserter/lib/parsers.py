import logging

import pyinotify
import threading
import os

logger = logging.getLogger("GMI")

from datetime import datetime

from pymongo import MongoClient

from prefixed_lines_parser import parse_prefixed_lines
from prefixed_sections_parser import parse_prefixed_sections
from coastal_station_parser import parse_coastal_station
from trdi_adcp_parser import parse_trdi_adcp
from multi_table_parser import parse_multi_table


parsers = {
    'trdi_adcp_pd15': parse_trdi_adcp,
    'prefixed_lines': parse_prefixed_lines,
    'prefixed_sections': parse_prefixed_sections,
    'coastal_station': parse_coastal_station,
    'multi_table': parse_multi_table
}


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
    with open(path, 'rb') as f:
        for line in f:
            file_contents.append(line)

    # Parse out header and initialize field dictionary to be inserted
    if len(file_contents) > 0:
        fields = parse_goes_header(file_contents[0])
        fields['contents'] = file_contents
        fields['processed'] = datetime.utcnow()
        fields['modified'] = datetime.fromtimestamp(int(fstat.st_mtime))

        try:
            file_object_id = mongo_collection.insert(fields)
        except Exception, ex:
            logger.error('Unable to insert file from GOES %s: %s'
                         % (goes_id, ex))

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

        file_collection = db[self.config['station']+'.files']
        file_object_id, fields = insert_goes_file(self.path,
                                                  self.config,
                                                  self.goes_id,
                                                  file_collection)

        if file_object_id is None:
            return

        logger.info("Processing GOES %s. Station: %s. Mongo ID: %s"
                    % (self.path, self.config['station'], file_object_id))

        if fields['failure_code'] != 'G':
            logger.warning(
                'Poor signal found in %s.  Logging contents only.' % self.path
            )

        if file_object_id is not None and fields['failure_code'] == 'G':
            if self.config['type'] in parsers:
                try:
                    parsers[self.config['type']](self.path, self.config,
                                                 file_object_id, db)
                except Exception, e:
                    logger.error('Exception parsing '
                                 '%s type: %s' % (self.config['type'], e))
            else:
                logger.error(
                    'Unrecognized config type %s.  Are you using '
                    'an old version of GMI?' % self.config['type']
                )
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
