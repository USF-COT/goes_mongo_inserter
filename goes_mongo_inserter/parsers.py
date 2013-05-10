import logging

import pyinotify
import threading
import os

logger = logging.getLogger("GMI")

from datetime import datetime

from pymongo import MongoClient

from prefixed_lines_parser import parse_prefixed_lines
from prefixed_sections_parser import parse_prefixed_sections
from trdi_adcp_parser import parse_trdi_pd15


def insert_goes_file(path, config, goes_id, mongo_collection):
    fstat = os.stat(path)
    file_object_id = None
    with open(path, 'r') as f:
        file_contents = f.read()
        file_object_id = mongo_collection.insert(
            {
                'goes_id': goes_id,
                'contents': file_contents,
                'processed': datetime.utcnow(),
                'modified': datetime.fromtimestamp(int(fstat.st_mtime))
            }
        )

    return file_object_id


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
        file_object_id = insert_goes_file(self.path, self.config,
                                          self.goes_id, file_collection)

        if file_object_id is not None:
            if self.config['type'] == 'trdi_adcp_pd15':
                parse_trdi_pd15(self.path, self.config,
                                file_object_id, db)
            elif self.config['type'] == 'prefixed_lines':
                parse_prefixed_lines(self.path, self.config,
                                     file_object_id, db)
            elif self.config['type'] == 'prefixed_sections':
                parse_prefixed_sections(self.path, self.config,
                                        file_object_id, db)
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
