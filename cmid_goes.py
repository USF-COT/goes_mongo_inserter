import logging

import pyinotify
import threading

logger = logging.getLogger("CMID")

from pymongo import MongoClient

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

                    # Store in data array

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
