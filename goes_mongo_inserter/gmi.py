#!/usr/bin/python

# GOES Mongo Inserter Daemon
#
# A configurable daemon that monitors a given
# directory for new GOES data from COMPS buoys.
# When new data is detected, it parses the file
# according to a given format and inserts it into a mongo database.

import logging
import json

import pyinotify
import os
import sys

import argparse
from goes_mongo_inserter.lib.parsers import GOESUpdateHandler


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("config_path", help="Path to directory containing "
                                            "GOES parser configuration files")
    parser.add_argument("GOES_directory", help="Path to directory containing "
                        "live GOES files.  This folder will be monitored for "
                        "file writes and parsed if a configuration is "
                        "specified for it in the config_path")
    args = parser.parse_args()

    # Setup Logger
    logger = logging.getLogger("GMI")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s "
                                  "- %(levelname)s - %(message)s")
    handler = logging.FileHandler('/var/log/gmi/gmi.log')  # NOQA
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Load Configurations
    configs = {}
    for filename in os.listdir(args.config_path):
        if filename[0] == '.':
            continue

        config_contents = ''
        with open(args.config_path+'/'+filename, 'r') as f:
            config_contents = f.read()

            try:
                conf = json.loads(config_contents)
            except Exception, e:
                logger.error('Error processing %s: %s' % (filename, e))
                return 0

            for k, v in conf.items():
                configs[k] = v

    # Setup PyInotify
    wm = pyinotify.WatchManager()
    handler = GOESUpdateHandler(configs)
    notifier = pyinotify.Notifier(wm, handler)
    mask = pyinotify.IN_CLOSE_WRITE
    wm.add_watch(args.GOES_directory, mask)
    pid_file = '/var/run/gmi/gmi.pid'
    try:
        logger.info("Starting")
        notifier.loop(daemonize=True, pid_file=pid_file)
    except pyinotify.NotifierError, err:
        logger.error('Unable to start notifier loop: %s' % (err))
        return 0

    logger.info("GOES Mongo Inserter Exited Successfully")

    return 1

if __name__ == '__main__':
    sys.exit(main())
