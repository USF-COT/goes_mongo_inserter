#!/usr/bin/python

# CMID - COMPS Mongo Inserter Daemon
#
# A configurable daemon that monitors a given directory for new GOES data from COMPS buoys.
# When new data is detected, it parses the file according to a given format and inserts it into a mongo database.

import logging
import json

import pyinotify
import os
import sys
import signal

import threading

from cmid_goes import *

# Check arg vector length
if len(sys.argv) != 2:
    print "Unknown usage.  Must enter: python cmid.py start|stop"
    sys.exit(1)

# Setup Logger
logger = logging.getLogger("CMID")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler = logging.FileHandler('/var/log/cmid/cmid.log')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Load Configurations
configs = {}
config_path = '/etc/cmid'
for filename in os.listdir(config_path):
    config_contents = ''
    with open(config_path+'/'+filename, 'r') as f:
        config_contents = f.read()
        conf = json.loads(config_contents)
        for k,v in conf.items():
            configs[k] = v

# Setup PyInotify
wm = pyinotify.WatchManager()
handler = GOESUpdateHandler(configs)
notifier = pyinotify.Notifier(wm, handler)
mask = pyinotify.IN_CLOSE_WRITE 
wm.add_watch('/DATA/GOES_Raw', mask)

pid_file = '/var/run/cmid.pid'

if sys.argv[1] == 'start':
    # Start Daemon
    logger.info("Started")
    try:
        notifier.loop(daemonize=True, pid_file=pid_file)
    except pyinotify.NotifierError, err:
        logger.error('Unable to start notifier loop: %s' % (err))
elif sys.argv[1] == 'debug':
    # Start Daemon
    logger.info("Started in Debug Mode")
    try:
        notifier.loop()
    except pyinotify.NotifierError, err:
        logger.error('Unable to start notifier loop: %s' % (err))
elif sys.argv[1] == 'stop':
    if os.path.exists(pid_file):
        with open(pid_file, 'r') as f:
            pid = int(f.readline())
            os.kill(pid, signal.SIGTERM)
        os.remove(pid_file)

    logger.info("Stopped")
else:
    print sys.argv
    print "Unknown usage.  Must enter: python cmid.py start|stop"
    sys.exit(1)
