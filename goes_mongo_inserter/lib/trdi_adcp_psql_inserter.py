"""
This file is here to bridge the gap to the new system.
It inserts ADCP data into the PostgreSQL database for COMPS.

By: Michael Lindemuth
"""

import os
import psycopg2

import logging
log = logging.getLogger("GMI")

from itertools import izip


class Old_COMPS_DB(object):
    def __init__(self, connection_string):
        self.conn = psycopg2.connect(connection_string)
        self.cur = self.conn.cursor()


def insert_to_pgsql(pd0_data, config):
    table = config['pg_table']

    conn_strings_env = os.environ.get('OLD_DBS_CONNECTION_STRINGS')
    conn_strings = conn_strings_env.split(',')

    connections = []
    for conn_string in conn_strings:
        try:
            conn = Old_COMPS_DB(conn_string)
            connections.append(conn)
        except Exception, e:
            log.error('Error connecting to database with parameters %s. '
                      'Message: %s' % (conn_string, e))

    timestamp = pd0_data['timestamp']
    bin_1_distance = pd0_data['fixed_leader']['bin_1_distance']
    depth_cell_length = pd0_data['fixed_leader']['depth_cell_length']
    last_good = pd0_data['qaqc']['bottom_stats']['last_good_counter']

    for i, dataset in enumerate(izip(pd0_data['velocity']['data'][:last_good],
                                     pd0_data['current_speed']['data'][:last_good],  # NOQA
                                     pd0_data['current_direction']['data'][:last_good]),  # NOQA
                                start=1):

        eastward = round(dataset[0][0], 2)
        northward = round(dataset[0][1], 2)
        speed = round(dataset[1], 2)
        direction = round(dataset[2], 2)
        depth = float(bin_1_distance) + float(depth_cell_length) * (i-1)
        for conn in connections:
            try:
                conn.cur.execute('INSERT INTO %s '
                                 '(timestamp, bin_number, '
                                 'eastward_current, northward_current,'
                                 'current_speed, current_direction, water_depth) '  # NOQA
                                 'VALUES (\'%s\', %s, %s, %s, %s, %s, %s)'
                                 % (table, timestamp, i, eastward, northward,
                                    speed, direction, depth)
                                 )
            except Exception, e:
                log.error('Error inserting ADCP data '
                          'into old COMPS database. Error: %s' % e)

    for connection in connections:
        connection.conn.commit()
        connection.cur.close()
        connection.conn.close()
