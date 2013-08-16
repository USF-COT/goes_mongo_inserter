"""
This file is here to bridge the gap to the new system.
It inserts ADCP data into the PostgreSQL database for COMPS.

By: Michael Lindemuth
"""

import os
import psycopg2

import logging
logger = logging.getLogger("GMI")

from itertools import izip


def insert_to_pgsql(pd0_data, config):
    host = os.environ.get('COMPSDB_HOST', 'localhost')
    port = os.environ.get('COMPSDB_PORT', '5432')
    dbname = os.environ.get('COMPSDB_DATABASE', 'comps')
    user = os.environ.get('COMPSDB_USER', 'postgres')
    password = os.environ.get('COMPSDB_PASS', '')

    table = config['pg_table']

    conn_string = (
        'host=%s dbname=%s port=%s '
        'user=%s password=%s' % (host, dbname, port, user, password)
    )

    conn = psycopg2.connect(conn_string)

    cur = conn.cursor()

    timestamp = pd0_data['timestamp']
    bin_1_distance = pd0_data['fixed_leader']['bin_1_distance']
    depth_cell_length = pd0_data['fixed_leader']['depth_cell_length']
    last_good = pd0_data['qaqc']['bottom_stats']['last_good_counter']

    for i, dataset in enumerate(izip(pd0_data['velocity']['data'][:last_good],
                                     pd0_data['current_speed']['data'][:last_good],  # NOQA
                                     pd0_data['current_direction']['data'][:last_good]),  # NOQA
                                start=1):

        eastward = dataset[0][0]
        northward = dataset[0][1]
        speed = dataset[1]
        direction = dataset[2]
        depth = bin_1_distance + depth_cell_length * (i-1)  # i starts at 1
        cur.execute('INSERT INTO %s '
                    '(timestamp, bin_number, '
                    'eastward_current, northward_current,'
                    'current_speed, current_direction, water_depth) VALUES '
                    '(\'%s\', %s, %s, %s, %s, %s, %s)'
                    % (table, timestamp, i, eastward, northward,
                       speed, direction, depth)
                    )

    conn.commit()
    cur.close()
    conn.close()
