import math

import logging
logger = logging.getLogger("GMI")

from adcp_qartod_qaqc.trdi import TRDIQAQC

from trdi_adcp_psql_inserter import insert_to_pgsql

from datetime import datetime

# NOTE Assumes only one ensemble given from TRDI ADCP


def gen_qaqc_doc_part(qaqc):
    """
    Generates Quality Assurance and Control Flags
    then returns a document ready to be
    inserted into a Mongo collection
    """

    qaqc_doc = {}
    qaqc_doc['bottom_stats'] = qaqc.bottom_stats
    qaqc_doc['battery_power'] = qaqc.battery_flag()
    qaqc_doc['checksum'] = qaqc.checksum_flag()
    qaqc_doc['TRDI_BIT_test'] = qaqc.bit_flag()
    qaqc_doc['sensor_orientation'] = qaqc.orientation_flags()
    qaqc_doc['speed_of_sound'] = qaqc.sound_speed_flags()
    qaqc_doc['correlation_magnitude'] = qaqc.correlation_magnitude_flags()
    qaqc_doc['percent_good'] = qaqc.percent_good_flags()
    qaqc_doc['current_speed'] = qaqc.current_speed_flags()
    qaqc_doc['current_direction'] = qaqc.current_direction_flags()
    qaqc_doc['horizontal_velocity'] = qaqc.horizontal_velocity_flags()
    qaqc_doc['vertical_velocity'] = qaqc.vertical_velocity_flags()
    qaqc_doc['error_velocity'] = qaqc.error_velocity_flags()
    qaqc_doc['echo_intensity'] = qaqc.echo_intensity_flags()
    qaqc_doc['range_drop_off'] = qaqc.range_drop_off_flags()
    qaqc_doc['current_gradient'] = qaqc.current_speed_gradient_flags()
    return qaqc_doc


def convert_to_COMPS_units(pd0_data):
    # Convert to meters and add transducer depth to bin 1 distance
    pd0_data['fixed_leader']['bin_1_distance'] = (
        pd0_data['fixed_leader']['bin_1_distance']/100.0 + 1.04
    )

    # Convert bin height to meters
    pd0_data['fixed_leader']['depth_cell_length'] = (
        pd0_data['fixed_leader']['depth_cell_length']/100.0
    )

    variable_adjustments = {
        'heading': lambda x: x/100.0,  # 0.01 degrees to degrees
        'pitch': lambda x: x/100.0,  # 0.01 degrees to degrees
        'roll': lambda x: x/100.0,  # 0.01 degrees to degrees
        'temperature': lambda x: x/100.0,  # 0.01 degrees to degrees
    }

    for k, f in variable_adjustments.items():
        pd0_data['variable_leader'][k] = f(pd0_data['variable_leader'][k])

    # Extract Timestamp
    vl = pd0_data['variable_leader']  # Readability

    year = 2000 + vl['rtc_year']
    month = vl['rtc_month']
    day = vl['rtc_day']
    hour = vl['rtc_hour']
    minute = vl['rtc_minute']
    second = vl['rtc_second']

    pd0_data['timestamp'] = datetime(year, month, day,
                                     hour, minute, second)

    # Velocity mm/s to cm/s
    for bin in pd0_data['velocity']['data']:
        bin[:] = [beam / 10.0 for beam in bin]

    # Calculate Current Speed and Direction
    pd0_data['current_speed'] = {}
    pd0_data['current_speed']['data'] = []

    pd0_data['current_direction'] = {}
    pd0_data['current_direction']['data'] = []

    for bin in pd0_data['velocity']['data']:
        u = bin[0]
        v = bin[1]
        z = u + 1j + v

        pd0_data['current_speed']['data'].append(abs(z))
        direction = math.atan2(z.real, z.imag) * 180/math.pi
        pd0_data['current_direction']['data'].append(direction)

    ## Echo Intensity to decibels
    #for bin in pd0_data['echo_intensity']['data']:
    #    bin[:] = [beam * 0.45 for beam in bin]

    return pd0_data


def parse_trdi_PD0(pd0_data, config, file_object_id, mongo_db):
    logger.info(pd0_data)
    doc = convert_to_COMPS_units(pd0_data)
    doc['file_id'] = file_object_id

    qaqc = TRDIQAQC(pd0_data)
    doc['qaqc'] = gen_qaqc_doc_part(qaqc)

    mongo_collection = mongo_db[config['station']+'.ADCP']
    mongo_collection.insert(doc)

    if 'insert_to_pgsql' in config and config['insert_to_pgsql']:
        insert_to_pgsql(doc, config)
