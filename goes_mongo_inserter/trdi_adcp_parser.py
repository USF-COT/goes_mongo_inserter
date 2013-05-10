import os
from tempfile import mkstemp
from datetime import datetime
import numpy as np
import numpy.ma as ma

import logging
logger = logging.getLogger("GMI")

from rdi_pd15.pd0_converters import PD15_string_to_PD0
from pycurrents.adcp.rdiraw import Multiread
from adcp_qartod_qaqc.trdi import TRDIQAQC

# NOTE Assumes only one ensemble given from TRDI ADCP


def gen_adcp_qaqc_doc(pd0_data, config):
    """
    Generates Quality Assurance and Control Flags
    then returns a document ready to be
    inserted into a Mongo collection
    """

    qaqc = TRDIQAQC(pd0_data, transducer_depth=config['transducer_depth'])
    bottom_stats = qaqc.ensemble_bottom_stats[0]

    qaqc_doc = {}
    qaqc_doc['sample_number'] = pd0_data.ens_num[0]
    qaqc_doc['number_good_bins'] = bottom_stats['last_good_counter']
    qaqc_doc['water_depth'] = bottom_stats['range_to_bottom']
    qaqc_doc['battery_power'] = qaqc.battery_flag()
    qaqc_doc['checksum'] = qaqc.checksum_flag()
    qaqc_doc['TRDI_BIT_test'] = qaqc.bit_flag()
    qaqc_doc['sensor_orientation'] = qaqc.orientation_flags()[0]
    qaqc_doc['speed_of_sound'] = qaqc.sound_speed_flags()[0]
    qaqc_doc['correlation_magnitude'] = qaqc.correlation_magnitude_flags()[0]
    qaqc_doc['percent_good'] = qaqc.percent_good_flags()[0]
    qaqc_doc['current_speed'] = qaqc.current_speed_flags()
    qaqc_doc['current_direction'] = qaqc.current_direction_flags()
    qaqc_doc['horizontal_velocity'] = qaqc.horizontal_velocity_flags()
    qaqc_doc['vertical_velocity'] = qaqc.vertical_velocity_flags()
    qaqc_doc['error_velocity'] = qaqc.error_velocity_flags()
    qaqc_doc['echo_intensity'] = qaqc.echo_intensity_flags()[0]
    qaqc_doc['range_drop_off'] = qaqc.range_drop_off_flags()[0]
    qaqc_doc['current_gradient'] = qaqc.current_speed_gradient_flags()

    return (qaqc_doc, bottom_stats['last_good_counter'])


def gen_adcp_config_doc(pd0_data, config):
    """
    Generates configuration and diagnostic
    parameters of the ADCP and returns a
    document ready to be inserted into a
    Mongo collection
    """

    config_doc = {}
    config_doc['speed_of_sound'] = pd0_data.VL[0][10]
    config_doc['pitch'] = pd0_data.pitch[0]
    config_doc['roll'] = pd0_data.roll[0]
    config_doc['heading'] = pd0_data.heading[0]
    config_doc['distance_to_bin_1'] = pd0_data.Bin1Dist
    config_doc['transducer_depth'] = config['transducer_depth']
    config_doc['depth_of_bin_1'] = (pd0_data.Bin1Dist +
                                    config['transducer_depth'])
    config_doc['temperature'] = pd0_data.temperature[0]
    config_doc['number_of_bins'] = pd0_data.NCells
    config_doc['instrument_frequency'] = pd0_data.sysconfig['kHz']
    config_doc['beam_angle_degrees'] = pd0_data.sysconfig['angle']
    config_doc['bin_size'] = pd0_data.FL.CellSize
    config_doc['blanking'] = pd0_data.FL.Blank
    config_doc['pings_per_ensemble'] = pd0_data.FL.NPings
    config_doc['transmit_lag'] = pd0_data.FL.TransLag
    config_doc['transmit_pulse'] = pd0_data.FL.Pulse

    TPmin = str(pd0_data.FL.TPP_min).zfill(2)     # ping interval - minutes
    TPsec = str(pd0_data.FL.TPP_sec).zfill(2)     # ping interval - seconds
    TP100 = str(pd0_data.FL.TPP_hun).zfill(2)     # ping interval - hundre
    # ping interval text string
    ping_interval = ':'.join([TPmin, TPsec, TP100])

    config_doc['time_between_pings'] = ping_interval
    config_doc['magnetic_declination'] = pd0_data.FL.EV / 10.0

    return config_doc


def gen_adcp_data_doc(pd0_data, last_good_counter,
                      file_object_id, qaqc_id, config_id):
    """
    Generates a document of ADCP data that is ready to
    be inserted into a Mongo collection
    """

    yr = str(pd0_data.yearbase)
    mon = str(pd0_data.VL[0][2]).zfill(2)
    day = str(pd0_data.VL[0][3]).zfill(2)
    hr = str(pd0_data.VL[0][4]).zfill(2)
    min = str(pd0_data.VL[0][5]).zfill(2)
    sec = str(pd0_data.VL[0][6]).zfill(2)
    dt = '/'.join([yr, mon, day])
    tm = ':'.join([hr, min, sec])
    time_str = ' '.join([dt, tm])
    timestamp = datetime.strptime(time_str, '%Y/%m/%d %H:%M:%S')

    data_doc = {}
    data_doc['timestamp'] = timestamp
    data_doc['goes_file_id'] = file_object_id
    data_doc['qaqc_id'] = qaqc_id
    data_doc['config_id'] = config_id

    # Calculate Velocities
    u_vel_masked = pd0_data.vel1
    u_vel_masked.mask = ma.nomask
    u_vel = u_vel_masked.compressed()
    u = u_vel * 100.

    v_vel_masked = pd0_data.vel2
    v_vel_masked.mask = ma.nomask
    v_vel = v_vel_masked.compressed()
    v = v_vel * 100.

    w_vel_masked = pd0_data.vel3
    w_vel_masked.mask = ma.nomask
    w_vel = w_vel_masked.compressed()
    w = w_vel * 100.

    z = u + 1j * v
    current_speed = abs(z)
    current_direction = np.arctan2(z.real, z.imag) * 180 / np.pi
    current_negatives = np.where(current_direction < 0.0)
    current_direction[current_negatives] += 360

    data_doc['eastward_current'] = u[:last_good_counter]
    data_doc['northward_current'] = v[:last_good_counter]
    data_doc['upward_current'] = w[:last_good_counter]
    data_doc['current_speed'] = current_speed[:last_good_counter]
    data_doc['current_direction'] = current_direction[:last_good_counter]

    return data_doc


def trdi_pd15_line_to_mongo(line_data, config,
                            file_object_id, mongo_collection):
    print line_data
    if len(line_data) > 0:
        data = PD15_string_to_PD0(line_data)
    else:
        logger.error('No ADCP data found in specified GOES file.')
        return

    if data is not None:
        temp_file = mkstemp()
        with open(temp_file[1], 'wb') as f:
            f.write(data)

        print temp_file[1]

        pd0_data = Multiread(temp_file[1], config['multiread_mode']).read()
        os.remove(temp_file[1])

        (qaqc_doc, last_good_counter) = gen_adcp_qaqc_doc(pd0_data, config)
        qaqc_id = mongo_collection.insert(qaqc_doc)

        adcp_config_doc = gen_adcp_config_doc(pd0_data, config)
        adcp_config_id = mongo_collection.insert(adcp_config_doc)

        data_doc = gen_adcp_data_doc(pd0_data, last_good_counter,
                                     file_object_id, qaqc_id, adcp_config_id)
        mongo_collection.insert(data_doc)


def parse_trdi_pd15(path, config, file_object_id, mongo_db):
    with open(path, 'r') as f:
        # Bleed off headers
        for i in xrange(0, config['line_offset']-1):
            f.readline()

        # Get PD15 data line
        line_data = f.readline()

    mongo_collection = mongo_db[config['station']+'.ADCP']
    trdi_pd15_line_to_mongo(line_data, config,
                            file_object_id, mongo_collection)
