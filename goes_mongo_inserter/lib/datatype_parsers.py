
import logging
logger = logging.getLogger("GMI")

from datetime import datetime
from pytz import timezone


def parse_int(line_data, field_desc, part):
    if 'ignore' in field_desc:
        ignore_vals = field_desc['ignore'].split(',')
        if part in ignore_vals:
            return None

    retVal = int(part)
    if 'multiplier' in field_desc:
        retVal *= int(field_desc['multiplier'])

    return retVal


def parse_float(line_data, field_desc, part):
    if 'ignore' in field_desc:
        ignore_vals = field_desc['ignore'].split(',')
        if part in ignore_vals:
            return None

    retVal = float(part)
    if 'multiplier' in field_desc:
        retVal *= float(field_desc['multiplier'])

    if 'divisor' in field_desc:
        retVal /= float(field_desc['divisor'])

    if 'round' in field_desc:
        retVal = round(retVal, int(field_desc['round']))

    return retVal


def parse_text(line_data, field_desc, part):
    if 'ignore' in field_desc:
        ignore_vals = field_desc['ignore'].split(',')
        if part in ignore_vals:
            return None

    return part


def parse_timestamp(line_data, field_desc, part):
    fmt = field_desc['format']
    tz = timezone(field_desc['timezone'])

    if 'part' in field_desc:
        if field_desc['part'] == 'date':
            dt = datetime.strptime(part, fmt)
            dt = tz.localize(dt)
            if 'timestamp' in line_data:
                dt = datetime.combine(dt.date(), line_data['timestamp'])
        elif field_desc['part'] == 'time':
            dt = datetime.strptime(part, fmt)
            dt = tz.localize(dt)
            if 'timestamp' in line_data:
                dt = datetime.combine(line_data['timestamp'], dt.time())
    else:
        dt = datetime.strptime(part, fmt)
        dt = tz.localize(dt)

    return dt


def parse_julian(line_data, field_desc, data):
    part = field_desc['part']

    if part == 'year':
        return int(data)
    elif part == 'day':
        julian_day = int(data)
        julian_string = "%d %d" % (line_data['julian'], julian_day)
        dt = datetime.strptime(julian_string, "%Y %j")
        return dt


def convert_merged_degrees_minutes(degrees_minutes):
    degrees = 0

    decimal_place = degrees_minutes.find('.')

    if decimal_place == -1:
        logger.error('Unable to parse GPS string: '
                     'No decimal place '
                     'present in string (%s)' % (degrees_minutes,))
        return -1

    deg_str = degrees_minutes[0:decimal_place-2]
    min_str = degrees_minutes[decimal_place-2:]

    try:
        degrees = float(deg_str)
        minutes = float(min_str)
        if degrees < 0:
            minutes *= -1

        degrees += minutes/60
        return degrees
    except:
        return -1


def parse_point_degrees_minutes(line_data, field_desc, part):
    gps_pos = [0, 0]
    name = field_desc['name']
    if name in line_data:
        gps_pos = line_data[name]

    value = convert_merged_degrees_minutes(part)
    if field_desc['component'] == 'lng':
        # For some reason GOES returns a positive
        # longitude when it should be negative
        gps_pos[0] = value * -1
    elif field_desc['component'] == 'lat':
        gps_pos[1] = value

    return gps_pos


def convert_directional_degrees_minutes(degrees_minutes_direction):
    coordinates = degrees_minutes_direction[:-1]
    direction = degrees_minutes_direction[-1]

    position = convert_merged_degrees_minutes(coordinates)
    if direction == 'S' or direction == 'W':
        position *= -1

    return position


def parse_point_degrees_minutes_direction(line_data, field_desc, part):
    name = field_desc['name']
    if name in line_data:
        gps_pos = line_data[name]
    else:
        gps_pos = [0, 0]

    value = convert_directional_degrees_minutes(part)
    if field_desc['component'] == 'lng':
        gps_pos[0] = value
    elif field_desc['component'] == 'lat':
        gps_pos[1] = value

    return gps_pos


from calculated_fields import parse_calculated_fields

datatype_parsers = {
    'int': parse_int, 'float': parse_float, 'text': parse_text,
    'timestamp': parse_timestamp,
    'julian': parse_julian,
    'point_degrees_minutes': parse_point_degrees_minutes,
    'point_degrees_minutes_direction': parse_point_degrees_minutes_direction,
    'calculated': parse_calculated_fields
}
