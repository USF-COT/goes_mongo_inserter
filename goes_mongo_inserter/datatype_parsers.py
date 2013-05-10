
import logging
logger = logging.getLogger("GMI")

from datetime import datetime
from pytz import timezone


def parse_int(line_data, field_desc, part):
    if 'ignore' in field_desc:
        ignore_vals = field_desc['ignore'].split(',')
        if part in ignore_vals:
            return None

    return int(part)


def parse_float(line_data, field_desc, part):
    if 'ignore' in field_desc:
        ignore_vals = field_desc['ignore'].split(',')
        if part in ignore_vals:
            return None

    return float(part)


def parse_text(line_data, field_desc, part):
    if 'ignore' in field_desc:
        ignore_vals = field_desc['ignore'].split(',')
        if part in ignore_vals:
            return None

    return part


def parse_timestamp(line_data, field_desc, part):
    fmt = field_desc['format']
    tz = timezone(field_desc['timezone'])

    dt = datetime.strptime(part, fmt)
    return tz.localize(dt)


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


datatype_parsers = {'int': parse_int, 'float': parse_float, 'text': parse_text,
                    'timestamp': parse_timestamp,
                    'point_degrees_minutes': parse_point_degrees_minutes}
