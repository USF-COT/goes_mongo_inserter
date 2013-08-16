
import logging
logger = logging.getLogger("GMI")

from datatype_parsers import datatype_parsers
from itertools import izip
from datetime import datetime


def parse_line(fields, line_parts):
    line_data = {}

    for field_desc, data in izip(fields, line_parts):
        if 'skip_field' in field_desc and field_desc['skip_field']:
            continue  # Skip field

        data_key = field_desc['name']
        if 'units' in field_desc:
            data_key += '-%s' % (field_desc['units'],)

        field_type = field_desc['type']
        if field_type in datatype_parsers:
            value = datatype_parsers[field_type](line_data,
                                                 field_desc,
                                                 data.strip())
            if value is not None:
                line_data[data_key] = value
        else:
            logger.error('Unknown type %s in configuration '
                         'file for field %s' % (data_key,))

    return line_data


def parse_data_line(diagnostics, field_config, line_parts):
    data = parse_line(field_config, line_parts)

    if 'julian' in diagnostics and 'timestamp' in data:
        time = data['timestamp']
        dt = datetime.combine(diagnostics['julian'], time.time())
        data['timestamp'] = dt

    return data


def parse_coastal_station(path, config, file_object_id, mongo_db):
    """
    Parses GOES files containing mixed sensor data.
    The diagnostic_line parameter contains the line number
    for coastal station diagnostic information which also stores
    the year and date of the reading.  The rest is read according to
    a lines configuration setting.
    """

    # Bleed off headers to get to data
    data = []
    with open(path, 'r') as f:
        offset = config['line_offset']
        while offset > 0:
            f.readline()
            offset = offset - 1

        diagnostic_line_number = (
            config['diagnostic_line'] - config['line_offset']
        )
        while diagnostic_line_number > 0:
            f.readline()
            diagnostic_line_number = diagnostic_line_number - 1

        diagnostic_line = f.readline()
        diagnostic_line_parts = diagnostic_line.split()

        diagnostics = parse_line(config['diagnostic_fields'],
                                 diagnostic_line_parts)
        if len(diagnostics) == 0:
            logger.error('Error parsing diagnostics '
                         'for station %s' % (config['station'],))
            return

        # Insert Diagnostic Document
        diagnostics['file_id'] = file_object_id
        diagnostic_collection = mongo_db[config['station']+'.diagnostics']
        diagnostic_id = diagnostic_collection.insert(diagnostics)

        # Read remainder of lines with diagnostic information
        for line in f:
            line_parts = line.split()
            line_data = parse_data_line(diagnostics=diagnostics,
                                        field_config=config['data_fields'],
                                        line_parts=line_parts)

            if len(line_data) > 0:
                line_data['diagnostic_id'] = diagnostic_id
                line_data['file_id'] = file_object_id
                data.append(line_data)

    # Bulk INSERT data
    if len(data) > 0:
        mongo_collection = mongo_db[config['station']+'.env']
        mongo_collection.insert(data)
