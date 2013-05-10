
import logging
logger = logging.getLogger("GMI")

from datatype_parsers import datatype_parsers


def parse_line(config, prefix, line_parts, file_object_id):
    line_data = {}

    line_data['goes_file_id'] = file_object_id

    line_config = config['lines'][prefix]

    # Parse text fields
    for i, part in enumerate(line_parts[1:]):
        if i < len(line_config):
            field_desc = line_config[i]
            data_key = field_desc['name']
            if 'units' in field_desc:
                data_key += "-%s" % (field_desc['units'],)

            field_type = field_desc['type']
            if field_type in datatype_parsers:
                value = datatype_parsers[field_type](line_data,
                                                     field_desc,
                                                     part.strip())
                if value is not None:
                    line_data[data_key] = value
            else:
                logger.error('Unknown type %s in configuration file line '
                             '%s, field %s' % (field_type,
                                               line_parts[0],
                                               field_desc['name']))
        else:
            logger.error('Exceeded specified number of field '
                         'descriptors.  Ignoring remaining fields')

    return line_data


def parse_prefixed_lines(path, config, file_object_id, mongo_db):
    """
    Parses GOES files containing mixed sensor data.
    Each line is prefixed according to what sensor reported
    the data.
    """

    # Bleed off headers to get to data
    with open(path, 'r') as f:
        offset = config['line_offset']
        while offset > 0:
            f.readline()
            offset = offset - 1

        data = []
        for line in f:
            line_parts = line.split(',')
            prefix = None
            if line_parts[0] in config['lines']:
                prefix = line_parts[0]
            elif 'NOHEADER' in config['lines']:
                prefix = 'NOHEADER'
            else:
                logger.info("Ignoring line with unknown prefix"
                            "in file %s: %s" % (path,
                                                line_parts[0]))

            if prefix is not None:
                line_data = parse_line(config=config,
                                       prefix=prefix,
                                       line_parts=line_parts,
                                       file_object_id=file_object_id)

            if len(line_data) > 0:
                line_data['prefix'] = prefix
                line_data['file_id'] = file_object_id
                data.append(line_data)

    # Bulk INSERT data
    if len(data) > 0:
        mongo_collection = mongo_db[config['station']+'.env']
        mongo_collection.insert(data)
