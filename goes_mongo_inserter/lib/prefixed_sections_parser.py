
import logging
logger = logging.getLogger("GMI")

from datatype_parsers import datatype_parsers
from trdi_adcp_readers.readers import read_PD15_string
from trdi_adcp_parser import parse_trdi_PD0

from update_latest_readings import update_env_latest


def parse_line(config, section, line, file_object_id, mongo_db):
    """
    Parses a line contained in a given section.
    Returns a dictionary of values found accroding
    to configuration
    """

    line_data = {}

    line_config = config['sections'][section]
    line_parts = line.split(',')

    # Parse text fields
    for i, field_desc in enumerate(line_config):
        if i < len(line_parts):
            part = line_parts[i]
        else:
            part = '-99'

        data_key = field_desc['name']
        if 'units' in field_desc:
            data_key += "-%s" % (field_desc['units'],)

        field_type = field_desc['type']
        if field_type == 'trdi_adcp_pd15':
            try:
                pd0_data = read_PD15_string(line)
                parse_trdi_PD0(pd0_data, field_desc,
                               file_object_id, mongo_db)
            except Exception, e:
                logger.error('Unable to parse file with PD15 '
                             'string for %s.  Exception %s' % (config['station'], e))  # NOQA
        elif field_type in datatype_parsers:
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

    return line_data


def parse_prefixed_sections(path, config, file_object_id, mongo_db):
    """
    Parses GOES files containing mixed sensor data.
    Each sensor has a section in the file with a known prefix before
    lines of data from that sensor.
    """

    # Bleed off headers to get to data
    with open(path, 'r') as f:
        offset = config['line_offset']
        while offset > 0:
            f.readline()
            offset = offset - 1

        section = None
        dataset_index = 0
        data = []
        for line in f:
            line = line.strip()
            if line in config['sections']:
                if len(config['sections'][line]) > 0:
                    section = line
                else:
                    section = None
                dataset_index = 0
            elif line == config['no_section_data_character']:
                logger.info('No section data character (%s) found for section '
                            '%s' % (config['no_section_data_character'],
                                    section))
                continue
            else:
                # Make sure we know what section we're on
                # Initialize a dictionary for dataset if necessary
                if section is not None:
                    if len(data) == dataset_index:
                        data.append({
                            'goes_file_id': file_object_id
                        })

                    dataset = data[dataset_index]
                    line_data = parse_line(config, section, line,
                                           file_object_id, mongo_db)
                    for k, v in line_data.items():
                        if k in dataset:
                            logger.info('Possible sensor conflict. '
                                        'Overriding %s with %s because they '
                                        'share the key %s.' % (
                                            dataset[k], v, k)
                                        )
                        dataset[k] = v

                    dataset_index += 1
                else:
                    logger.info('Skipping line: %s.  It is before '
                                'any known section specifier.' % (line,))

    # Bulk INSERT data
    if len(data) > 0:
        mongo_collection = mongo_db[config['station']+'.env']
        mongo_collection.insert(data)

    update_env_latest(mongo_db, config, data)
