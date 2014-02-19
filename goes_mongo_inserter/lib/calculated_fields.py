"""
   calculated_fields.py - Calculates values for fields that are derived from
       parsed fields.

    Each defined parser takes in two arguments:

    * config - A line that defines the name, type, and type arguments for a
        calculated field.  For example, a sample salinity calculated field
        config would look like:

            {
                "name": "salinity_1",
                "type": "salinity",
                "conductivity": "conductivity_1",
                "temperature": "temperature_1",
                "pressure": 1
            }

         If any field value is an integer, it will be interpreted as a constant
         for the calculation

    * data - All previously parsed data stored as a dictionary.

    By: Michael Lindemuth
"""


def parse_calc_param(config, field, data):
    """
    Retrieves a either a constant value or data field value.

    - Constant is retrieved if the field value is and int, long, or float.
    - Data field value is retrieved if the field value is a string.
    """

    if isinstance(config[field], (int, long, float)):
        return config[field]
    else:
        return data[config[field]]


def compute_salinity(conductivity, temperature, pressure):
    """
    Conductivity to salinity conversion using 1978 Practical Salinity
    Scale Equations from IEEE Journal of Ocean Engineering,
    Vol. OE-5, No. 1, January 1980, page 14,
    supplied by Sea-Bird Electronics Inc. SBE 37-IM MicroCAT Users Manuel
    conductivity is non-normalized conductivity in miilisiemens per cm
    temperature is in degrees Celcius
    pressure is in pa
    """

    salinity = -99

    if (conductivity >= 10 and conductivity <= 80 and
            temperature >= 0 and temperature <= 50):
        R = conductivity / 42.914

        aa = [2.07e-5, -6.37e-10, 3.989e-15]
        bb = [0.03426, 0.0004464, 0.4215, -0.003107]

        rp = (
            1.0+(pressure*(aa[0]+aa[1]*pressure+aa[2]*pressure*pressure))
            / (1.0+bb[0]*temperature+bb[1]*temperature*temperature +
                bb[2]*R+bb[3]*R*temperature)
        )

        c = [0.6766097, 0.0200564, 1.104259e-4, -6.9698e-7, 1.0031e-9]

        rt = (
            c[0] +
            c[1]*temperature +
            c[2]*pow(temperature, 2) +
            c[3]*pow(temperature, 3) +
            c[4]*pow(temperature, 4)
        )

        Rr = R/(rp*rt)
        # constants
        a = [0.0080, -0.1692, 25.3851, 14.0941, -7.0261, 2.7081]
        b = [0.0005, -0.0056, -0.0066, -0.0375, 0.0636, -0.0144]

        salinity = (
            a[0] +
            a[1]*pow(Rr, 0.5) +
            a[2]*Rr +
            a[3]*pow(Rr, 1.5) +
            a[4]*pow(Rr, 2.0) +
            a[5]*pow(Rr, 2.5)
        )
        salinity = (
            salinity +
            (temperature-15.0) *
            (b[0] +
                b[1]*pow(Rr, 0.5) +
                b[2]*Rr +
                b[3]*pow(Rr, 1.5) +
                b[4]*pow(Rr, 2.0) +
                b[5]*pow(Rr, 2.5))
            / (1.0 + 0.0162*(temperature-15.0))
        )

        salinity = round(salinity, 3)

    return salinity


def salinity_parser(config, data):
    """
    Parses fields for salinity and returns salinity calculation
    """

    conductivity = parse_calc_param(config, 'conductivity', data)
    temperature = parse_calc_param(config, 'temperature', data)
    pressure = parse_calc_param(config, 'pressure', data)

    return compute_salinity(conductivity, temperature, pressure)


calculated_fields_parsers = {
    'salinity': salinity_parser
}


def parse_calculated_fields(line_data, config, part):
    """
    Iterates through calculated_fields array in station configuration
    and parses known calculated field types.
    """

    for calc_config in config['fields']:
        if calc_config['type'] in calculated_fields_parsers:
            line_data[calc_config['name']] = (
                calculated_fields_parsers[calc_config['type']](calc_config,
                                                               line_data)
            )

    return None
