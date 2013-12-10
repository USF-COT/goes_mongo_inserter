import unittest

from gmi import load_configs
from trdi_adcp_readers.readers import read_PD15_file
from lib.trdi_adcp_parser import convert_to_COMPS_units

configs = load_configs('../example_config')


class TestGMIADCP(unittest.TestCase):
    def setUp(self):
        if '140B97C6' in configs:
            config = configs['140B97C6']
            self.pd0_data = read_PD15_file('../test_data/140B97C6',
                                           config['line_offset'])
        else:
            raise NotImplemented('No configuration found for 140B97C6')

    def test_ADCP_convert_to_COMPS(self):
        comps_data = convert_to_COMPS_units(self.pd0_data)
        print comps_data


from lib.calculated_fields import compute_salinity


class TestSalinityCalculation(unittest.TestCase):
    def test_salinity(self):
        cond = 51.1662
        temp = 22.6275
        depth = 1
        salinity = 35.438

        self.assertEqual(salinity, compute_salinity(cond, temp, depth))


if __name__ == '__main__':
    unittest.main()
