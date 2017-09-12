# coding=utf-8
import unittest

from geonames_mappings import NominatimService

class NominatimTestCase(unittest.TestCase):
    def setUp(self):
        self.service = NominatimService()

    def test_streets_for_geoname(self):
        streets = self.service.streets_for_geoname('http://sws.geonames.org/2761055/')

        self.assertNotEqual(streets, None)

    def test_address_for_string(self):
        streets = self.service.address_for_string('Winkl, Koppl, Österreich')

        self.assertNotEqual(streets, None)


if __name__ == '__main__':
    unittest.main()
