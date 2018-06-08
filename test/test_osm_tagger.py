# coding=utf-8
import unittest

from postal.parser import parse_address
from pymongo import MongoClient
from pyyacp.datatable import parseDataTables
from pyyacp.yacp import YACParser

from openstreetmap.osm_inserter import get_geonames_id
from services.geo_tagger import GeoTagger
from services.osm_tagger import OSMTagger


class OSMAddressTextCase(unittest.TestCase):
    def test_csv(self):
        client = MongoClient('localhost', 27017)
        tagger = OSMTagger(client)

        db = client.geostore
        q = db.geonames.find({'admin_level': 6, 'parent': "http://sws.geonames.org/2769848/", "country" : "http://sws.geonames.org/2782113/"})

        r_tmp = [get_geonames_id(r['_id']) for r in q]
        regions = []
        for r in r_tmp:
            regions.append(r)
            q = db.geonames.find({'admin_level': 8, 'parent': r,
                                  "country": "http://sws.geonames.org/2782113/"})
            for sub_r in q:
                regions.append(get_geonames_id(sub_r['_id']))

        yacp = YACParser(filename='testdata/AdressenJHB.csv', sample_size=1800)
        tables = parseDataTables(yacp)
        t = tables[0]
        for i, row in enumerate(t.columnIter()):
            tagger.label_values(row, regions)


    def test_string_column_labelling(self):
        tagger = GeoTagger('localhost', 27017)

        yacp = YACParser(filename='testdata/AdressenJHB.csv', sample_size=1800)
        tables = parseDataTables(yacp)
        t = tables[0]
        for i, row in enumerate(t.columnIter()):
            tagger.string_column(row)


    def test_metadata_text(self):
        c = 'Selbständige Ambulatorien des Landes Salzburg.'
        tmp2 = parse_address(c)
        #print('parse:' + str(tmp2))


if __name__ == '__main__':
    unittest.main()
