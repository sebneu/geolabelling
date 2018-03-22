
import argparse
import logging
import xml.etree.ElementTree as ET
import os
import logging
import itertools

import requests
import urllib
import time

from pymongo import MongoClient


def rewrite_geojson(client, args):
    db = client.geostore
    geonames = db.geonames
    for gn in geonames.find({'geojson': {'$exists': True}}):
        if isinstance(gn['geojson'], list):
            coords = gn['geojson']
            geonames.update_one({'_id': gn['_id']}, {'$set': {'geojson':
                                                                  {
                                                                      'type': 'Point',
                                                                      'coordinates': [coords[0], coords[1]]
                                                                  }
            }})

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='fix-geo-store')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=27018)
    #filename = datetime.now().strftime('%Y-%m-%d') + '.log'
    parser.add_argument('--logfile')
    parser.add_argument('--loglevel', type=str, default='debug')

    subparsers = parser.add_subparsers()

    subparser = subparsers.add_parser('rewrite-geojson')
    subparser.set_defaults(func=rewrite_geojson)
    subparser.add_argument('--directory', default='poly-exports/osm-export/')

    args = parser.parse_args()

    if args.loglevel == 'info':
        lvl = logging.INFO
    elif args.loglevel == 'debug':
        lvl = logging.DEBUG
    else:
        lvl = logging.DEBUG
    logging.basicConfig(level=lvl, filename=args.logfile, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    client = MongoClient(args.host, args.port)
    args.func(client, args)