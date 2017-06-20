import sys
import argparse
import logging
from collections import defaultdict
from datetime import datetime

import pymongo
from pymongo import MongoClient

import csv
import rdflib
from rdflib.namespace import RDF, RDFS, OWL
from rdflib import Namespace

GN = Namespace('http://www.geonames.org/ontology#')


def postalcode_csv_to_mongo(client, args):
    db = client.geostore
    keywords = db.keywords
    postalcodes_collection = db.postalcodes
    postalcodes = defaultdict(set)

    with open("local/allCountries.txt") as f:
        reader = csv.reader(f, delimiter='\t')
        for i, row in enumerate(reader):
            if i % 1000 == 0:
                logging.debug("parsed entries: " + str(i) + ", postal codes: " + str(len(postalcodes)))

            country, postalcode, localname, state, district, town = row[0].decode('utf-8'), row[1].decode('utf-8'), row[
                2].decode('utf-8'), row[3].decode('utf-8'), row[5].decode('utf-8'), row[7].decode('utf-8')
            l = localname.strip().lower()
            keywordentry = keywords.find_one({'_id': l})
            if keywordentry and 'geonames' in keywordentry:
                for geo_id in keywordentry['geonames']:
                    parent_names = get_all_parents(client, geo_id)
                    if town in parent_names and (district in parent_names or state in parent_names):
                        postalcodes[postalcode].add(geo_id)
            else:
                logging.debug("Try to find parts of local name in keywords: " + l)
                multiple_names = localname.split(',')
                if len(multiple_names) <= 1:
                    multiple_names = localname.split('-')
                if len(multiple_names) <= 1:
                    multiple_names = localname.split('/')
                if len(multiple_names) <= 1:
                    multiple_names = localname.split(' ')
                geo_id = None
                if len(multiple_names) > 1:
                    for n in multiple_names:
                        n = n.strip().lower()
                        # try again to find an entry
                        keywordentry = keywords.find_one({'_id': n})
                        if keywordentry and 'geonames' in keywordentry:
                            for geo_id in keywordentry['geonames']:
                                parent_names = get_all_parents(client, geo_id)
                                if town in parent_names and (district in parent_names or state in parent_names):
                                    postalcodes[postalcode].add(geo_id)
                if not geo_id:
                    logging.debug("Local name not in keywords: " + l)

        codestats = {'min_len': sys.maxint, 'max_len': 0, 'chars': set()}
        for c in postalcodes:
            try:
                postalcodes_collection.insert({'_id': c, 'geonames': list(postalcodes[c])})
            except pymongo.errors.DuplicateKeyError as e:
                logging.debug("Postal code already in mongodb: " + c)

            if len(c) < codestats['min_len']:
                codestats['min_len'] = len(c)
            if len(c) > codestats['max_len']:
                codestats['max_len'] = len(c)
            if not c.isdigit():
                for char in c:
                    if not char.isdigit():
                        codestats['chars'].add(char)
        import pprint
        pprint.pprint(codestats)


def _get_all_parents(geo_id, geonames_collection, all_names):
    current = geonames_collection.find_one({"_id": geo_id})
    if current and "parent" in current:
        if "name" in current:
            all_names.append(current["name"])
        _get_all_parents(current["parent"], geonames_collection, all_names)


def get_all_parents(client, geo_id):
    db = client.geostore
    geonames_collection = db.geonames
    names = []
    _get_all_parents(geo_id, geonames_collection, names)
    return names


def geonames_to_mongo(client, args):
    db = client.geostore
    geonames_collection = db.geonames

    with open('local/all-geonames-rdf.txt') as f:
        geonames = []

        for i, entry in enumerate(f):
            try:
                g = rdflib.Graph()
                g.parse(data=entry)

                for s in g.subjects(RDF.type, GN.Feature):
                    entry = {'_id': unicode(s)}
                    name = g.value(subject=s, predicate=GN.name)
                    if name:
                        entry['name'] = unicode(name)
                    for seeAlso in g.objects(s, RDFS.seeAlso):
                        seeAlso = unicode(seeAlso)
                        if 'dbpedia' in seeAlso:
                            entry['dbpedia'] = seeAlso
                    # parent features
                    parent = g.value(subject=s, predicate=GN.parentFeature)
                    if parent:
                        entry['parent'] = unicode(parent)
                    # parent country
                    country = g.value(subject=s, predicate=GN.parentCountry)
                    if country:
                        entry['country'] = unicode(country)
                    # postal code
                    postalCodes = [unicode(c) for c in g.objects(s, GN.postalCode)]
                    if postalCodes:
                        entry['postalCodes'] = unicode(postalCodes)

                    geonames.append(entry)

                if i % 10000 == 0:
                    logging.debug("parsed entries: " + str(i) + ", geonames: " + str(len(geonames)))

            except Exception as e:
                logging.debug('Exception for entry: ' + entry)
                logging.debug(e)

    for n in geonames:
        geonames_collection.insert(n)


def keyword_extraction(client, args):
    db = client.geostore
    keywords = db.keywords

    with open('local/all-geonames-rdf.txt') as f:
        names = defaultdict(set)
        for i, entry in enumerate(f):
            if i % 10000 == 0:
                logging.debug("parsed entries: " + str(i) + ", new keywords: " + str(len(names)))
            try:
                if entry.startswith('http://sws.geonames.org/'):
                    continue
                g = rdflib.Graph()
                g.parse(data=entry)

                for s in g.subjects(RDF.type, GN.Feature):
                    for label in g.objects(s, GN.name):
                        l = label.strip().lower()
                        n = unicode(l)
                        names[n].add(unicode(s))
                    for label in g.objects(s, GN.alternateName):
                        l = label.strip().lower()
                        n = unicode(l)
                        names[n].add(unicode(s))


            except Exception as e:
                logging.debug('Exception for entry: ' + entry)
                logging.debug(e)

            break
        print len(names)

        for n in names:
            keywords.insert_one({'_id': n, 'geonames': list(names[n])})


def dbpedia_links_to_mongo(client, args):
    db = client.geostore
    geonames = db.geonames

    g = rdflib.Graph()
    g.parse('local/geonames_links_en.ttl', format="nt")

    for dbp, geon in g.subject_objects(OWL.sameAs):
        entry = geonames.find_one({'_id': geon, 'dbpedia': {'$exists': False}})
        if entry:
            geonames.update_one({'_id': geon}, {'$set': {'dbpedia': dbp}})
        else:
            logging.debug('Geonames entry not in DB: ' + str(geon))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='geo-store')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=27017)
    filename = datetime.now().strftime('%Y-%m-%d') + '.log'
    parser.add_argument('--logfile', default=filename)
    parser.add_argument('--loglevel', type=str, default='debug')

    subparsers = parser.add_subparsers()

    # create the parser for the "geonames" command
    subparser = subparsers.add_parser('geonames')
    subparser.set_defaults(func=geonames_to_mongo)

    # create the parser for the "keywords" command
    subparser = subparsers.add_parser('keywords')
    subparser.set_defaults(func=keyword_extraction)

    # create the parser for the "postalcode" command
    subparser = subparsers.add_parser('postalcode')
    subparser.set_defaults(func=postalcode_csv_to_mongo)

    # create the parser for the "dbpedia" command
    subparser = subparsers.add_parser('dbpedia')
    subparser.set_defaults(func=dbpedia_links_to_mongo)

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
