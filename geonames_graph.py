import sys
import argparse
import logging
from collections import defaultdict
from datetime import datetime
import time

import pymongo
from pymongo import MongoClient

import csv
import rdflib
from rdflib.namespace import RDF, RDFS, OWL
from rdflib import Namespace
import sparql

GN = Namespace('http://www.geonames.org/ontology#')

WIKIDATA_ENDPOINT = 'https://query.wikidata.org/sparql'


def countries_to_mongo(client, args):
    db = client.geostore
    countries = db.countries

    with open("local/countryInfo.csv") as f:
        reader = csv.reader(f, delimiter='\t')
        reader.next()
        for i, row in enumerate(reader):
            iso2, iso3, country, capital, continent, postalcode_regex, geoname_id = row[0].decode('utf-8'), row[1].decode('utf-8'), \
                                                                  row[4].decode('utf-8'), row[5].decode('utf-8'), row[8].decode('utf-8'), \
                                                                  row[14].decode('utf-8'), row[16].decode('utf-8')

            geoname_url = 'http://sws.geonames.org/' + geoname_id + '/'
            entry = {
                '_id': geoname_url,
                'iso': iso2,
                'iso3': iso3,
                'name': country,
                'capital': capital,
                'continent': continent,
                'postalcode-regex': postalcode_regex
            }
            countries.insert(entry)


def get_subregions(region_id, region_name, waiting_time=1):
    sub_g = rdflib.Graph()
    # waiting time to reduce heavy use
    time.sleep(waiting_time)
    sub_g.parse(region_id + "contains.rdf")

    all_childs = list(sub_g.subjects(GN.parentFeature, rdflib.URIRef(region_id)))
    if len(all_childs) == 0:
        return []
    elif len(all_childs) == 1:
        c_name = sub_g.value(subject=all_childs[0], predicate=GN.name)
        subr = get_subregions(all_childs[0], c_name, waiting_time=waiting_time)
        if subr:
            return subr
        else:
            return [all_childs[0]]
    else:
        res = []
        for c in all_childs:
            c_name = sub_g.value(subject=c, predicate=GN.name)
            if c_name == region_name:
                subr = get_subregions(c, c_name, waiting_time=waiting_time)
                if subr:
                    res += subr
                else:
                    res.append(c)
            else:
                res.append(c)
        return res


def add_city_level_divisions(client, args):
    country = args.country

    db = client.geostore
    countries = db.countries
    geonames = db.geonames

    if country:
        countries_iter = countries.find({'_id': country})
    else:
        countries_iter = countries.find()

    for c in countries_iter:
        if args.update:
            if geonames.find_one({'country': c['_id'], 'admin_level': 8}):
                print 'found admin level 8: ', c['name']
                continue
        for region in geonames.find({'country': c['_id'], 'admin_level': 6}):
            try:
                sub_regions = get_subregions(region['_id'], region['name'])

                for sub_r in sub_regions:
                    geonames.update_one({'_id': sub_r}, {'$set': {'admin_level': 8}})
            except Exception as e:
                msg = "Could not get region: " + region['name'] + ", " + c['name']
                print msg
                print str(e)
                logging.debug(msg)
                logging.error(e)


def add_divisions_to_geonames(client, args):
    country = args.country
    db = client.geostore
    countries = db.countries
    geonames = db.geonames

    if country:
        countries_iter = countries.find({'_id': country})
    else:
        countries_iter = countries.find()

    for c in countries_iter:
        g = rdflib.Graph()
        geonames.update_one({'_id': c['_id']}, {'$set': {'admin_level': 2}})

        g.parse(c['_id'] + "contains.rdf")
        for s in g.subjects(GN.parentFeature, rdflib.URIRef(c['_id'])):
            geonames.update_one({'_id': s}, {'$set': {'admin_level': 4}})

            sub_g = rdflib.Graph()
            sub_g.parse(s + "contains.rdf")
            for sub_s in sub_g.subjects(GN.parentFeature, rdflib.URIRef(s)):
                geonames.update_one({'_id': sub_s}, {'$set': {'admin_level': 6}})


def postalcode_csv_to_mongo(client, args):
    db = client.geostore
    keywords = db.keywords
    countries = db.countries
    postalcodes_collection = db.postalcodes
    postalcodes = defaultdict(dict)

    # function/heuristic for retrieving all geonames ids in given country
    def add_geo_ids(localname, country_id, result):
        keywordentry = keywords.find_one({'_id': localname})
        if keywordentry and 'geonames' in keywordentry:
            for geo_id in keywordentry['geonames']:
                parent_ids = get_all_parent_ids(client, geo_id)
                if country_id in parent_ids:
                    parent_names = get_all_parents(client, geo_id)
                    if town in parent_names and (district in parent_names or state in parent_names):
                        result.add(geo_id)


    with open("local/allCountries.txt") as f:
        reader = csv.reader(f, delimiter='\t')
        for i, row in enumerate(reader):
            if i % 1000 == 0:
                logging.debug("parsed entries: " + str(i) + ", postal codes: " + str(len(postalcodes)))

            country, postalcode, localname, state, district, town = row[0].decode('utf-8'), row[1].decode('utf-8'), row[
                2].decode('utf-8'), row[3].decode('utf-8'), row[5].decode('utf-8'), row[7].decode('utf-8')
            # store under new country id
            c_e = countries.find_one({'iso': country})
            c_id = c_e['_id']

            l = localname.strip().lower()
            geo_ids = set()
            add_geo_ids(l, c_id, geo_ids)
            if not geo_ids:
                logging.debug("Try to find parts of local name in keywords: " + l)
                multiple_names = localname.split(',')
                if len(multiple_names) <= 1:
                    multiple_names = localname.split('-')
                if len(multiple_names) <= 1:
                    multiple_names = localname.split('/')
                if len(multiple_names) <= 1:
                    multiple_names = localname.split(' ')

                if len(multiple_names) > 1:
                    for n in multiple_names:
                        n = n.strip().lower()
                        # try again to find an entry
                        add_geo_ids(n, c_id, geo_ids)

                if not geo_ids:
                    logging.debug("Local name not in keywords: " + l)
                    # TODO go to wikidata and look for entity of this postalcode
            # add geonames ids for current country and postalcode
            if geo_ids:
                postalcodes[postalcode][c_id] = geo_ids


        for c in postalcodes:
            entry = postalcodes.find_one({'_id': c})
            if not entry:
                entry = {'_id': c, 'countries': []}
            for country_id in postalcodes[c]:
                geonames_ids = list(postalcodes[c][country_id])
                tmp = {'country': country_id, 'geonames': geonames_ids}
                region = get_lowest_common_ancestor(client, geonames_ids)
                if region:
                    tmp['region'] = region
                entry['countries'].append(tmp)
            postalcodes_collection.insert(entry)


def wikidata_osmrelations_to_geonames(client, args):
    db = client.geostore

    s = sparql.Service(WIKIDATA_ENDPOINT, "utf-8", "GET")
    statement = '''
    SELECT ?s ?o ?g WHERE {
          ?s wdt:P402 ?o.
          ?s wdt:P1566 ?g.
        }
    '''
    result = s.query(statement)
    for row in result.fetchone():
        wikidata_id = row[0].value.strip()
        osm_relation = row[1].value.strip()
        geon = 'http://sws.geonames.org/' + row[2].value + '/'

        geon_entry = db.geonames.find_one({'_id': geon})
        if geon_entry:
            db.geonames.update_one({'_id': geon}, {'$set': {'osm_relation': osm_relation, 'wikidata': wikidata_id}})



def wikidata_geojson_geonames(client, args):
    db = client.geostore

    if args.country:
        countries_iter = db.countries.find({'_id': args.country})
    else:
        countries_iter = db.countries.find({'continent': 'EU'})

    for c in countries_iter:
        x = db.geonames.find_one({'_id': c['_id']})
        country = x['wikidata']

        s = sparql.Service(WIKIDATA_ENDPOINT, "utf-8", "GET")
        statement = '''
            SELECT ?s ?g ?lon ?lat WHERE {{
              ?s wdt:P1566 ?g.
              ?s wdt:P17 <{0}>;
                 p:P625 [
                   ps:P625 ?coord;
                   psv:P625 [
                     wikibase:geoLongitude ?lon;
                     wikibase:geoLatitude ?lat;
                     wikibase:geoGlobe ?globe;
                   ] ;
                 ]
            }}
        '''.format(country)
        result = s.query(statement)
        for row in result.fetchone():
            wikidata_id = row[0].value.strip()
            geon = 'http://sws.geonames.org/' + row[1].value + '/'
            lon = row[2].value
            lat = row[3].value

            geojson = {
                "type": "Point",
                "coordinates": [lon, lat]
            }
            geon_entry = db.geonames.find_one({'_id': geon, 'geojson': {'$exists': False}})
            if geon_entry:
                db.geonames.update_one({'_id': geon}, {'$set': {'geojson': geojson, 'wikidata': wikidata_id}})




def wikidata_postalcodes_to_geonames(client, args):
    db = client.geostore
    postalcodes = db.postalcodes

    s = sparql.Service(WIKIDATA_ENDPOINT, "utf-8", "GET")
    statement = '''
    SELECT ?s ?o ?g WHERE {
      ?s wdt:P281 ?o.
      ?s wdt:P1566 ?g.
    }
    '''
    result = s.query(statement)
    for row in result.fetchone():
        wikidata_id = row[0].value.strip()
        postalcode = row[1].value.strip()
        geon = 'http://sws.geonames.org/' + row[2].value + '/'

        geon_entry = db.geonames.find_one({'_id': geon})
        if geon_entry and 'country' in geon_entry:
            db.geonames.update_one({'_id': geon}, {'$set': {'postalcode': postalcode}})

            geon_country = geon_entry['country']
            entry = postalcodes.find_one({'_id': postalcode})
            if not entry:
                entry = {'_id': postalcode, 'countries': []}

            country_found = None
            for c in entry['countries']:
                if c['country'] == geon_country:
                    country_found = c
                    break

            if not country_found:
                country_found = {'country': geon_country, 'region': geon}
                entry['countries'].append(country_found)

            if not 'geonames' in country_found:
                country_found['geonames'] = []
            if geon not in country_found['geonames']:
                country_found['geonames'].append(geon)
            postalcodes.update_one({'_id': entry['_id']}, {'$set': {'countries': entry['countries']}}, upsert=True)


def get_lowest_common_ancestor(client, geonames_ids):
    parents = {}

    for geo_id in geonames_ids:
        parents[geo_id] = get_all_parent_ids(client, geo_id)
    if len(geonames_ids) > 0:
        for p in parents[geonames_ids[0]]:
            common_anc = True
            for geo_id in parents:
                if p not in parents[geo_id]:
                    common_anc = False
                    break
            if common_anc:
                return p
    return None


def _get_all_parents(geo_id, geonames_collection, all_names):
    current = geonames_collection.find_one({"_id": geo_id})
    if current:
        if "name" in current:
            all_names.append(current["name"])
        if "parent" in current and current["parent"] != geo_id:
            _get_all_parents(current["parent"], geonames_collection, all_names)


def get_all_parents(client, geo_id):
    db = client.geostore
    geonames_collection = db.geonames
    names = []
    _get_all_parents(geo_id, geonames_collection, names)
    return names


def _get_all_parent_ids(geo_id, geonames_collection, all_ids):
    current = geonames_collection.find_one({"_id": geo_id})
    if current and current["_id"] not in all_ids:
        all_ids.append(current["_id"])
        if "parent" in current and current["parent"] != geo_id:
            _get_all_parent_ids(current["parent"], geonames_collection, all_ids)


def get_all_parent_ids(client, geo_id):
    db = client.geostore
    geonames_collection = db.geonames
    all_ids = []
    _get_all_parent_ids(geo_id, geonames_collection, all_ids)
    return all_ids


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

        print len(names)

        for n in names:
            keywords.insert_one({'_id': n, 'geonames': list(names[n])})


def dbpedia_links_to_mongo(client, args):
    db = client.geostore
    geonames = db.geonames

    g = rdflib.Graph()
    g.parse('local/dbpedia/geonames_links_en.ttl', format="nt")

    for dbp, geon in g.subject_objects(OWL.sameAs):
        entry = geonames.find_one({'_id': geon, 'dbpedia': {'$exists': False}})
        if entry:
            geonames.update_one({'_id': geon}, {'$set': {'dbpedia': dbp}})
        else:
            logging.debug('Geonames entry not in DB: ' + str(geon))


def wikidata_nuts_to_geonames(client, args):
    db = client.geostore
    nuts = db.nuts

    s = sparql.Service(WIKIDATA_ENDPOINT, "utf-8", "GET")
    statement = '''
    SELECT ?s ?o ?g WHERE {
      ?s wdt:P605 ?o.
      ?s wdt:P1566 ?g
    }
    '''
    result = s.query(statement)
    for row in result.fetchone():
        wikidata_id = row[0].value.strip()
        nuts_code = row[1].value.strip()
        geon = 'http://sws.geonames.org/' + row[2].value + '/'

        entry = nuts.find_one({'_id': nuts_code})
        if entry:
            nuts.update_one({'_id': nuts_code}, {'$set': {'geonames': geon, 'wikidata': wikidata_id}})
        else:
            logging.debug('NUTS entry not in DB: ' + str(nuts_code))
            geon_entry = db.geonames.find_one({'_id': geon})
            if geon_entry:
                if 'country' in geon_entry:
                    nuts.insert_one({'_id': nuts_code, 'geonames': geon, 'wikidata': wikidata_id, 'name': geon_entry['name'], 'country': geon_entry['country']})
                else:
                    nuts.insert_one({'_id': nuts_code, 'geonames': geon, 'wikidata': wikidata_id, 'name': geon_entry['name']})


def wikidata_links_to_mongo(client, args):
    db = client.geostore
    geonames = db.geonames

    s = sparql.Service(WIKIDATA_ENDPOINT, "utf-8", "GET")
    statement = '''
    SELECT ?s ?o WHERE {
      ?s wdt:P1566 ?o
    }
    '''
    result = s.query(statement)
    for row in result.fetchone():
        wikid = row[0].value
        geon = 'http://sws.geonames.org/' + row[1].value + '/'

        entry = geonames.find_one({'_id': geon, 'wikidata': {'$exists': False}})
        if entry:
            geonames.update_one({'_id': geon}, {'$set': {'wikidata': wikid}})
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

    subparser = subparsers.add_parser('geonames')
    subparser.set_defaults(func=geonames_to_mongo)

    subparser = subparsers.add_parser('keywords')
    subparser.set_defaults(func=keyword_extraction)

    subparser = subparsers.add_parser('postalcode')
    subparser.set_defaults(func=postalcode_csv_to_mongo)

    subparser = subparsers.add_parser('dbpedia')
    subparser.set_defaults(func=dbpedia_links_to_mongo)

    subparser = subparsers.add_parser('wikidata')
    subparser.set_defaults(func=wikidata_links_to_mongo)

    subparser = subparsers.add_parser('wikidata-nuts')
    subparser.set_defaults(func=wikidata_nuts_to_geonames)

    subparser = subparsers.add_parser('wikidata-postalcodes')
    subparser.set_defaults(func=wikidata_postalcodes_to_geonames)

    subparser = subparsers.add_parser('wikidata-osm')
    subparser.set_defaults(func=wikidata_osmrelations_to_geonames)

    subparser = subparsers.add_parser('wikidata-coordinates')
    subparser.add_argument('--country')
    subparser.set_defaults(func=wikidata_geojson_geonames)

    subparser = subparsers.add_parser('countries')
    subparser.set_defaults(func=countries_to_mongo)

    subparser = subparsers.add_parser('divisions')
    subparser.add_argument('--country')
    subparser.set_defaults(func=add_divisions_to_geonames)

    subparser = subparsers.add_parser('city-divisions')
    subparser.add_argument('--country')
    subparser.add_argument('--update', action='store_true')
    subparser.set_defaults(func=add_city_level_divisions)

    args = parser.parse_args()

    if args.loglevel == 'info':
        lvl = logging.INFO
    elif args.loglevel == 'debug':
        lvl = logging.DEBUG
    else:
        lvl = logging.DEBUG

    logging.basicConfig(level=lvl)
    rootLogger = logging.getLogger()
    logFormatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fileHandler = logging.FileHandler(args.logfile)
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)


    client = MongoClient(args.host, args.port)
    args.func(client, args)
