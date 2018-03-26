import os
import urlparse
import argparse
import datetime

import structlog
from pymongo import MongoClient

import profiler

import rdflib
from rdflib import URIRef, BNode, Literal
from rdflib.namespace import Namespace, RDF, RDFS, DCTERMS, XSD, OWL

import re
from rfc3987 import get_compiled_pattern
import hashlib

from openstreetmap.osm_inserter import get_geonames_url

PW_AGENT = URIRef("http://data.wu.ac.at/portalwatch")

DCAT = Namespace("http://www.w3.org/ns/dcat#")
CSVW = Namespace("http://www.w3.org/ns/csvw#")
PROV = Namespace("http://www.w3.org/ns/prov#")
GN = Namespace("http://www.geonames.org/ontology#")
WDT = Namespace("http://www.wikidata.org/prop/direct/")

CSVWX = Namespace("http://data.wu.ac.at/csvwx#")


log = structlog.get_logger()

# TODO disallows whitespaces
VALID_URL = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

URI = get_compiled_pattern('^%(URI)s$')


def is_valid_url(references):
    try:
        res = urlparse.urlparse(references)
        return bool(res.scheme and res.netloc)
    except Exception as e:
        return False


def is_valid_uri(references):
    return bool(URI.match(references))



def addMetadata( obj, graph, location_search):
    url = obj['url']
    snapshot = obj.get('transaction_time', str(datetime.date.today()))
    cols = obj.get('column', [])

    dataset_ref = URIRef(obj['dataset']['dataset_link'])
    graph.add((dataset_ref, RDF.type, DCAT.Dataset))
    portal_uri = URIRef(obj['portal']['uri'])
    graph.add((portal_uri, DCAT.dataset, dataset_ref))

    bnode_hash = hashlib.sha1(url + str(snapshot) + DCAT.distribution)
    dist = BNode(bnode_hash.hexdigest())

    graph.add((dist, RDF.type, DCAT.Distribution))
    graph.add((dataset_ref, DCAT.distribution, dist))

    bnode_hash = hashlib.sha1(url + str(snapshot))
    resource = BNode(bnode_hash.hexdigest())

    if is_valid_uri(url):
        ref = URIRef(url)
    else:
        ref = Literal(url)
    # add url to graph
    graph.add((resource, CSVW.url, ref))
    graph.add((dist, DCAT.accessURL, ref))

    # dialect
    # BNode: url + snapshot + CSVW.dialect
    if 'dialect' in obj and isinstance(obj['dialect'], dict):
        d = obj['dialect']
        bnode_hash = hashlib.sha1(url + str(snapshot) + CSVW.dialect.n3())
        dialect = BNode(bnode_hash.hexdigest())
        graph.add((resource, CSVW.dialect, dialect))
        if d.get('encoding'):
            graph.add((dialect, CSVW.encoding, Literal(d['encoding'])))
        if d.get('encoding'):
            graph.add((dialect, CSVW.delimiter, Literal(d['delimiter'])))
        if d.get('skipinitialspace') and d['skipinitialspace'] > 0:
            graph.add((dialect, CSVW.skipBlankRows, Literal(d['skipinitialspace'])))

        if len(cols) > 0 and 'header' in cols[0]:
            h_count = len(cols[0]['header'][0]['exact'])
            graph.add((dialect, CSVW.header, Literal(True)))
            graph.add((dialect, CSVW.headerRowCount, Literal(h_count)))
        else:
            graph.add((dialect, CSVW.header, Literal(False)))
            graph.add((dialect, CSVW.headerRowCount, Literal(0)))

        # columns
        # BNode: url + snapshot + CSVW.tableSchema
        bnode_hash = hashlib.sha1(url + str(snapshot) + CSVW.tableSchema.n3())
        tableschema = BNode(bnode_hash.hexdigest())
        graph.add((resource, CSVW.tableSchema, tableschema))
        col_types = profiler.profile(cols)

        for i, c in enumerate(cols):
            if 'header' in c:
                h = c['header'][0]['exact'][0].replace(' ', '_')
            else:
                h = 'column' + str(i)
            # BNode: url + snapshot + CSVW.column + col_i
            #bnode_hash = hashlib.sha1(url + str(snapshot) + CSVW.column.n3() + str(i))
            #column = BNode(bnode_hash.hexdigest())
            column = rdflib.URIRef(url + '#' + h)

            graph.add((tableschema, CSVW.column, column))
            graph.add((column, CSVW.name, Literal(h)))
            graph.add((column, CSVW.datatype, col_types[i]))

            if 'entities' in c:
                row_i = 0
                for e, v in zip(c['entities'], c['values']['exact']):
                    if e:
                        # representation 1
                        entity = location_search.format_entities(e)
                        graph.add((rdflib.URIRef(entity), column, rdflib.Literal(v)))

                        # representation 2
                        # BNode: url + snapshot + CSVW.column + col_i + value + row_i
                        bnode_hash = hashlib.sha1(url + str(snapshot) + CSVW.column.n3() + 'col' + str(i) + 'row' + str(row_i))
                        cell = BNode(bnode_hash.hexdigest())
                        row_url = URIRef(url + '#row=' + str(row_i))
                        graph.add((cell, RDF.type, CSVWX.Cell))
                        graph.add((cell, CSVWX.rowURL, row_url))
                        graph.add((cell, CSVWX.columnURL, column))
                        graph.add((cell, RDF.value, Literal(v)))

                        graph.add((cell, CSVWX.refersToEntity, URIRef(entity)))
                    row_i += 1




def exportOSM(client, args):
    db = client.geostore
    osm_url = 'http://www.openstreetmap.org/'

    g = rdflib.Graph()

    print 'OSM data'
    for i, osm in enumerate(db.osm.find()):
        if 'osm_type' in osm:
            osm_id = osm['_id']
            c_url = osm_url + osm['osm_type'] + '/' + osm_id
            if 'name' in osm:
                g.add((URIRef(c_url), GN.name, Literal(osm['name'])))

            if 'geonames_ids' in osm:
                for geo_id in osm['geonames_ids']:
                    g_url = get_geonames_url(geo_id)
                    g.add((URIRef(c_url), GN.parentFeature, URIRef(g_url)))

        if i % 100000 == 0:
            print 'processed: ', str(i)

    g.serialize(destination='osm.nt', format='nt')
    print 'stored nt file'


def exportNUTS(client, args):
    db = client.geostore

    g = rdflib.Graph()

    print 'NUTS data'
    for nuts in db.nuts.find():
        # use blank nodes for nuts
        n = URIRef('http://dd.eionet.europa.eu/vocabularyconcept/common/nuts/' + nuts['_id'])
        g.add((n, WDT.P605, Literal(nuts['_id'])))
        if 'name' in nuts:
            g.add((n, GN.name, Literal(nuts['name'])))

        if 'geonames' in nuts:
            g.add((n, OWL.sameAs, URIRef(nuts['geonames'])))
        if 'wikidata' in nuts:
            g.add((n, OWL.sameAs, URIRef(nuts['wikidata'])))
        if 'dbpedia' in nuts:
            g.add((n, OWL.sameAs, URIRef(nuts['dbpedia'])))
        if 'geovocab' in nuts:
            g.add((n, OWL.sameAs, URIRef(nuts['geovocab'])))

        if 'parent' in nuts:
            p = URIRef('http://dd.eionet.europa.eu/vocabularyconcept/common/nuts/' + nuts['parent'])
            g.add((n, GN.parentFeature, p))

    g.serialize(destination='nuts.nt', format='nt')
    print 'stored nt file'


def exportGeoNames(client, args):
    db = client.geostore
    osm_url = 'http://www.openstreetmap.org/'

    graph_count = 1
    g = rdflib.Graph()
    print 'GeoNames data'
    for i, geo_id in enumerate(db.geonames.find()):
        if 'name' in geo_id:
            g.add((URIRef(geo_id['_id']), GN.name, Literal(geo_id['name'])))
        if 'parent' in geo_id:
            g.add((URIRef(geo_id['_id']), GN.parentFeature, Literal(geo_id['parent'])))
        if 'country' in geo_id:
            g.add((URIRef(geo_id['_id']), GN.parentCountry, Literal(geo_id['country'])))

        if 'wikidata' in geo_id:
            g.add((URIRef(geo_id['_id']), OWL.sameAs, URIRef(geo_id['wikidata'])))
        if 'dbpedia' in geo_id:
            g.add((URIRef(geo_id['_id']), OWL.sameAs, URIRef(geo_id['dbpedia'])))

        if 'osm_id' in geo_id:
            osm_id = geo_id['osm_id']
            c_url = osm_url + 'relation/' + osm_id
            g.add((URIRef(geo_id['_id']), OWL.sameAs, URIRef(c_url)))

        if 'postalcode' in geo_id:
                g.add((URIRef(geo_id['_id']), GN.postalCode, Literal(geo_id['postalcode'])))

        if (i+1) % 3000000 == 0:
            print 'processed: ', str(i)

            g.serialize(destination='geonames' + str(graph_count) + '.nt', format='nt')
            print 'stored nt file', graph_count
            graph_count += 1
            g = rdflib.Graph()

    g.serialize(destination='geonames' + str(graph_count) + '.nt', format='nt')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='rdf-export')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=27018)

    subparsers = parser.add_subparsers()

    subparser = subparsers.add_parser('geonames')
    subparser.set_defaults(func=exportGeoNames)

    subparser = subparsers.add_parser('osm')
    subparser.set_defaults(func=exportOSM)

    subparser = subparsers.add_parser('nuts')
    subparser.set_defaults(func=exportNUTS)

    args = parser.parse_args()
    client = MongoClient(args.host, args.port)
    args.func(client, args)
