import os
import urlparse
import argparse
import datetime

import structlog
from elasticsearch import Elasticsearch
from pymongo import MongoClient

import profiler

import rdflib
from rdflib import URIRef, BNode, Literal
from rdflib.namespace import Namespace, RDF, RDFS, OWL, XSD

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

CSVWX = Namespace("http://data.wu.ac.at/ns/csvwx#")
TIMEX = Namespace("http://data.wu.ac.at/ns/timex#")
OSMX = Namespace("http://data.wu.ac.at/ns/osmx#")


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


def getTables(es, indexName, size, scroll='5m', source_exclude=None):
    res = es.search(index=indexName, doc_type='table', size=size, scroll=scroll, _source_exclude=source_exclude)
    scroll_id = res['_scroll_id']

    while len(res['hits']['hits']) > 0:
        for t in res['hits']['hits']:
            yield t
        res = es.scroll(scroll_id=scroll_id, scroll=scroll, _source_exclude=source_exclude)
        scroll_id = res['_scroll_id']


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

    # add metadata entities
    for m in obj.get('metadata_entities', []):
        entity = location_search.format_entities(m)
        graph.add((dataset_ref, CSVWX.refersToEntity, URIRef(entity)))

    # add metadata entities
    for m in obj.get('data_entities', []):
        entity = location_search.format_entities(m)
        graph.add((ref, CSVWX.refersToEntity, URIRef(entity)))

    # add temporal info
    if 'metadata_temp_start' in obj:
        graph.add((dist, TIMEX.hasStartTime, Literal(obj['metadata_temp_start'], datatype=XSD.date)))
    if 'metadata_temp_end' in obj:
        graph.add((dist, TIMEX.hasEndTime, Literal(obj['metadata_temp_end'], datatype=XSD.date)))

    if 'data_temp_start' in obj:
        graph.add((ref, TIMEX.hasStartTime, Literal(obj['data_temp_start'], datatype=XSD.date)))
    if 'data_temp_end' in obj:
        graph.add((ref, TIMEX.hasEndTime, Literal(obj['data_temp_end'], datatype=XSD.date)))

    if 'transaction_time' in obj:
        graph.add((ref, TIMEX.transactionTime, Literal(obj['transaction_time'], datatype=XSD.date)))

    if 'data_temp_pattern' in obj and obj['data_temp_pattern'] != 'varying':
        graph.add((ref, TIMEX.hasTemporalPattern, Literal(obj['data_temp_pattern'])))

    # dialect
    # BNode: url + snapshot + CSVW.dialect
    if 'dialect' in obj and isinstance(obj['dialect'], dict):
        d = obj['dialect']
        bnode_hash = hashlib.sha1(url + str(snapshot) + CSVW.dialect.n3())
        dialect = BNode(bnode_hash.hexdigest())
        graph.add((resource, CSVW.dialect, dialect))
        if d.get('encoding'):
            graph.add((dialect, CSVW.encoding, Literal(d['encoding'])))
        if d.get('delimiter'):
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
                h = re.sub(r"[\n\t\s]*", "", h)
            else:
                h = 'column' + str(i)
            # BNode: url + snapshot + CSVW.column + col_i
            #bnode_hash = hashlib.sha1(url + str(snapshot) + CSVW.column.n3() + str(i))
            #column = BNode(bnode_hash.hexdigest())
            if is_valid_uri(url):
                column = rdflib.URIRef(url + '#' + h)
            else:
                # BNode: url + snapshot + CSVW.column + col_i
                bnode_hash = hashlib.sha1(url + str(snapshot) + CSVW.column.n3() + str(i))
                column = BNode(bnode_hash.hexdigest())

            graph.add((tableschema, CSVW.column, column))
            graph.add((column, CSVW.name, Literal(h)))
            graph.add((column, CSVW.datatype, col_types[i]))

            if 'entities' in c or 'dates' in c:
                for row_i, v in enumerate(c['values']['exact']):
                    # BNode: url + snapshot + CSVW.column + col_i + value + row_i
                    bnode_hash = hashlib.sha1(
                        url + str(snapshot) + CSVW.column.n3() + 'col' + str(i) + 'row' + str(row_i))
                    cell = BNode(bnode_hash.hexdigest())
                    if is_valid_uri(url):
                        row_url = URIRef(url + '#row=' + str(row_i))
                    else:
                        # BNode: url + snapshot + col_i + CSVWX.row + row_i
                        bnode_hash = hashlib.sha1(url + str(snapshot) + str(i) + CSVWX.row.n3() + str(row_i))
                        row_url = BNode(bnode_hash.hexdigest())

                    graph.add((cell, RDF.type, CSVWX.Cell))
                    graph.add((cell, CSVWX.rowURL, row_url))
                    graph.add((cell, CSVWX.columnURL, column))
                    graph.add((cell, RDF.value, Literal(v)))

                    if 'entities' in c and c['entities'][row_i]:
                        e = c['entities'][row_i]

                        entity = location_search.format_entities(e)
                        graph.add((cell, CSVWX.refersToEntity, URIRef(entity)))

                        # alternative representation
                        graph.add((rdflib.URIRef(entity), column, rdflib.Literal(v)))

                    if 'dates' in c and c['dates'][row_i]:
                        graph.add((cell, CSVWX.hasTime, Literal(c['dates'][row_i], datatype=XSD.date)))





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
                g.add((URIRef(c_url), RDFS.label, Literal(osm['name'])))

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
            g.add((n, RDFS.label, Literal(nuts['name'])))

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
            g.add((URIRef(geo_id['_id']), RDFS.label, Literal(geo_id['name'])))
        if 'parent' in geo_id:
            g.add((URIRef(geo_id['_id']), GN.parentFeature, URIRef(geo_id['parent'])))
        if 'country' in geo_id:
            g.add((URIRef(geo_id['_id']), GN.parentCountry, URIRef(geo_id['country'])))

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



def exportCountries(client, args):
    db = client.geostore

    g = rdflib.Graph()
    print 'Countries'
    for i, geo_entity in enumerate(db.countries.find()):
        country = URIRef(geo_entity['_id'])
        if 'iso' in geo_entity:
            g.add((country, GN.countryCode, Literal(geo_entity['iso'])))
            g.add((country, WDT.P297, Literal(geo_entity['iso'])))
        if 'iso3' in geo_entity:
            g.add((country, WDT.P298, Literal(geo_entity['iso3'])))
        if 'osm_data' in geo_entity:
            g.add((country, OSMX.downloadExtract, URIRef(geo_entity['osm_data'])))

    g.serialize(destination='countries.nt', format='nt')


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

    subparser = subparsers.add_parser('countries')
    subparser.set_defaults(func=exportCountries)


    args = parser.parse_args()
    client = MongoClient(args.host, args.port)
    args.func(client, args)
