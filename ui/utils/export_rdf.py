import os
import urlparse

import datetime

import structlog
import profiler

import rdflib
from rdflib import URIRef, BNode, Literal
from rdflib.namespace import Namespace, RDF, RDFS, DCTERMS, XSD

import re
from rfc3987 import get_compiled_pattern
import hashlib

PW_AGENT = URIRef("http://data.wu.ac.at/portalwatch")

DCAT = Namespace("http://www.w3.org/ns/dcat#")
CSVW = Namespace("http://www.w3.org/ns/csvw#")
PROV = Namespace("http://www.w3.org/ns/prov#")

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


def storeGraph(graph, portalid, directory):
    destination=os.path.join(directory, portalid + '.n3')
    graph.serialize(destination=destination, format='n3')
    log.info("CSVW Metadata graph stored", portal=portalid, destination=destination)

