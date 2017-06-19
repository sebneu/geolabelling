import csv
from collections import defaultdict
import rdflib
from rdflib.namespace import RDF, OWL
from rdflib import Namespace

RAMON = Namespace("http://rdfdata.eionet.europa.eu/ramon/ontology/")
NUTS = Namespace("http://nuts.geovocab.org/id/")
SPATIAL = Namespace("http://geovocab.org/spatial#")

from pymongo import MongoClient


def at_postalcode_to_nuts3(host, port):
    client = MongoClient(host, port)
    db = client.geostore
    postalcodes = db.postalcodes

    with open('local/nuts/pc2016_at_NUTS-2010_v2.3.csv') as f:
        codes = defaultdict(set)
        for i, row in enumerate(csv.reader(f, delimiter=';')):
            if i == 0:
                # header
                continue
            code = row[0]
            nuts3 = row[1]
            codes[code].add(nuts3)

    for c in codes:
        postalcodes.update_one({'_id': c}, {'$set': {'nuts3': list(codes[c])} }, upsert=True)


def add_lau_to_keywords(host, port):
    client = MongoClient(host, port)
    db = client.geostore
    keywords = db.keywords
    lau_index = 2
    name_index = 4
    with open('local/nuts/at_lau.csv') as f:
        for i, row in enumerate(csv.reader(f, delimiter=',')):
            if i == 0:
                # header
                header = row
                name_index = header.index('NAME_1')
                if 'LAU2_NAT_CODE_NEW' in header:
                    lau_index = header.index('LAU2_NAT_CODE_NEW')
                else:
                    lau_index = header.index('LAU2_NAT_CODE')
                continue
            code = row[lau_index]
            name = row[name_index].strip().lower()

            keywords.update_one({'_id': name}, {'$set': {'lau_code': code}}, upsert=True)



def rdfnuts_to_geonames(host, port):
    client = MongoClient(host, port)
    db = client.geostore
    nuts = db.nuts

    g = rdflib.Graph()
    g.parse("/home/neumaier/Repos/odgraph/local/nuts/nuts-rdf-0.91.ttl", format='ttl')

    for region in g.subjects(predicate=RDF.type, object=RAMON.NUTSRegion):
        data = {}
        code = g.value(subject=region, predicate=RAMON.code)
        if not code:
            continue
        data['_id'] = code
        data['geovocab'] = region

        name = g.value(subject=region, predicate=RAMON.name)
        if name:
            data['name'] = name

        level = g.value(subject=region, predicate=RAMON.level)
        if level != None:
            data['level'] = int(level)

        parent = g.value(subject=region, predicate=SPATIAL.PP)
        if parent:
            p = str(parent)
            prefix = 'http://nuts.geovocab.org/id/'
            p_id = p
            if p.startswith(prefix):
                p_id = p[len(prefix):]
            data['parent'] = p_id

        dbpedia = None
        geonames = None
        for sameAs in g.objects(subject=region, predicate=OWL.sameAs):
            if 'dbpedia.org' in sameAs:
                dbpedia = sameAs
            if 'geonames.org' in sameAs:
                geonames = sameAs
        if dbpedia:
            data['dbpedia'] = dbpedia
        if geonames:
            data['geonames'] = geonames

        nuts.insert(data)


if __name__ == '__main__':
    #import sys
    #sys.setrecursionlimit(5000)
    #rdfnuts_to_geonames('localhost', 27017)
    #at_postalcode_to_nuts3('localhost', 27017)

    add_lau_to_keywords('localhost', 27017)