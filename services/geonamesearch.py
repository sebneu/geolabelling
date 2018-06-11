from elasticsearch import Elasticsearch

from geonames_graph import get_all_parent_ids
from indexing.mapping import mappings
from openstreetmap.osm_inserter import get_geonames_id
from ui.utils import utils
from ui.utils.error_handler import ErrorHandler
import pymongo
import logging


def generate_id(db, gn_id):
    all_ids = get_all_parent_ids(db, gn_id)
    geoids = [get_geonames_id(url) for url in reversed(all_ids)]
    return '.'.join(geoids)


def index_all_geonames(es, es_names, indexName, db):
    for gn in db.geonames.find(no_cursor_timeout=True):
        gn_id = gn['_id']
        name = gn.get('name')
        parent_id = gn.get('parent')
        parent = None
        if parent_id:
            parent = db.geonames.find_one({'_id': parent_id})['name']
        country_id = gn.get('country')
        country = None
        if country_id:
            country = db.geonames.find_one({'_id': country_id})['name']

        count = es.searchEntities([gn_id], count=True)['count']
        if count > 0:
            geo_id = generate_id(db, gn_id)
            alt_names = [alt_name['_id'] for alt_name in db.keywords.find({'geonames': gn_id})]

            body = {'url': gn_id, 'name': name, 'alternateName': alt_names}
            if country_id:
                body['country'] = country_id
            if country:
                body['countryName'] = country
            if parent_id:
                body['parentFeature'] = parent_id
            if parent:
                body['parentFeatureName'] = parent
            res = es_names.index(index=indexName, doc_type='geonames', body=body, id=geo_id)


def setup(es_names, indexName, mappingName, delete=True):
    if delete:
        logging.info("ESClient, delete index")
        es_names.indices.delete(index=indexName, ignore=[400, 404])
    mappingConfig = mappings[mappingName]
    res = es_names.indices.create(index=indexName, body={'mappings': mappingConfig['mapping']})

    logging.info("ESClient, created index " + str(res))


def help():
    return "Indexing of all names for geo-entities"


def name():
    return 'NamesIndex'


def setupCLI(pa):
    pa.add_argument('-i','--init', help="Delete and re-initialize ElasticSearch index", action='store_true')


def cli(args, es):
    try:
        config = utils.load_config(args.config)
    except Exception as e:
        ErrorHandler.DEBUG=True
        logging.exception("Exception during config initialisation: " + str(e))
        return

    mongodb_host = config['mongodb']['host']
    mongodb_port = config['mongodb']['port']
    client = pymongo.MongoClient(host=mongodb_host, port=mongodb_port)
    db = client.geostore

    indexName = 'geonames'
    es_host = config['es']['host']
    es_port = config['es']['port']
    es_names = Elasticsearch(hosts=[{'host': es_host, 'port': es_port}])

    if args.init:
        setup(es_names, indexName=indexName, mappingName='geonames')

    index_all_geonames(es, es_names, indexName, db)









