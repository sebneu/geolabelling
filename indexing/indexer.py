import datetime
import logging
import sys
import traceback
import urllib2
from collections import defaultdict

import pycountry
import requests
import yaml

from indexing import language_mapping
from services import time_tagger


def getURLperPortal(odpwAPI,portal, snapshot, format):
    return odpwAPI+"portal/{}/{}/resources?format={}".format(portal, snapshot, format)


class FileTooBigException(Exception):
    def __init__(self, m):
        super(FileTooBigException, self).__init__(m)


def getFile(url, datamonitor, max_file_size):
    if datamonitor:
        d_url = datamonitor + 'data/data/' + url
        response = urllib2.urlopen(d_url)
        info = response.info()
        t = info.type
        if t == 'application/json':
            response = urllib2.urlopen(url)
    else:
        response = urllib2.urlopen(url)
    content = response.read()

    # convert to MB:
    if 0 < max_file_size < sys.getsizeof(content) / 1048576.:
        raise FileTooBigException("File exceeds max file size of " + str(max_file_size) + " MB.\nURL: " + url)
    return content

def get_dataset_info(dataset, portal, format, heideltime_path, language):
    res = {}
    dataset_name = dataset.get('name', '')
    dataset_link = dataset.get('@id', '')
    dataset_description = dataset.get('description', '')
    keywords = dataset.get('keywords', '')
    publisher = dataset.get('publisher', {}).get('name', '')
    publisher_link = dataset.get('publisher', {}).get('@id', '')
    publisher_email = dataset.get('publisher', {}).get('email', '')
    for dist in dataset.get('distribution', []):

        if 'contentUrl' in dist:
            url = dist['contentUrl']
            dist_format = dist.get('encodingFormat', '')

            if url.endswith('.' + format) or format in dist_format:
                if url in urlPortalID and portal not in urlPortalID[url]:
                    urlPortalID[url].append(portal)
                else:
                    urlPortalID[url] = [portal]

                dsfields = {}
                fields = {'dataset': dsfields}
                if time_tagger:
                    start, end, published, modified = time_tagger.get_temporal_information(dist, dataset, heideltime_path=heideltime_path, language=language)
                    if start and end:
                        fields['metadata_temp_start'] = start
                        fields['metadata_temp_end'] = end
                    if published:
                        fields['published'] = published
                    if modified:
                        fields['modified'] = modified

                name = dist.get('name', '')
                if name:
                    dsfields['name'] = name
                if dataset_name:
                    dsfields['dataset_name'] = dataset_name
                if dataset_link:
                    dsfields['dataset_link'] = dataset_link
                if dataset_description:
                    dsfields['dataset_description'] = dataset_description
                if keywords and isinstance(keywords, list):
                    dsfields['keywords'] = keywords
                if publisher:
                    dsfields['publisher'] = publisher
                if publisher_link:
                    dsfields['publisher_link'] = publisher_link
                if publisher_email:
                    dsfields['publisher_email'] = publisher_email
                res[url] = fields
    return res


def getURLandDatasetInfoPerPortal(odpwAPI, portal, snapshot, format, heideltime_path, language, max_file_size):
    api_url = odpwAPI + 'portal/' + portal + '/' + str(snapshot) + '/datasets'
    resp = requests.get(api_url)
    if resp.status_code == 200:
        datasets = resp.json()
        for d in datasets:
            d_id = d['id']
            try:
                d_url = odpwAPI + 'portal/' + portal + '/' + str(snapshot) + '/dataset/' + d_id + '/schemadotorg'
                resp = requests.get(d_url)

                if resp.status_code == 200:
                    dataset = resp.json()
                    datasetinfo = get_dataset_info(dataset, portal, format, heideltime_path, language)
                    for url in datasetinfo:
                        try:
                            content = getFile(url, datamonitor=False, max_file_size=max_file_size)
                            yield url, content, datasetinfo[url]
                        except Exception as e:
                            logging.error('Error while downloading dataset: ' + url)
                            logging.error(str(e))

            except Exception as e:
                logging.error('Error while retrieving all datasets: ' + d_id)
                logging.error(e)


def getURLInfo(datamonitorAPI,url):
    return datamonitorAPI+"info/{}".format(url)

urlPortalID={}

def index(es, portalInfo, snapshot, format, odpwAPI, heideltime_path, language, geotagging=False, repair=False, max_file_size=-1):
    status=defaultdict(int)
    portal=portalInfo['id']

    for url, content, dsfields in getURLandDatasetInfoPerPortal(odpwAPI, portal, snapshot, format, heideltime_path, language, max_file_size):
        logging.info("Dataset from ODPW: " + portal + ", snapshot: " + str(snapshot) + ", URL: " + url)

        try:
            if not repair or (repair and not es.exists(url)):
                resp = es.indexTable(url=url, content=content, portalInfo=portalInfo, datasetInfo=dsfields, geotagging=geotagging)
                logging.info("ES INDEXED, URL: " + url + ", status: " + resp['result'])
                status['ok']+=1
        except Exception as e:
            traceback.print_stack()
            logging.error("ES INDEX " + portal + ", URL: " + url + ", Error:" + str(e))
            status['error'] += 1
            status[str(e.__class__)] += 1
    return status



def bulkIndex(es, portalInfo, snapshot, format, odpwAPI, heideltime_path, language, geotagging=False, max_file_size=-1, bulk=10):
    portal=portalInfo['id']
    tables_bulk = []

    for i, (url, content, dsfields) in enumerate(getURLandDatasetInfoPerPortal(odpwAPI, portal, snapshot, format, heideltime_path, language, max_file_size)):
        if (i+1) % bulk == 0:
            res = es.bulkIndexTables(tables_bulk, portalInfo=portalInfo, geotagging=geotagging)
            errors = ''
            if res and 'errors' in res:
                errors = res['errors']
                logging.info("Bulk-index: " + portal + ", snapshot: " + str(snapshot) + ", tables: " + str(len(tables_bulk)) + ", es_errors: " + str(errors))
            tables_bulk = []

            logging.info("Dataset from ODPW: " + portal + ", snapshot: " + str(snapshot) + ", URL: " + url)
        tables_bulk.append((url, content, dsfields))


def getCurrentSnapshot():
    now = datetime.datetime.now()
    y=now.isocalendar()[0]
    w=now.isocalendar()[1]
    sn=str(y)[2:]+'{:02}'.format(w)

    return sn

def help():
    return "Index urls from portal"
def name():
    return 'Indexer'

def setupCLI(pa):
    pa.add_argument("-f", "--format", help='filter by file format', dest='format', default="csv")
    pa.add_argument("--setup")
    pa.add_argument("-s", "--snapshot", help='snapshot', dest='snapshot', default=None)
    pa.add_argument("-p", "--portal", help='filter by portalid ( sperated by whitespace)', dest='portal', nargs='+')
    pa.add_argument("--url", help='index single URLs (sperated by whitespace)', dest='url', nargs='+')
    pa.add_argument("-r", "--repair", help='add only CSVs not yet in the index', action='store_true')
    pa.add_argument("--geotagging", help='add geo information to index', action='store_true')
    pa.add_argument("--max-size", help='set maximum file size', type=float)
    pa.add_argument("--bulk", help='bulk insert to elasticsearch', action='store_true')
    pa.add_argument("--disable-heideltime", help='disable Heideltime tagging to increase indexing speed', action='store_true')

def cli(args, es):
    heideltime_path = None
    if args.config:
        with open(args.config) as f_conf:
            config = yaml.load(f_conf)
            if 'odpw' in config:
                odpwAPI=config['odpw']['apiurl']
            else:
                logging.info("ODPW ERROR: Please specify the (ADEQUATe) Portal Watch API in the config file")
                return -1
            heideltime_path = config.get('heideltime')

    if args.setup:
        es.setup(language=args.setup)
        exit()

    logging.info("Getting portal Info")
    res = requests.get(odpwAPI+"portals/list")
    if res.status_code!=200:
        logging.info("ODPW ERROR: Could not get portal list")
    else:
        portalInfo = { p['id']:p for p in res.json()}

    geotagging = False
    if args.geotagging:
        from services import geo_tagger
        mongodb_host = config['db']['host']
        mongodb_port = config['db']['port']
        geotagging = geo_tagger.GeoTagger(host=mongodb_host, port=mongodb_port)

    if args.disable_heideltime:
        heideltime_path = None

    portalIDs = args.portal if args.portal else portalInfo.keys()
    snapshot = int(args.snapshot) if args.snapshot else getCurrentSnapshot()

    if args.url:
        for url in args.url:
            # just index single URLs
            resp= es.indexTable(url=url, portalInfo=portalInfo[args.portal[0]], geotagging=geotagging)
            logging.info("ES INDEXED: " + url + ", STATUS: " + resp['result'])
    else:
        for portal in portalIDs:
            status = {}
            if portal not in portalInfo:
                logging.error("Portal not known: " + portal)
                exit()

            p = portalInfo[portal]
            iso_code = p['iso'].lower()
            lang = language_mapping.iso_to_language(iso_code)
            if not lang:
                try:
                    lang = pycountry.languages.get(alpha_2=iso_code)
                    lang = lang.name
                except:
                    lang = 'english'
            lang = lang.upper()
            logging.info("Starting indexing: " + portal + ", snapshot: " + str(snapshot) + ", format: " +args.format)
            if args.bulk:
                bulkIndex(es, p, snapshot, args.format, odpwAPI, heideltime_path, lang,
                               geotagging=geotagging, max_file_size=args.max_size)

            else:
                status=index(es, p, snapshot, args.format, odpwAPI, heideltime_path, lang, geotagging=geotagging, repair=args.repair, max_file_size=args.max_size)
