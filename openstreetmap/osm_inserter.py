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
from geonames_graph import get_all_parent_ids

DATA_WU_REFERRER = 'http://data.wu.ac.at'


def write_polygon_to_file(geojson, filename, name='polygon'):
    """
    create a boundary polygon file
    """
    coordinates = geojson['coordinates']
    type = geojson['type']
    if type == 'MultiPolygon':
        polygons = [polygon[0] for polygon in coordinates]
    else:
        polygons = coordinates

    fid = open(filename, 'wt')
    n = 1
    fid.write(name + '\n')
    for p in polygons:
        fid.write('%i\n' % (n))
        for node in p:
            fid.write('\t%s\t%s\n' % (node[0], node[1]))
        fid.write('END\n')
        n += 1
    fid.write('END\n')
    fid.close()


def get_geonames_url(id):
    url = "http://sws.geonames.org/" + id + "/"
    return url

def get_geonames_id(url):
    id = url.split('/')[-2]
    return id

def format_country_name(name):
    name = name.lower()
    name = name.replace(' and ', ' ')
    name = name.replace(' ', '-')
    return name

def process_eu_countries(client, args):
    db = client.geostore
    geonames = db.geonames
    countries = db.countries
    directory = args.directory

    for c in countries.find({'continent': 'EU', 'osm_data': {'$exists': False}}):
        name = format_country_name(c['name'])
        url = 'http://download.geofabrik.de/europe/{0}-latest.osm.pbf'.format(name)
        print url
        try:
            testfile = urllib.URLopener()
            testfile.retrieve(url, os.path.join(directory, '{0}-latest.osm.pbf'.format(name)))
            countries.update_one({'_id': c['_id']}, {'$set': {'osm_data': url}})
        except Exception as e:
            logging.debug('URL not found:' + url + ' - ' + str(e))



def export_polygons(client, args):
    db = client.geostore
    geonames = db.geonames
    countries = db.countries

    directory = args.directory
    admin_level = args.level
    all_c = []
    if args.country:
        all_c.append(countries.find_one({'_id': args.country}))

    else:
        for c in countries.find({'continent': 'EU'}):
            all_c.append(c)
    for country in all_c:
        c_name = format_country_name(country['name'])
        if not os.path.exists(directory):
            os.mkdir(directory)
        c_dir = os.path.join(directory, c_name)
        if not os.path.exists(c_dir):
            os.mkdir(c_dir)
        lvl_dir = os.path.join(c_dir, str(admin_level))
        if not os.path.exists(lvl_dir):
            os.mkdir(lvl_dir)

        for region in geonames.find({'admin_level': admin_level, 'country': country['_id']}):
            if 'geojson' in region:
                geojson = region['geojson']
                g_id = get_geonames_id(region['_id'])
                write_polygon_to_file(geojson=geojson, filename=os.path.join(lvl_dir, g_id), name=g_id)


def remove_stopwords(s):
    with open('openstreetmap/location_stopwords.txt') as f:
        for w in f:
            s = s.replace(w.strip(), '')
    return s.strip()


def get_osm_id(candidates, admin_level):
    for c in candidates:
        if 'osm_type' in c and c['osm_type'] == 'relation':
            osm_id = c['osm_id']
            c_url = 'http://www.openstreetmap.org/api/0.6/relation/' + osm_id

            # waiting time to reduce heavy use
            time.sleep(1)
            s = requests.Session()
            s.headers.update({'referrer': DATA_WU_REFERRER})
            req = requests.get(c_url)

            root = ET.fromstring(req.content)
            for node in root.iter('tag'):
                if node.attrib['k'] == "admin_level" and node.attrib['v'] == str(admin_level):
                    select_url = 'http://nominatim.openstreetmap.org/reverse?osm_id={0}&osm_type=R&polygon_geojson=1&format=json'.format(osm_id)

                    # waiting time to reduce heavy use
                    time.sleep(1)
                    s = requests.Session()
                    s.headers.update({'referrer': DATA_WU_REFERRER})
                    req = s.get(select_url)
                    select = req.json()
                    return (osm_id, select['geojson'])
    return None


def get_polygons_via_wiki(client, args):

    db = client.geostore
    countries = db.countries
    geonames = db.geonames

    all_c = []
    if args.country:
        all_c.append(countries.find_one({'_id': args.country}))
    else:
        for c in countries.find({'continent': 'EU'}):
            if c['iso'] not in args.skip:
                all_c.append(c)

    for c in all_c:
        country = c['_id']

        res1 = geonames.find({'country': country, 'geojson': {'$exists': False}, 'osm_relation': {'$exists': True}})
        res2 = geonames.find({'country': country, 'geojson.type': 'Point', 'osm_relation': {'$exists': True}})
        for region in itertools.chain(res1, res2):

            osm_id = region['osm_relation']

            select_url = 'http://nominatim.openstreetmap.org/reverse?osm_id={0}&osm_type=R&polygon_geojson=1&format=json'\
                .format(osm_id)

            # waiting time to reduce heavy use
            time.sleep(1)
            s = requests.Session()
            s.headers.update({'referrer': DATA_WU_REFERRER})
            req = s.get(select_url)
            if req.status_code == 200:
                select = req.json()

                if 'geojson' in select:
                    geojson = select['geojson']
                    geonames.update_one({'_id': region['_id']}, {'$set': {'osm_id': osm_id, 'geojson': geojson}})
            else:
                print 'error:', req.status_code
                print req.content


def get_polygons(client, args):
    db = client.geostore
    countries = db.countries
    geonames = db.geonames

    update = args.update
    admin_level = args.level
    skip = args.skip

    all_c = []
    if args.country:
        all_c.append(countries.find_one({'_id': args.country}))
    else:
        for c in countries.find({'continent': 'EU'}):
            if c['iso'] not in args.skip:
                all_c.append(c)

    for c in all_c:
        country = c['_id']
        c_iso = c['iso'].lower()

        if update:
            res = geonames.find({'admin_level': admin_level, 'country': country, 'geojson': {'$exists': False}})
        else:
            res = geonames.find({'admin_level': admin_level, 'country': country})

        for region in res:
            # waiting time to reduce heavy use
            time.sleep(1)

            r_name = region['name']
            r_name = remove_stopwords(r_name)
            r_url = u'http://nominatim.openstreetmap.org/search?q={0}&countrycodes={1}&format=json'.format(r_name, c_iso).encode('utf-8')

            s = requests.Session()
            s.headers.update({'referrer': DATA_WU_REFERRER})
            req = s.get(r_url)

            if req.status_code == 200:
                candidates = req.json()

                osm = get_osm_id(candidates, admin_level)
                if not osm:
                    osm = get_osm_id(candidates, admin_level+1)

                if osm:
                    geonames.update_one({'_id': region['_id']}, {'$set': {'osm_id': osm[0], 'geojson': osm[1]}})
                else:
                    print 'not found:', r_name


def read_osm_files(client, args):
    logging.info("OSM inserter started. Args: " + str(args))
    db = client.geostore
    countries = db.countries
    admin_level = args.level

    dir = args.directory

    all_c = []
    if args.country:
        all_c.append(countries.find_one({'_id': args.country}))
    else:
        for c in countries.find({'continent': 'EU'}):
            if c['iso'] not in args.skip:
                all_c.append(c)


    for country in all_c:
        c_name = format_country_name(country['name'])
        path = os.path.join(dir, c_name, str(admin_level))

        if os.path.isdir(path):
            for filename in os.listdir(path):
                if filename.endswith(".osm"):
                    f = os.path.join(path, filename)
                    geonames_id = filename[:-4]
                    read_osm_xml(client, f, geonames_id)


def geonamesId_to_url(geo_id):
    return 'http://sws.geonames.org/{0}/'.format(geo_id)

def in_geonames_parents(geo_id, ids):
    # check if geonames_id is more specific or general
    all_parents = []
    for x in ids:
        all_parents += get_all_parent_ids(client, geonamesId_to_url(x))
    return geonamesId_to_url(geo_id) in all_parents



def read_osm_xml(client, filename, geonames_id):
    logging.info("Inserting osm extract for geonames ID: " + geonames_id)
    db = client.geostore
    osm_names = db.osm_names
    osm = db.osm

    tree = ET.parse(filename)
    root = tree.getroot()

    for element in root:
        osm_type = element.tag
        osm_id = element.attrib.get('id')
        if not osm_id:
            continue

        tags = {
            'name': None,
            'highway': None,
            'type': None,
            'wikidata': None,
            'amenity': None
        }

        for t in element.findall('tag'):
            for tag in tags:
                if tag == t.attrib['k']:
                    tags[tag] = t.attrib['v']

        if tags['name']:
            l = tags['name'].strip().lower()
            n = unicode(l)

            # 1) check if there is already an entry c with this name
            added_sameAs_ref = False
            c = osm_names.find_one({'_id': n})
            if c and osm_id not in c['osm_id']:
                for osm_cand in [osm.find_one({'_id': c_id}) for c_id in c['osm_id']]:
                    # check if identical region and osm type
                    if in_geonames_parents(geonames_id, osm_cand['geonames_ids']) and osm_cand['osm_type'] == osm_type:
                        # add ref to same osm entry with same name
                        if osm_id not in osm_cand.get('osm_sameAs', []):
                            osm.update_one({'_id': osm_cand['_id']}, {'$push': {'osm_sameAs': osm_id}})
                        added_sameAs_ref = True
                if not added_sameAs_ref:
                    osm_names.update_one({'_id': c['_id']}, {'$push': {'osm_id': osm_id}})
            elif not c:
                osm_names.insert_one({'_id': n, 'osm_id': [osm_id]})

            # 2) check if osm id already exists
            osm_entry = osm.find_one({'_id': osm_id})
            if not osm_entry and not added_sameAs_ref:
                osm_entry = {t: tags[t] for t in tags if tags[t]}
                osm_entry['geonames_ids'] = [geonames_id]
                osm_entry['osm_type'] = osm_type
                osm_entry['_id'] = osm_id

                lat = element.attrib.get('lat')
                lon = element.attrib.get('lon')
                if lat and lon:
                    osm_entry['geojson'] = {
                        "type": "Point",
                        "coordinates": [lon, lat]
                    }
                osm.insert_one(osm_entry)
            elif osm_entry and not in_geonames_parents(geonames_id, osm_entry['geonames_ids']):
                osm.update_one({'_id': osm_entry['_id']}, {'$push': {'geonames_ids': geonames_id}})



if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='geo-store')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=27017)
    #filename = datetime.now().strftime('%Y-%m-%d') + '.log'
    parser.add_argument('--logfile')
    parser.add_argument('--loglevel', type=str, default='debug')

    subparsers = parser.add_subparsers()

    subparser = subparsers.add_parser('all-eu-countries')
    subparser.set_defaults(func=process_eu_countries)
    subparser.add_argument('--directory', default='poly-exports/osm-export/')

    subparser = subparsers.add_parser('poly-export')
    subparser.set_defaults(func=export_polygons)
    subparser.add_argument('--directory', default='poly-exports')
    subparser.add_argument('--country')
    subparser.add_argument('--level', type=int, default=8)

    subparser = subparsers.add_parser('osm-polygons')
    subparser.set_defaults(func=get_polygons)
    subparser.add_argument('--country')
    subparser.add_argument('--level', type=int, default=6)
    subparser.add_argument('--update', action='store_true')
    subparser.add_argument('--skip', action='append', help='ISO2 codes of countries', default=[])

    subparser = subparsers.add_parser('osm-polygons-2')
    subparser.set_defaults(func=get_polygons_via_wiki)
    subparser.add_argument('--country')
    subparser.add_argument('--skip', action='append', help='ISO2 codes of countries', default=[])


    subparser = subparsers.add_parser('insert-osm')
    subparser.set_defaults(func=read_osm_files)
    subparser.add_argument('--country')
    subparser.add_argument('--level', type=int, default=8)
    subparser.add_argument('--directory', default='poly-exports/osm-export/')
    subparser.add_argument('--skip', action='append', help='ISO2 codes of countries', default=[])

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
