# get coords from dbpedia, wikidata, usw.. save also links, ..
# use geonames reverse lookup
# http://api.geonames.org/findNearbyPlaceNameJSON?lat=52.5117&lng=13.3831&username=demo
from pymongo import MongoClient

GEONAMES_USER = 'seb.neumaier'

'''
SELECT DISTINCT ?poi ?coor ?range
WHERE
{
  ?poi wdt:P31/wdt:P279* wd:Q570116 ;
  ?range wd:Q40;
  wdt:P625 ?coor.
}
'''
import requests


def get_dbpedia_coords():
    instances = '<http://dbpedia.org/ontology/ArchitecturalStructure>'
    country = 'dbr:Austria'
    url = 'http://dbpedia.org/sparql?default-graph-uri=http%3A%2F%2Fdbpedia.org&query=select+%3Fs+%3Flat+%3Flong%0D%0Awhere+%7B%0D%0A+++%3Fs+a+{0}+.%0D%0A+++%3Fs+%3Fl+{1}+.%0D%0A+++%3Fs+geo%3Alat+%3Flat+.%0D%0A+++%3Fs+geo%3Along+%3Flong+.%0D%0A%0D%0A%7D&format=application%2Fsparql-results%2Bjson&CXML_redir_for_subjs=121&CXML_redir_for_hrefs=&timeout=30000&debug=on'.format(instances, country)
    resp = requests.get(url)
    if resp.status_code == 200:
        data = resp.json()
        for r in data['results']['bindings']:
            entity = r['s']['value'].encode('utf-8')
            lat = r['lat']['value']
            long = r['long']['value']

            yield entity, lat, long


def get_wikidata_coords():
    # tourist_attractions = 'wd:Q570116'
    # social spaces
    instances = 'wd:Q7551384'
    country = 'wd:Q40'
    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql?query=SELECT%20DISTINCT%20%3Fpoi%20%3Fcoor%20%3Frange%0AWHERE%0A%7B%0A%20%20%3Fpoi%20wdt%3AP31%2Fwdt%3AP279*%20{0}%20%3B%0A%20%20%3Frange%20{1}%3B%0A%20%20wdt%3AP625%20%3Fcoor.%0A%7D&format=json'.format(instances, country)
    resp = requests.get(url)
    if resp.status_code == 200:
        data = resp.json()
        for r in data['results']['bindings']:
            entity = r['poi']['value']
            coords = r['coor']['value']
            latlong = coords[6:-1].split(' ')
            lat = latlong[1]
            long = latlong[0]

            yield entity, lat, long


def get_all_dbpedia_labels(entity):
    url = 'http://dbpedia.org/sparql?default-graph-uri=http%3A%2F%2Fdbpedia.org&query=select+%3Flabel%0D%0Awhere+%7B%0D%0A{0}+rdfs%3Alabel+%3Flabel.%0D%0A%7D&format=application%2Fsparql-results%2Bjson&CXML_redir_for_subjs=121&CXML_redir_for_hrefs=&timeout=30000&debug=on'.format('<'+entity+'>')
    return get_all_labels(url)

def get_all_wikidata_labels(entity):
    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql?query=SELECT%20DISTINCT%20%3Flabel%0AWHERE%0A%7B%0A%20%20{0}%20rdfs%3Alabel%20%3Flabel%20.%0A%7D&format=json'.format('<'+entity+'>')
    return get_all_labels(url)


def get_all_labels(url):
    resp = requests.get(url)
    labels = set()
    if resp.status_code == 200:
        data = resp.json()
        for l in data['results']['bindings']:
            labels.add(l['label']['value'])
    return list(labels)


def geonames_lookup(lat, long, username):
    url = 'http://api.geonames.org/findNearbyPlaceNameJSON?lat={0}&lng={1}&username={2}'.format(lat, long, username)
    resp = requests.get(url)

    if resp.status_code == 200:
        data = resp.json()
        if 'geonames' in data:
            for g in data['geonames']:
                return g['geonameId']


def entity_mapping_to_mongo(label, geonameId, entity, client, entity_type='wikidata'):
    db = client.geostore
    keywords = db.keywords
    geonames = db.geonames
    geo_url = 'http://sws.geonames.org/' + str(geonameId) + '/'
    tmp = geonames.find_one({'_id': geo_url})
    #geo_par_url = tmp['parent']
    if tmp:
        l = label.lower()
        e = keywords.find_one({'_id': l})
        if e:
            print 'exists:', l
    #        if geo_url not in e['geonames']:
    #            found_in_parent = False
    #            for g in e['geonames']:
    #                g_e = geonames.find_one({'_id': g})
    #                if g_e['parent'] == geo_url or g_e['parent'] == geo_par_url:
    #                    found_in_parent = True
    #                    break
    #            if not found_in_parent:
    #                print 'update:', geo_url
    #                e['geonames'].append(geo_url)
    #                keywords.update_one({'_id': l}, e)
        else:
            print 'insert:', l
            keywords.insert_one({'_id': l, 'geonames': [geo_url], entity_type: entity})
    else:
        print 'no geonames entry', geo_url

if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    coords = get_dbpedia_coords()

    for dbpedia_entity, lat, long in coords:
        print dbpedia_entity
        all_labels = get_all_dbpedia_labels(dbpedia_entity)
        geo_entity = geonames_lookup(lat, long, GEONAMES_USER)
        if geo_entity:
            for l in all_labels:
                entity_mapping_to_mongo(l.encode('utf-8'), geo_entity, dbpedia_entity, client, 'dbpedia')
        print
