import json

import yaml
import pymongo
from pymongo import MongoClient
import urllib
from elasticsearch import Elasticsearch


class LocationSearch:
    def __init__(self, host, port):
        client = MongoClient(host, port)
        db = client.geostore
        self.countries = db.countries
        self.postalcodes = db.postalcodes
        self.geonames = db.geonames
        self.nuts = db.nuts

    def get_postalcode_mappings_by_country(self, code, country):
        results = []
        if country:
            pc = self.postalcodes.find_one({'_id': code})
            if pc:
                for c in pc['countries']:
                    if c['country'] == country['_id'] and 'geonames' in c:
                        results += c['geonames']
                        break
        return results

    def get_by_substring(self, q, search_api, limit=10):
        results = []
        #for res in self.geonames.find({'name': {'$regex': q, '$options': 'i'}}).limit(limit):

        cursor = self.geonames.find({'$text': { '$search': q }}, {'score': {'$meta': "textScore" }})
        cursor.sort([('score', {'$meta': 'textScore'})])
        cursor.limit(limit)

        for res in cursor:
            tmp = {
                'title': res['name'],
                'url': search_api + '?' + urllib.urlencode({'q': res['name'].encode('utf-8'), 'l': res['_id']})
            }
            if 'country' in res:
                c = self.geonames.find_one({'_id': res['country']})
                if 'name' in c:
                    tmp['price'] = c['name']
            if 'parent' in res:
                c = self.geonames.find_one({'_id': res['parent']})
                if c and 'name' in c:
                    tmp['description'] = c['name']
            results.append(tmp)
        return results

    def get_postalcodes(self, q, search_api, limit=5):
        results = []
        for res in self.postalcodes.find({'_id': {'$regex': '^' + q}}).limit(limit):
            if 'countries' in res:
                for country in res['countries']:
                    tmp = {
                        'title': res['_id']
                    }
                    c = self.countries.find_one({'_id': country['country']})
                    if 'name' in c:
                        tmp['price'] = c['name']
                        tmp['url'] = search_api + '?' + urllib.urlencode(
                            {'q': res['_id'].encode('utf-8'), 'p': c['iso'] + '#' + res['_id']})
                    if 'region' in country:
                        region = self.geonames.find_one({'_id': country['region']})
                        if region and 'name' in region:
                            tmp['description'] = region['name']
                    results.append(tmp)
        return results

    def get_nuts(self, q, search_api, limit=5):
        results = []
        for res in self.nuts.find({'_id': {'$regex': '^' + q}}).limit(limit):
            resp = {'q': res['name'].encode('utf-8')}
            if 'geonames' in res:
                resp['l'] = res['geonames']
            elif 'dbpedia' in res:
                resp['l'] = res['dbpedia']
            else:
                resp['l'] = res['geovocab']

            tmp = {
                'title': res['_id'],
                'description': res['name'],
                'url': search_api + '?' + urllib.urlencode(resp)
            }
            country_code = res['_id'][:2]
            if country_code == 'UK':
                country_code = 'GB'
            c = self.countries.find_one({'iso': country_code})
            if c and 'name' in c:
                tmp['price'] = c['name']
            results.append(tmp)
        return results

    def get_country(self, iso):
        country = self.countries.find_one({'iso': iso})
        return country

    def get_parents(self, id, parents=None):
        current = self.geonames.find_one({"_id": id})
        if parents is None:
            parents = []
        else:
            if current and "name" in current:
                parents.insert(0, (current["name"], current['_id']))
        if current and "parent" in current and current["parent"] != id:
            self.get_parents(current["parent"], parents)
        return parents

    def get_external_links(self, id):
        current = self.geonames.find_one({"_id": id})
        external = []
        if 'dbpedia' in current:
            external.append({'name': 'DBpedia', 'link': current['dbpedia']})
        if 'wikidata' in current:
            external.append({'name': 'Wikidata', 'link': current['wikidata']})
        if 'geovocab' in current:
            external.append({'name': 'GeoVocab.org', 'link': current['geovocab']})
        return external


class ESClient(object):
    def __init__(self, indexName='autcsv', config=None):
        if config:
            with open(config) as f_conf:
                conf = yaml.load(f_conf)
                host = {'host': conf['es']['host'], 'port': conf['es']['port']}
                if 'url_prefix' in conf['es']:
                    host['url_prefix'] = conf['es']['url_prefix']

            self.es = Elasticsearch(hosts=[host])
            self.indexName = conf['es']['indexName']
        else:
            self.es = Elasticsearch()
            self.indexName = indexName

    def get(self, url, columns=True, rows=True):
        include = ['column.headers.exact', 'row.*', 'no_columns', 'no_rows', 'portal.*', 'url']
        exclude = []
        if columns:
            include.append("column.*")
        if not rows:
            exclude.append("row.*")
        res = self.es.get(index=self.indexName, doc_type='table', id=url, _source_exclude=exclude, _source_include=include)
        return res

    def searchEntity(self, entity, limit=10, offset=0):
        q = {
            "_source": ["url", "column.headers.exact", "portal.*"],
            "query": {
                "nested": {
                    "path": "row",
                    "query": {
                        "constant_score": {
                            "filter": {
                                "term": {
                                    "row.entities": entity
                                }
                            }
                        }
                    },
                    "inner_hits": {}
                }
            }
        }
        return self.es.search(index=self.indexName, doc_type='table', body=q, size=limit, from_=offset)

    def searchEntities(self, entities, limit=10, offset=0):
        q = {
            "_source": ["url", "column.headers.exact", "portal.*"],
            "query": {
                "nested": {
                    "path": "row",
                    "query": {
                        "constant_score": {
                            "filter": {
                                "terms": {
                                    "row.entities": entities
                                }
                            }
                        }
                    },
                    "inner_hits": {}
                }
            }
        }
        return self.es.search(index=self.indexName, doc_type='table', body=q, size=limit, from_=offset)

    def searchText(self, term, limit=10, offset=0):
        q = {
            "_source": ["url", "column.headers.exact", "portal.*"],
            "query": {
                "nested": {
                    "path": "row",
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "match": {
                                        "row.values.value": term
                                    }
                                }
                            ]
                        }
                    },
                    "inner_hits": {}
                }
            }
        }
        return self.es.search(index=self.indexName, doc_type='table', body=q, size=limit, from_=offset)


MAX_STRING_LENGTH = 20
def _get_doc_headers(doc):
    headers = []
    if 'column' in doc['_source']:
        for h in doc['_source']['column']:
            if 'headers' in h:
                v = h['headers'][0]['exact'][0]
                headers.append(v if len(v) < MAX_STRING_LENGTH else v[:MAX_STRING_LENGTH] + '...')
    return headers

def format_results(results):
    """Print results nicely:
    doc_id) content
    """
    data = []
    for doc in results['hits']['hits']:
        d = {"url": doc['_source']['url'], "portal": doc['_source']['portal']['uri']}
        d["headers"] = _get_doc_headers(doc)

        if 'row' in doc['inner_hits']:
            d['row'] = [c if len(c) < MAX_STRING_LENGTH else c[:MAX_STRING_LENGTH] + '...' for c in
                        doc['inner_hits']['row']['hits']['hits'][0]['_source']['values']['exact']]
            d['row_no'] = doc['inner_hits']['row']['hits']['hits'][0]['_source']['row_no']
            if 'entities' in doc['inner_hits']['row']['hits']['hits'][0]['_source']:
                d['entities'] = doc['inner_hits']['row']['hits']['hits'][0]['_source']['entities']
        data.append(d)
    return data


def format_table(doc, max_rows=500):
    d = {"url": doc['_source']['url'], "portal": doc['_source']['portal']['uri'], 'rows': []}
    d['headers'] = _get_doc_headers(doc)
    if 'row' in doc['_source']:
        rows = doc['_source']['row']
        for row_no, row in enumerate(rows):
            d_row = []
            for i, e in enumerate([c if len(c) < MAX_STRING_LENGTH else c[:MAX_STRING_LENGTH] + '...'
                              for c in row['values']['exact']]):
                entry = {'value': e}

                if 'entities' in row:
                    entry['entity'] = row['entities'][i]
                d_row.append(entry)
            d['rows'].append(d_row)
            if row_no > max_rows:
                break
    return d
