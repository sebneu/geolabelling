import json

import yaml
import pymongo
from pymongo import MongoClient
import urllib
from elasticsearch import Elasticsearch
from ui.utils import mongo_collections_utils


class LocationSearch:
    def __init__(self, host, port):
        client = MongoClient(host, port)
        db = client.geostore
        self.countries = db.countries
        self.postalcodes = db.postalcodes
        self.geonames = db.geonames
        self.keywords = db.keywords
        self.nuts = db.nuts

    def get(self, id):
        return self.geonames.find_one({'_id': id})

    def get_geonames(self, term=None, link=None):
        t = term.strip().lower()
        return self.keywords.find_one({'_id': t})

    def get_nuts_by_geovocab(self, l):
        return self.nuts.find_one({'geovocab': l})

    def get_nuts_by_id(self, id):
        return self.nuts.find_one({'_id': id})

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
                'url': search_api + '?' + urllib.urlencode({'l': res['_id']})
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
                            {'p': c['iso'] + '#' + res['_id']})
                    if 'region' in country:
                        region = self.geonames.find_one({'_id': country['region']})
                        if region and 'name' in region:
                            tmp['description'] = region['name']
                    results.append(tmp)
        return results

    def get_nuts(self, q, search_api, limit=5):
        results = []
        for res in self.nuts.find({'_id': {'$regex': '^' + q}}).limit(limit):
            resp = {'l': mongo_collections_utils.get_nuts_link(res)}
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
    def __init__(self, indexName='autcsv', conf=None):
        if conf:
            host = {'host': conf['es']['host'], 'port': conf['es']['port']}
            if 'url_prefix' in conf['es']:
                host['url_prefix'] = conf['es']['url_prefix']

            self.es = Elasticsearch(hosts=[host])
            self.indexName = conf['es']['indexName']
        else:
            self.es = Elasticsearch()
            self.indexName = indexName

    def get(self, url, columns=True, rows=True):
        include = ['column.header.value', 'row.*', 'no_columns', 'no_rows', 'portal.*', 'url']
        exclude = []
        if columns:
            include.append("column.*")
        if not rows:
            exclude.append("row.*")
        res = self.es.get(index=self.indexName, doc_type='table', id=url, _source_exclude=exclude, _source_include=include)
        return res

    def exists(self, url):
        res = self.es.exists(index=self.indexName, doc_type='table', id=url)
        return res

    def searchEntity(self, entity, limit=10, offset=0):
        q = {
            "_source": ["url", "column.header.value", "portal.*", "dataset.*"],
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


    def searchEntityAndText(self, entity, term, limit=10, offset=0, intersect=False):
        tmp = 'should'
        if intersect:
            tmp = 'must'
        q = {
            "_source": ["url", "column.header.value", "portal.*", "dataset.*"],
            "query": {
                "bool": {
                    tmp: [
                        {
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
                                "inner_hits": {},
                                "boost": 2
                            }
                        },
                        {
                            "nested": {
                                "path": "dataset",
                                "query": {
                                    "multi_match": {
                                        "query": term,
                                        "fields": ["dataset.name", "dataset.dataset_name",
                                                   "dataset.dataset_description", "dataset.publisher"]
                                    }
                                },
                                "inner_hits": {},
                                "boost": 0.1
                            }
                        }
                    ]
                }
            }
        }
        return self.es.search(index=self.indexName, doc_type='table', body=q, size=limit, from_=offset)


    def searchEntities(self, entities, limit=10, offset=0):
        q = {
            "_source": ["url", "column.header.value", "portal.*", "dataset.*"],
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
            "_source": ["url", "column.header.value", "portal.*", "dataset.*"],
            "query": {
                "bool": {
                    "should": [
                        {
                            "match": {
                                "column.header.value": term
                            }
                        },
                        {
                            "nested": {
                                "path": "column",
                                "query": {
                                    "match": {
                                        "column.header.value": term
                                    }
                                },
                                "inner_hits": {}
                            }
                        },
                        {
                            "nested": {
                                "path": "dataset",
                                "query": {
                                    "multi_match": {
                                        "query": term,
                                        "fields": ["dataset.name", "dataset.dataset_name",
                                                   "dataset.dataset_description", "dataset.publisher"]
                                    }
                                },
                                "inner_hits": {}
                            }
                        },
                        {
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
                    ]
                }
            }
        }

        return self.es.search(index=self.indexName, doc_type='table', body=q, size=limit, from_=offset)

    def update(self, id, fields):
        q = {
            "doc": fields
        }
        return self.es.update(index=self.indexName, doc_type='table', id=id, body=q)


MAX_STRING_LENGTH = 20
def _get_doc_headers(doc, row_cutoff):
    headers = []
    if 'column' in doc['_source']:
        for h in doc['_source']['column']:
            if 'header' in h:
                v = h['header'][0]['value'][0]
                headers.append(v[:MAX_STRING_LENGTH] + '...' if len(v) > MAX_STRING_LENGTH and row_cutoff else v)
    return headers


def format_results(results, row_cutoff):
    """Print results nicely:
    doc_id) content
    """
    data = []
    for doc in results['hits']['hits']:
        d = {"url": doc['_source']['url'], "portal": doc['_source']['portal']['uri']}
        d["headers"] = _get_doc_headers(doc, row_cutoff)

        if 'dataset' in doc['_source']:
            d['dataset'] = doc['_source']['dataset']

        if 'row' in doc['inner_hits'] and doc['inner_hits']['row']['hits']['total'] > 0:
            d['row'] = [c[:MAX_STRING_LENGTH] + '...' if len(c) > MAX_STRING_LENGTH and row_cutoff else c for c in
                        doc['inner_hits']['row']['hits']['hits'][0]['_source']['values']['exact']]
            d['row_no'] = doc['inner_hits']['row']['hits']['hits'][0]['_source']['row_no']
            if 'entities' in doc['inner_hits']['row']['hits']['hits'][0]['_source']:
                d['entities'] = doc['inner_hits']['row']['hits']['hits'][0]['_source']['entities']
        data.append(d)
    return data


def format_table(doc, max_rows=500):
    d = {"url": doc['_source']['url'], "portal": doc['_source']['portal']['uri'], 'rows': []}
    d['headers'] = _get_doc_headers(doc, row_cutoff=False)
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
