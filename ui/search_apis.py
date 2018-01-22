import json
from collections import defaultdict

import yaml
import pymongo
from pymongo import MongoClient
import urllib
from elasticsearch import Elasticsearch

from openstreetmap.osm_inserter import get_geonames_url
from ui.utils import mongo_collections_utils

import rdflib


class LocationSearch:
    def __init__(self, host, port):
        client = MongoClient(host, port)
        db = client.geostore
        self.countries = db.countries
        self.postalcodes = db.postalcodes
        self.geonames = db.geonames
        self.osm = db.osm
        self.keywords = db.keywords
        self.nuts = db.nuts

    def format_entities(self, e):
        if e == '' or not e:
            return ''
        elif e.startswith('http://sws.geonames.org/'):
            return e
        else:
            osm_entity = self.osm.find_one({'_id': e})
            return 'http://www.openstreetmap.org/' + osm_entity['osm_type'] + '/' + e

    def get(self, id):
        return self.geonames.find_one({'_id': id})

    # def getRandomGeoNames(self, count=10):
    #    return self.geonames.aggregate([{'$sample': {'size': count}}])

    def get_osm(self, id):
        return self.osm.find_one({'_id': id})

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
        # for res in self.geonames.find({'name': {'$regex': q, '$options': 'i'}}).limit(limit):

        cursor = self.geonames.find({'$text': {'$search': q}, 'datasets': True}, {'score': {'$meta': "textScore"}})
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

    def get_osm_names_by_substring(self, q, search_api, limit=10):
        results = []

        cursor = self.osm.find({'$text': {'$search': q}, 'datasets': True}, {'score': {'$meta': "textScore"}})
        cursor.sort([('score', {'$meta': 'textScore'})])
        cursor.limit(limit)

        for res in cursor:
            tmp = {
                'title': res['name'],
                'url': search_api + '?' + urllib.urlencode({'l': 'osm:' + res['_id']})
            }
            if 'geonames_ids' in res:
                regions = []
                for id in res['geonames_ids']:
                    c = self.geonames.find_one({'_id': get_geonames_url(id)})
                    regions.append(c['name'])
                tmp['description'] = ', '.join(regions)
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
        include = ['column.header.value', 'row.*', 'no_columns', 'no_rows', 'portal.*', 'url', 'locations', 'dataset.*']
        exclude = []
        if columns:
            include.append("column.*")
        if not rows:
            exclude.append("row.*")
        res = self.es.get(index=self.indexName, doc_type='table', id=url, _source_exclude=exclude,
                          _source_include=include)
        return res

    def get_triples(self, url, location_search):
        include = ['column.header.value', 'column.*', 'no_columns', 'no_rows', 'portal.*', 'url', 'locations',
                   'dataset.*']
        exclude = ['row.*']
        res = self.es.get(index=self.indexName, doc_type='table', id=url, _source_exclude=exclude,
                          _source_include=include)

        # convert column to RDF
        g = rdflib.Graph()
        if 'column' in res['_source']:
            for i, c in enumerate(res['_source']['column']):
                if 'entities' in c:
                    if 'header' in c and c['header'][0]['exact'][0]:
                        h = c['header'][0]['exact'][0].replace(' ', '_')
                    else:
                        h = 'col' + str(i)
                    h_prop = rdflib.URIRef(url + '#' + h)
                    for e, v in zip(c['entities'], c['values']['exact']):
                        if e:
                            entity = location_search.format_entities(e)
                            g.add((rdflib.URIRef(entity), h_prop, rdflib.Literal(v)))
        return g.serialize(format='nt')

    def get_urls(self, portal=None, columnlabels='none'):
        if portal:
            p = {"nested": {"path": "portal", "query": {"term": {"portal.id": portal}}}}
        else:
            p = {"match_all": {}}

        if columnlabels == 'all':
            q = {
                "_source": "_id",
                "query": {
                    "bool": {
                        "must": [
                            p, {
                                "nested": {
                                    "path": "column",
                                    "query": {
                                        "exists": {
                                            "field": "column.entities"
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        elif columnlabels == 'geonames':
            q = {
                "_source": "_id",
                "query": {
                    "bool": {
                        "must": [
                            p, {
                                "nested": {
                                    "path": "column",
                                    "query": {
                                        "prefix": {
                                            "column.entities": "http://sws.geonames.org/"
                                        }

                                    }
                                }
                            }
                        ]
                    }
                }
            }

        else:
            q = {
                "_source": "_id",
                "query": p
            }

        limit = 100
        scroll = "5m"
        res = self.es.search(index=self.indexName, doc_type='table', body=q, size=limit, scroll=scroll, timeout='30s')

        while True:
            if '_scroll_id' in res:
                scroll_id = res['_scroll_id']
            else:
                break
            res = self.es.scroll(scroll_id=scroll_id, scroll=scroll)
            urls = res['hits']['hits']

            if len(urls) == 0:
                break

            for l in urls:
                yield l['_id']

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

    def searchEntitiesAndText(self, entities, term, locations=None, limit=10, offset=0, intersect=False,
                              temporal_constraints=None):
        tmp = 'should'
        if intersect:
            tmp = 'must'
        q = {
            "_source": ["url", "column.header.value", "portal.*", "dataset.*", "locations", "temporal_start",
                        "temporal_end"],
            "query": {
                "bool": {
                    tmp: [
                        {
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
        if temporal_constraints:
            if tmp == 'must':
                q['query']['bool']['must'] += temporal_constraints
            else:
                q['query']['bool']['must'] = temporal_constraints
        if locations:
            q['query']['bool'][tmp].append({
                "constant_score": {
                    "filter": {
                        "terms": {
                            "locations": entities
                        }
                    }
                }
            })
        return self.es.search(index=self.indexName, doc_type='table', body=q, size=limit, from_=offset, timeout='30s')

    def searchEntities(self, entities, locations=None, limit=10, offset=0, intersect=False, temporal_constraints=None):
        entities = [e[4:] if e.startswith('osm:') else e for e in entities]
        tmp = 'should'
        if intersect:
            tmp = 'must'
        q = {
            "_source": ["url", "column.header.value", "portal.*", "dataset.*", "locations", "temporal_start",
                        "temporal_end"],
            "query": {
                "bool": {
                    tmp: [
                        {
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
                                "inner_hits": {},
                                "boost": 2
                            }
                        }
                    ]
                }
            }
        }
        if temporal_constraints:
            if tmp == 'must':
                q['query']['bool']['must'] += temporal_constraints
            else:
                q['query']['bool']['must'] = temporal_constraints
        if locations:
            q['query']['bool'][tmp].append({
                "constant_score": {
                    "filter": {
                        "terms": {
                            "locations": entities
                        }
                    }
                }
            })
        return self.es.search(index=self.indexName, doc_type='table', body=q, size=limit, from_=offset)

    def searchText(self, term, limit=10, offset=0, temporal_constraints=None):
        q = {
            "_source": ["url", "column.header.value", "portal.*", "dataset.*", "locations", "temporal_start",
                        "temporal_end"],
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
        if temporal_constraints:
            q['query']['bool']['must'] = temporal_constraints

        return self.es.search(index=self.indexName, doc_type='table', body=q, size=limit, from_=offset)

    def update(self, id, fields):
        q = {
            "doc": fields
        }
        return self.es.update(index=self.indexName, doc_type='table', id=id, body=q)

    def get_temporal_constraints(self, start, end):
        q = None
        if start or end:
            q = []
            if start:
                q.append({
                    "range": {
                        "temporal_start": {
                            "gte": start,
                        }
                    }
                })
            if end:
                q.append({
                    "range": {
                        "temporal_end": {
                            "lt": end
                        }
                    }
                })
        return q


MAX_STRING_LENGTH = 20


def _get_doc_headers(doc, row_cutoff):
    headers = []
    if 'column' in doc['_source']:
        for h in doc['_source']['column']:
            if 'header' in h:
                v = h['header'][0]['value'][0]
                headers.append(v[:MAX_STRING_LENGTH] + '...' if len(v) > MAX_STRING_LENGTH and row_cutoff else v)
    return headers


def format_results(results, row_cutoff, dataset=False):
    """Print results nicely:
    doc_id) content
    """
    datasets = defaultdict(list)
    data = []
    for doc in results['hits']['hits']:
        d = {"url": doc['_source']['url'], "portal": doc['_source']['portal']['uri']}
        d["headers"] = _get_doc_headers(doc, row_cutoff)

        if dataset and "dataset_link" in d["portal"]:
            d_link = d["portal"]["dataset_link"]
            datasets[d_link].append()

        for f in ['dataset', 'locations', 'temporal_start', 'temporal_end']:
            if f in doc['_source']:
                d[f] = doc['_source'][f]

        if 'row' in doc['inner_hits'] and doc['inner_hits']['row']['hits']['total'] > 0:
            d['row'] = [c[:MAX_STRING_LENGTH] + '...' if len(c) > MAX_STRING_LENGTH and row_cutoff else c for c in
                        doc['inner_hits']['row']['hits']['hits'][0]['_source']['values']['exact']]
            d['row_no'] = doc['inner_hits']['row']['hits']['hits'][0]['_source']['row_no']
            if 'entities' in doc['inner_hits']['row']['hits']['hits'][0]['_source']:
                d['entities'] = doc['inner_hits']['row']['hits']['hits'][0]['_source']['entities']
        data.append(d)
    return data


def format_table(doc, row_cutoff, locationsearch, max_rows=500):
    d = {"url": doc['_source']['url'], "portal": doc['_source']['portal']['uri'],
         "publisher": doc['_source']['dataset'].get('publisher', ''),
         "title": doc['_source']['dataset'].get('dataset_name', ''), 'rows': []}
    d['headers'] = _get_doc_headers(doc, row_cutoff)

    d['locations'] = []
    if 'locations' in doc['_source']:
        for l in doc['_source']['locations']:
            geo_l = locationsearch.get(l)
            if geo_l:
                d['locations'].append({'name': geo_l['name'], 'uri': l})

    if 'row' in doc['_source']:
        rows = doc['_source']['row']
        for row_no, row in enumerate(rows):
            d_row = []
            for i, e in enumerate([c[:MAX_STRING_LENGTH] + '...' if len(c) > MAX_STRING_LENGTH and row_cutoff else c
                                   for c in row['values']['exact']]):
                entry = {'value': e}

                if 'entities' in row:
                    entry['entity'] = locationsearch.format_entities(row['entities'][i])

                d_row.append(entry)
            d['rows'].append(d_row)
            if row_no > max_rows:
                break
    return d
