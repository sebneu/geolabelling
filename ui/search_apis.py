import json
import logging
from collections import defaultdict

import yaml
import pymongo
from pymongo import MongoClient
import urllib
import random
from elasticsearch import Elasticsearch

from openstreetmap.osm_inserter import get_geonames_url
from ui.utils import mongo_collections_utils
from ui.utils import export_rdf

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

        cursor = self.geonames.find({'$text': {'$search': q}}, {'score': {'$meta': "textScore"}})
        cursor.sort([('score', {'$meta': 'textScore'})])
        if limit >= 0:
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
        if limit >= 0:
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
        cursor = self.postalcodes.find({'_id': {'$regex': '^' + q}})
        if limit >= 0:
            cursor.limit(limit)
        for res in cursor:
            try:
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
            except Exception as e:
                logging.error(e)
        return results

    def get_nuts(self, q, search_api, limit=5):
        results = []
        cursor = self.nuts.find({'_id': {'$regex': '^' + q}})
        if limit >= 0:
            cursor.limit(limit)

        for res in cursor:
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

            self.es = Elasticsearch(hosts=[host], timeout=30, max_retries=10, retry_on_timeout=True)
            self.indexName = conf['es']['indexName']
        else:
            self.es = Elasticsearch()
            self.indexName = indexName

    def get(self, url, columns=True, rows=True):
        include = ['column.header.value', 'row.*', 'no_columns', 'no_rows', 'portal.*', 'url', 'metadata_entities', 'data_entities', 'dataset.*']
        exclude = []
        if columns:
            include.append("column.*")
        if not rows:
            exclude.append("row.*")
        res = self.es.get(index=self.indexName, doc_type='table', id=url, _source_exclude=exclude,
                          _source_include=include)
        return res

    def _get_labelled_rows(self, doc, locationsearch):
        rows = []
        for row in doc['_source'].get('row', []):
            r = []
            row_v = row['values']['value']
            if 'entities' in row:
                row_e = [ ]
                for e in row['entities']:
                    if not e:
                        row_e.append('')
                    else:
                        if e.startswith('http://sws.geonames.org/'):
                            geo_id = locationsearch.get(e)
                            name = geo_id.get('name', '')
                            parent = locationsearch.get(geo_id['parent'])['name'] if 'parent' in geo_id else ''
                            country = locationsearch.get(geo_id['country'])['name'] if 'country' in geo_id else ''
                        else:
                            osm_id = locationsearch.get_osm(e)
                            name = osm_id.get('name', '')
                            parent = ''
                            country = ''
                            if 'geonames_ids' in osm_id and len(osm_id['geonames_ids']) > 0:
                                p_e = locationsearch.get(get_geonames_url(osm_id['geonames_ids'][0]))
                                parent = p_e['name']
                                country = locationsearch.get(p_e['country'])['name'] if 'country' in p_e else ''
                        form_e = locationsearch.format_entities(e)
                        row_e.append(' '.join([name,parent,country,form_e]).encode('utf-8'))
            else:
                row_e = ['' for _ in range(len(row_v))]
            for v, e in zip(row_v, row_e):
                r.append(v.encode('utf-8'))
                r.append(e)
            rows.append(r)
        return rows

    def getRandomRows(self, url, sample_size, locationsearch):
        res = self.get(url, columns=False)
        d = []
        header = _get_doc_headers(res, False)
        if header:
            r = []
            for h in header:
                r.append(h.encode('utf-8'))
                r.append('')
            d.append(r)
        rows = self._get_labelled_rows(res, locationsearch)
        if len(rows) >= sample_size:
            return d + random.sample(rows, sample_size)
        else:
            return d + rows

    def get_portal(self, portal_id, location_search):
        g = rdflib.Graph()
        for u in self.get_urls(portal=portal_id):
            self._get_triples(g, u, location_search)
        return g.serialize(format='nt')


    def _get_triples(self, graph, url, location_search):
        #include = ['column.header.value', 'column.*', 'no_columns', 'no_rows', 'portal.*', 'url', 'metadata_entities', 'data_entities', 'dataset.*']
        exclude = ['row.*']
        res = self.es.get(index=self.indexName, doc_type='table', id=url, _source_exclude=exclude)
        # convert column to RDF
        export_rdf.addMetadata(res['_source'], graph, location_search)

    def get_triples(self, url, location_search):
        g = rdflib.Graph()
        self._get_triples(g, url, location_search)
        # add data portal
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
        res = self.es.search(index=self.indexName, doc_type='table', body=q, size=limit, scroll=scroll)

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

    def getRandomURLs(self, portal, no_urls, columnlabels):
        if portal:
            p = {"nested": {"path": "portal", "query": {"term": {"portal.id": portal}}}}
        else:
            p = {"match_all": {}}

        q = {
            "_source": "_id",
            "query": {
                "function_score": {
                    "query": {
                        "bool": {
                            "must": [
                                p, {
                                    "nested": {
                                        "path": "column",
                                        "query": {
                                            "exists": {
                                                "field": "column.entities" if columnlabels else "column.values"
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    },
                    "functions": [{
                        "random_score": {
                            "seed": "1470617293028"
                        }
                    }]
                }
            }
        }
        res = self.es.search(index=self.indexName, doc_type='table', body=q, size=no_urls)
        urls = [u['_id'] for u in res['hits']['hits']]
        return urls


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
            "_source": ["url", "column.header.value", "portal.*", "dataset.*", "metadata_entities', 'data_entities", "metadata_temp_start",
                        "metadata_temp_end", 'data_temp_start', 'data_temp_end', 'data_temp_pattern'],
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
                            "metadata_entities": entities
                        }
                    }
                }
            })
        return self.es.search(index=self.indexName, doc_type='table', body=q, size=limit, from_=offset)

    def searchEntities(self, entities, locations=None, limit=10, offset=0, intersect=False, temporal_constraints=None):
        entities = [e[4:] if e.startswith('osm:') else e for e in entities]
        tmp = 'should'
        if intersect:
            tmp = 'must'
        q = {
            "_source": ["url", "column.header.value", "portal.*", "dataset.*", 'metadata_entities', 'data_entities', "metadata_temp_start",
                        "metadata_temp_end", 'data_temp_start', 'data_temp_end', 'data_temp_pattern'],
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
                            "metadata_entities": entities
                        }
                    }
                }
            })
        return self.es.search(index=self.indexName, doc_type='table', body=q, size=limit, from_=offset)

    def searchText(self, term, limit=10, offset=0, temporal_constraints=None):
        q = {
            "_source": ["url", "column.header.value", "portal.*", "dataset.*", 'metadata_entities', 'data_entities', "metadata_temp_start",
                        "metadata_temp_end", 'data_temp_start', 'data_temp_end', 'data_temp_pattern'],
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

    def get_temporal_constraints(self, metadata_start, metadata_end, start, end, pattern):
        q = []
        if start:
            q.append({
                "range": {
                    "data_temp_start": {
                        "gte": start,
                    }
                }
            })
        if end:
            q.append({
                "range": {
                    "data_temp_end": {
                        "lt": end
                    }
                }
            })
        if metadata_start:
            q.append({
                "range": {
                    "metadata_temp_start": {
                        "gte": metadata_start,
                    }
                }
            })
        if metadata_end:
            q.append({
                "range": {
                    "metadata_temp_end": {
                        "lt": metadata_end
                    }
                }
            })
        if pattern:
            q.append({
                "term": {
                    "data_temp_pattern": pattern
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

        for f in ['dataset', 'locations', 'metadata_temp_start', 'metadata_temp_end']:
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
    if 'metadata_entities' in doc['_source']:
        for l in doc['_source']['metadata_entities']:
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
