
from elasticsearch import Elasticsearch
import os
import csv

def iter_datasets(es):
    q = {
        "size": 100,
        "query": {
            "nested": {
                "path": "column",
                "query": {
                    "exists": {
                    "field": "column.entities"
                    }
                }
            }
        }
    }

    rows = []
    annotations = []

    res = get_all(es, q, 'eucsv', scroll_id=None)
    total = 0
    scrollId = None
    while '_scroll_id' in res and total < res['hits']['total']:
        scrollId = res['_scroll_id']
        for doc in res['hits']['hits']:
            total += 1
            if total % 100 == 0:
                print 'processed: ', total
            if len(doc['_source']['column']) > 0:
                rows.append(len(doc['_source']['column'][0]['values']['value']))
            for column in doc['_source']['column']:
                if 'entities' in column:
                    annotations.append(len(filter(None, column['entities']))/float(len(column['entities'])))

        res = get_all(es, q, 'eucsv', scroll_id=scrollId)
    print 'total:', total

    print 'mean'
    print 'rows', str(sum(rows) / float(len(rows)))
    print 'annotations', str(sum(annotations) / float(len(annotations)))

    print 'median'
    print 'rows', median(rows)
    print 'annotations', median(annotations)


def get_all(es, q, indexName, scroll_id):
    if scroll_id:
        res = es.scroll(scroll_id=scroll_id, scroll='1m')
    else:
        res = es.search(index=indexName, doc_type='table', body=q, scroll='1m')
    if '_shards' in res:
        del res['_shards']
    return res


def get(es, url, indexName):
    include = ['column.*']

    res = es.get(index=indexName, doc_type='table', id=url, _source_include=include)
    return res

def median(numericValues):
    theValues = sorted(numericValues)

    if len(theValues) % 2 == 1:
        return theValues[(len(theValues)+1)/2-1]
    else:
        lower = theValues[len(theValues)/2-1]
        upper = theValues[len(theValues)/2]
        return (float(lower + upper)) / 2


def jws_eval_stats(es):

    rows = []
    annotations = []
    total = 0
    with open('../jws_evaluation/index.csv') as f:
        csvr = csv.reader(f)
        csvr.next()
        for row in csvr:
            url = row[0]
            if url == '------':
                break
            total += 1
            res = get(es, url, 'eucsv')
            if len(res['_source']['column']) > 0:
                rows.append(len(res['_source']['column'][0]['values']['value']))
            for column in res['_source']['column']:
                if 'entities' in column:
                    annotations.append(len(filter(None, column['entities'])) / float(len(column['entities'])))

    print 'total: ', total
    print 'mean'
    print 'rows', str(sum(rows) / float(len(rows)))
    print 'annotations', str(sum(annotations) / float(len(annotations)))

    print 'median'
    print 'rows', median(rows)
    print 'annotations', median(annotations)



if __name__ == '__main__':
    es = Elasticsearch(hosts=[{'host': 'localhost', 'port': '9200'}], timeout=30, max_retries=10, retry_on_timeout=True)
    iter_datasets(es)
    #jws_eval_stats(es)
