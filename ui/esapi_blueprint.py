import urllib
from urlparse import urlparse

from flask import Blueprint, current_app, request, redirect, url_for, jsonify, json
from flask_cors import CORS
import pprint

esapi = Blueprint('esapi', __name__ )
CORS(esapi, origins=['*'])

@esapi.route('/table/_msearch', methods=['GET','POST'])
def location_msearch():
    limit = request.args.get("limit", 10)
    offset = request.args.get("offset", 0)
    ls = request.args.getlist("l")
    p = request.args.get("p")
    q = request.args.get("q")

    res = {'responses':[]}

    if request.method == 'POST':
	data = request.get_data()
	matchall_pos = data.find('"match_all":{}')

	if matchall_pos > 0:
                es_search = current_app.config['ELASTICSEARCH']
 		res = es_search.es.search(index=es_search.indexName, doc_type='table', 
				_source = ["url", "column.header.value", "portal.*", "dataset.*"],
				body={'query':{'match_all':{}}}, 
				size=30, from_=0)

		return jsonify({'responses':[res]})

	secondbr = data.find('{',1)

	if secondbr < 0:
		return jsonify(res)

	data = data[secondbr:]

	data = json.loads(data)

	print(pprint.pformat(data))

	if not data or not data['query']:
		return jsonify(res)

	if data['from']:
		offset = data['from']
	if data['size']:
		limit = data['size']

	ls = list(find('l', data))
	q = next(iter(find('q', data)), None) 

	print("POST request: l is {}, q is {}\n".format(ls, q))

    es_search = current_app.config['ELASTICSEARCH']
    #locationsearch = current_app.config['LOCATION_SEARCH']
    if ls and not q: 
	res = es_search.searchEntities(entities=ls, limit=limit, offset=offset)
    elif q and not ls:
	res = es_search.searchText(term=q, limit=limit, offset=offset)
    elif ls and q: 
        res = es_search.searchEntitiesAndText(entities=ls, term=q, limit=limit, offset=offset)

    return jsonify({'responses':[res]})


def find(key, dictionary):
    for k, v in dictionary.iteritems():
        if k == key:
            yield v
        elif isinstance(v, dict):
            for result in find(key, v):
                yield result
        elif isinstance(v, list):
            for d in v:
                for result in find(key, d):
                    yield result





#    columns = request.args.get("columns")
#    es_search = current_app.config['ELASTICSEARCH']
#    if columns:
#        result = es_search.get(url, rows=False)
#    else:
#        result = es_search.get(url, columns=False)
#    resp = jsonify(result)
#    return resp


