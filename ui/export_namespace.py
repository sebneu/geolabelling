
from flask import current_app, jsonify, request, Response
from flask_restplus import Resource
from ui.rest_api import api

get_ns = api.namespace('get', description='Operations to get the datasets')

rdf_ns = api.namespace('rdf', description='Operations to convert the data to RDF')


@get_ns.route('/urls')
@get_ns.doc(params={'portal': 'filter by urls from portal'}, description="Returns all indexed urls")
class GetURLs(Resource):
    def get(self):
        portal = request.args.get("portal")
        es_search = current_app.config['ELASTICSEARCH']
        urls = es_search.get_urls(portal)
        return jsonify(list(urls))


@get_ns.route('/get/<path:url>')
@get_ns.doc(params={'url': "The CSV's URL"}, description="Get an indexed CSV by its URL")
class GetCSV(Resource):
    def get(self, url):
        es_search = current_app.config['ELASTICSEARCH']
        return es_search.get(url)


@rdf_ns.route('/labels/<path:url>')
@rdf_ns.doc(params={'url': "The CSV's URL"}, description="Returns the geo-entities of an indexed CSV as RDF")
class GetCSVTriples(Resource):
    def get(self, url):
        es_search = current_app.config['ELASTICSEARCH']
        location_search = current_app.config['LOCATION_SEARCH']
        nt_file = es_search.get_triples(url, location_search=location_search)
        return Response(nt_file, mimetype='text')
