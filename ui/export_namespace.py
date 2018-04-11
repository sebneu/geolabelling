
from flask import current_app, jsonify, request, Response
from flask_restplus import Resource, inputs, Api
from ui.rest_api import api
import csv
import StringIO
import sparql


get_ns = api.namespace('get', description='Operations to get the datasets')
rdf_ns = api.namespace('rdf', description='Operations to convert the data to RDF')
temporal_ns = api.namespace('temporal', description='Operations to get the temporal KG')
random_ns = api.namespace('random', description='Operations to generate random samples for evaluation')


PERIOD_QUERY = "PREFIX timex: <http://data.wu.ac.at/ns/timex#> \n" \
               "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n" \
               "SELECT ?name ?value ?start ?end WHERE {{ " +\
               "  ?value rdfs:label ?name. " +\
               "  ?name bif:contains \"\'{0}*\'\" . " +\
               "  ?value timex:hasStartTime ?start ; "+ \
               "       timex:hasEndTime ?end . }} "


@temporal_ns.route('/periods')
@temporal_ns.doc(params={'q': 'filter'}, description="Returns all time periods")
class GetPeriods(Resource):
    def __init__(self, *args, **kwargs):
        super(GetPeriods, self).__init__(*args, **kwargs)

        sparql_endpoint = current_app.config['SPARQL']
        self.s = sparql.Service(sparql_endpoint, "utf-8", "GET")

    def get(self):
        q = request.args.get("q")
        # only query for 3 or more letters
        data = {'success': False}
        if len(q) >= 3:
            statement = PERIOD_QUERY.format(q)
            result = self.s.query(statement)
            if result:
                data['results'] = []
                for r in result.fetchone():
                    data['success'] = True
                    data['results'].append({'name': r[0].value, 'value': r[1].value, 'start': r[2].value, 'end': r[3].value})
        return jsonify(data)


urls_parser = api.parser()
urls_parser.add_argument('columnlabels', choices=['all', 'geonames', 'none'], default='all')

@get_ns.expect(urls_parser)
@get_ns.route('/urls')
@get_ns.doc(params={'portal': 'filter by urls from portal'}, description="Returns all indexed urls")
class GetURLs(Resource):
    def get(self):
        portal = request.args.get("portal")
        columnlabels = request.args.get('columnlabels')
        es_search = current_app.config['ELASTICSEARCH']
        urls = es_search.get_urls(portal, columnlabels)
        data = '\n'.join(urls)
        return Response(data, mimetype='text/plain')


@get_ns.route('/dataset')
@get_ns.doc(params={'url': "The CSV's URL"}, description="Get an indexed CSV by its URL")
class GetCSV(Resource):
    def get(self):
        url = request.args.get("url")
        es_search = current_app.config['ELASTICSEARCH']
        return es_search.get(url)



@rdf_ns.route('/portal')
@rdf_ns.doc(params={'portal': "The data portal ID"}, description="Returns the geo-entities of an indexed data portal as RDF")
class GetPortalRDF(Resource):
    def get(self):
        portal = request.args.get("portal")
        es_search = current_app.config['ELASTICSEARCH']
        location_search = current_app.config['LOCATION_SEARCH']
        nt_file = es_search.get_portal(portal, location_search)
        return Response(nt_file, mimetype='text/plain')


@rdf_ns.route('/dataset')
@rdf_ns.doc(params={'url': "The CSV's URL"}, description="Returns the geo-entities of an indexed CSV as RDF")
class GetCSVTriples(Resource):
    def get(self):
        url = request.args.get("url")
        es_search = current_app.config['ELASTICSEARCH']
        location_search = current_app.config['LOCATION_SEARCH']
        nt_file = es_search.get_triples(url, location_search=location_search)
        return Response(nt_file, mimetype='text/plain')



randurls_parser = api.parser()
randurls_parser.add_argument('urls', type=int, default=10)
randurls_parser.add_argument('columnlabels', type=bool, default=True)

@get_ns.expect(randurls_parser)
@random_ns.route('/urls')
@get_ns.doc(params={'portal': 'filter by urls from portal'}, description="Returns all indexed urls")
class GetURLs(Resource):
    def get(self):
        portal = request.args.get("portal")
        columnlabels = True if request.args.get('columnlabels')=='true' else False
        no_urls = int(request.args.get('urls'))
        es_search = current_app.config['ELASTICSEARCH']
        res = es_search.getRandomURLs(portal, no_urls, columnlabels)
        txt_file = '\n'.join(res)
        return Response(txt_file, mimetype='text/plain')



randrows_parser = api.parser()
randrows_parser.add_argument('rows', type=int, default=10)

@get_ns.expect(randrows_parser)
@random_ns.route('/dataset')
@random_ns.doc(params={'url': "The CSV's URL"}, description="Get an indexed CSV by its URL")
class GetCSV(Resource):
    def get(self):
        url = request.args.get("url")
        rows = int(request.args.get('rows'))
        es_search = current_app.config['ELASTICSEARCH']
        location_search = current_app.config['LOCATION_SEARCH']
        res = es_search.getRandomRows(url, rows, locationsearch=location_search)
        # generate csv file
        si = StringIO.StringIO()
        cw = csv.writer(si)
        for row in res:
            cw.writerow(row)
        csv_file = si.getvalue().strip('\r\n')
        return Response(csv_file, mimetype='text/csv')



