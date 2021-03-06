import urllib2

import requests
from flask import current_app, jsonify, request, Response
from flask_restplus import Resource

from services import time_tagger
from ui.rest_api import api
from openstreetmap.osm_inserter import get_geonames_url
import csv
import StringIO
import sparql
import urllib



get_ns = api.namespace('get', description='Operations to get the datasets')
rdf_ns = api.namespace('rdf', description='Operations to convert the data to RDF')
temporal_ns = api.namespace('temporal', description='Operations to get the temporal KG')
random_ns = api.namespace('random', description='Operations to generate random samples for evaluation')
index_ns = api.namespace('index', description='Operations to index new datasets')


PERIOD_QUERY = "PREFIX timex: <http://data.wu.ac.at/ns/timex#> \n" \
               "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n" \
               "SELECT ?name ?value ?start ?end WHERE {{ " +\
               "  ?value rdfs:label ?name. " +\
               "  ?name bif:contains \"\'{0}*\'\" . " +\
               "  ?value timex:hasStartTime ?start ; "+ \
               "       timex:hasEndTime ?end . }} "

ALL_PERIODS_LIMIT = "PREFIX timex: <http://data.wu.ac.at/ns/timex#> \n" \
               "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n" \
               "SELECT ?name ?value ?start ?end WHERE {{ " + \
               "  ?value rdfs:label ?name. " + \
               "  ?value timex:hasStartTime ?start ; " + \
               "       timex:hasEndTime ?end . }} LIMIT {0} "

START_END_QUERY = "PREFIX timex: <http://data.wu.ac.at/ns/timex#> \n" \
                  "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n" \
                "SELECT ?start ?end WHERE {{ " +\
                " <{0}> timex:hasStartTime ?start ; "+ \
                "      timex:hasEndTime ?end . }} "


@temporal_ns.route('/periods')
@temporal_ns.doc(params={'q': 'filter string'}, description="Returns all time periods")
class GetPeriods(Resource):
    def __init__(self, *args, **kwargs):
        super(GetPeriods, self).__init__(*args, **kwargs)

        sparql_endpoint = current_app.config['SPARQL']
        self.s = sparql.Service(sparql_endpoint, "utf-8", "GET")

    def get(self):
        q = request.args.get("q")
        limit = request.args.get("limit", 100)
        # only query for 3 or more letters
        data = {'success': False}
        result = None
        if not q or len(q) == 0:
            statement = ALL_PERIODS_LIMIT.format(limit)
            result = self.s.query(statement)
        else:
            statement = PERIOD_QUERY.format(q)
            result = self.s.query(statement)

        if result:
            data['success'] = True
            data['results'] = []
            for r in result.fetchone():
                data['results'].append({'name': r[0].value, 'value': r[1].value, 'start': r[2].value, 'end': r[3].value})
        return jsonify(data)


@temporal_ns.route('/period')
@temporal_ns.doc(params={'id': 'ID of the period'}, description="Returns start and end time ")
class GetSinglePeriod(Resource):
    def __init__(self, *args, **kwargs):
        super(GetSinglePeriod, self).__init__(*args, **kwargs)
        sparql_endpoint = current_app.config['SPARQL']
        self.s = sparql.Service(sparql_endpoint, "utf-8", "GET")

    def get(self):
        pid = request.args.get("id")
        # only query for 3 or more letters
        data = {'success': False}
        if pid:
            statement = START_END_QUERY.format(pid)
            result = self.s.query(statement)
            data['success'] = True
            for r in result.fetchone():
                data['result'] = {'value': pid, 'start': r[0].value, 'end': r[1].value}
                break
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


datasets_parser = api.parser()
datasets_parser.add_argument('limit', type=int, required=False, default=100)
datasets_parser.add_argument('offset', type=int, required=False, help="For traditional pagination, alternative to 'scroll_id'.")
datasets_parser.add_argument('scroll_id', required=False, help="For pagination. See 'scroll_id' and 'next' field in the response.")
datasets_parser.add_argument('has_features', type=str, required=False, help='List of required features (currently supported are {temporal, geolocation})')
datasets_parser.add_argument('l', type=str, required=False, help="Location ID of the form gn:GEONAMES-ID. Multiple parameters possible")
datasets_parser.add_argument('q', type=str, required=False, help="Full-text search query")
datasets_parser.add_argument('temp_start', type=str, required=False, help="Filter datasets where a CSV column contains date values greater than the given date (Y-m-d)")
datasets_parser.add_argument('temp_end', type=str, required=False, help="Filter datasets where a CSV column contain date values smaller than the given date (Y-m-d)")
datasets_parser.add_argument('temp_mstart', type=str, required=False, help="Filter datasets where title & description contain date values greater than the given date (Y-m-d)")
datasets_parser.add_argument('temp_mend', type=str, required=False, help="Filter datasets where title & description contain date values smaller than the given date (Y-m-d)")
datasets_parser.add_argument('pattern', type=str, required=False, choices=['static', 'daily', 'weekly', 'monthly', 'quarterly', 'yearly', 'other'], help="Filter datasets where a CSV column contains dates with a required pattern.")

@get_ns.expect(datasets_parser)
@get_ns.route('/datasets')
@get_ns.doc(description="Search for indexed CSVs")
class GetCSV(Resource):
    def get(self):
        es_search = current_app.config['ELASTICSEARCH']

        scroll_id = request.args.get('scroll_id')
        if scroll_id:
            res = es_search.es.scroll(scroll_id=scroll_id, scroll='1m')
        else:
            ls = request.args.getlist("l")
            q = request.args.get("q")
            temp_start = request.args.get("start")
            temp_end = request.args.get("end")
            temp_mstart = request.args.get("mstart")
            temp_mend = request.args.get("mend")
            pattern = request.args.get("pattern")
            features = request.args.get("features")
            limit = request.args.get("limit")
            offset = request.args.get("offset")

            temporal_constraints = es_search.get_temporal_constraints(temp_mstart, temp_mend, temp_start, temp_end, pattern)

            if ls:
                ls = [get_geonames_url(l[3:]) if l.startswith('gn:') else l for l in ls]

                if q:
                    res = es_search.searchEntitiesAndText(entities=ls, term=q, limit=limit, offset=offset, features=features, temporal_constraints=temporal_constraints)
                else:
                    res = es_search.searchEntities(entities=ls, limit=limit, offset=offset, features=features, temporal_constraints=temporal_constraints)

            elif q:
                res = es_search.searchText(q, limit=limit, offset=offset, features=features, temporal_constraints=temporal_constraints)

            else:
                res = es_search.get_all(limit, scroll_id, features=features, temporal_constraints=temporal_constraints)

        if '_scroll_id' in res:
            scrollId = res['_scroll_id']
            res['next'] = request.base_url + "?" + urllib.urlencode({'scroll_id': scrollId})
        return res



@rdf_ns.route('/portal')
@rdf_ns.doc(params={'portal': "The data portal ID"}, description="Returns the geo-entities of an indexed data portal as RDF")
class GetPortalRDF(Resource):
    def get(self):
        portal = request.args.get("portal")
        es_search = current_app.config['ELASTICSEARCH']
        location_search = current_app.config['LOCATION_SEARCH']
        nt_file = es_search.get_portal(portal, location_search)
        return Response(nt_file, mimetype='text/plain')


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



@index_ns.route('/ckan')
@index_ns.doc(params={'url': "The link to the CKAN metadata description", 'portal_url': "The portal URL", 'iso2': "The ISO2 country code of the portal", 'delimiter': "The CSV delimiter. Default is a detection algorithm."},
              description="Index a dataset by its CKAN metadata description")
class GetCSV(Resource):
    def get(self):
        url = request.args.get("url")
        delimiter = request.args.get("delimiter")
        es = current_app.config['ELASTICSEARCH']
        return_data = {}

        portalInfo = {
            "id": '',
            "iso": request.args.get("iso2"),
            "uri": request.args.get("portal_url"),
            "software": 'CKAN',
            "apiuri": request.args.get("portal_url")
        }

        res = requests.get(url)
        if res.status_code == 200:
            dataset = res.json()
            if 'result' in dataset:
                dataset = dataset['result']

            dataset_name = dataset.get('title', '')
            dataset_link = url
            dataset_description = dataset.get('notes', '')
            keywords = [t['name'] for t in dataset.get('tags', []) if isinstance(t, dict) and 'name' in t]
            publisher = dataset.get('publisher', '')
            publisher_link = dataset.get('publisher_link', '')
            publisher_email = dataset.get('publisher_email', '')

            for dist in dataset.get('resources', []):
                dist_url = dist['url']
                dist_format = dist.get('format', '')
                if 'csv' in dist_format.lower():
                    dsfields = {}
                    fields = {'dataset': dsfields}
                    if time_tagger:
                        start, end, published, modified = time_tagger.get_temporal_information(dist, dataset,
                                                                                               heideltime_path=current_app.config['HEIDELTIME'])
                        if start and end:
                            fields['metadata_temp_start'] = start
                            fields['metadata_temp_end'] = end
                        if published:
                            fields['published'] = published
                        if modified:
                            fields['modified'] = modified

                    name = dist.get('name', '')
                    if name:
                        dsfields['name'] = name
                    if dataset_name:
                        dsfields['dataset_name'] = dataset_name
                    if dataset_link:
                        dsfields['dataset_link'] = dataset_link
                    if dataset_description:
                        dsfields['dataset_description'] = dataset_description
                    if keywords and isinstance(keywords, list):
                        dsfields['keywords'] = keywords
                    if publisher:
                        dsfields['publisher'] = publisher
                    if publisher_link:
                        dsfields['publisher_link'] = publisher_link
                    if publisher_email:
                        dsfields['publisher_email'] = publisher_email

                    response = urllib2.urlopen(dist_url)
                    content = response.read()
                    resp = es.indexTable(url=dist_url, content=content, portalInfo=portalInfo, datasetInfo=fields,
                                         geotagging=current_app.config['GEO_TAGGER'], store_labels=True, index_errors=False, delimiter=delimiter)
                    return_data[dist_url] = resp

        return jsonify(return_data)




