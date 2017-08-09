import urllib
from urlparse import urlparse

import jinja2
from flask import Blueprint, current_app, render_template, request, redirect, url_for, jsonify
from geo_tagger import POSTAL_PATTERN, NUTS_PATTERN
from ui import search_apis
from ui.utils import mongo_collections_utils

ui = Blueprint('ui', __name__,
               template_folder='./templates',
               static_folder='./static',
               )


# using the method
@jinja2.contextfilter
def get_domain(context, url):
    return "%s" % urlparse(url).netloc


ui.add_app_template_filter(get_domain)


##-----------Helper Functions -----------##
def render(templateName, data=None, **kwargs):
    """
    FLask Jinja rendering function
    :param templateName: jinja template name
    :param data: json data for the template
    :return: html
    """
    if data is None:
        data = {}
    return render_template(templateName, data=data, **kwargs)


def decode_utf8(string):
    if isinstance(string, str):
        for encoding in (('utf-8',), ('windows-1252',), ('utf-8', 'ignore')):
            try:
                return string.decode(*encoding)
            except:
                pass
        return string # Don't know how to handle it...
    return unicode(string, 'utf-8')


@ui.route('/', methods=['GET'])
def index():
    return render('index.jinja')


def try_page(var):
    try:
        var = int(var)
        return var
    except Exception:
        return 1


@ui.route('/get/<path:url>', methods=['GET'])
def gettable(url):
    columns = request.args.get("columns")
    es_search = current_app.config['ELASTICSEARCH']
    if columns:
        result = es_search.get(url, rows=False)
    else:
        result = es_search.get(url, columns=False)
    resp = jsonify(result)
    return resp


@ui.route('/geonames', methods=['GET'])
def geonamesapi():
    q = request.args.get("q")
    l = request.args.get("l")
    if not q and not l:
        resp = jsonify({'error': 'no keyword or link supplied. Use argument q or l'})
        # resp.status_code = 404
        return resp
    locationsearch = current_app.config['LOCATION_SEARCH']
    if q:
        results = locationsearch.get_geonames(q)
        resp = jsonify(results)
        return resp
    if l:
        results = locationsearch.get(l)
        resp = jsonify(results)
        return resp



@ui.route('/searchapi', methods=['GET'])
def searchapi():
    q = request.args.get("q")
    if not q:
        resp = jsonify({'error': 'no keyword supplied. Use argument q'})
        # resp.status_code = 404
        return resp

    locationsearch = current_app.config['LOCATION_SEARCH']
    search_api = url_for('.search')
    results = locationsearch.get_by_substring(q, search_api)
    resp = {
      "results": {
        "locations": {
          "name": "Locations",
          "results": results
        }
      }
    }

    if POSTAL_PATTERN.match(q):
        postalcodes = locationsearch.get_postalcodes(q, search_api)
        resp["results"]["postalcodes"] = {
            "name": "Postal codes",
            "results": postalcodes
        }

    if NUTS_PATTERN.match(q):
        nuts = locationsearch.get_nuts(q, search_api)
        resp["results"]["nutscodes"] = {
            "name": "NUTS codes",
            "results": nuts
        }

    resp = jsonify(resp)
    return resp


def get_search_results(row_cutoff, aggregated_locations, limit=10, offset=0):
    ls = request.args.getlist("l")
    p = request.args.get("p")
    q = request.args.get("q")
    temp_start = request.args.get("start")
    temp_end = request.args.get("end")
    limit = request.args.get("limit", limit)
    offset = request.args.get("offset", offset)
    options = request.args.get("options")

    data = {'total': 0, 'results': [], 'entities': []}
    es_search = current_app.config['ELASTICSEARCH']
    locationsearch = current_app.config['LOCATION_SEARCH']

    temporal_constraints = es_search.get_temporal_constraints(temp_start, temp_end)

    if ls:
        if q:
            res = es_search.searchEntitiesAndText(ls, q, aggregated_locations, limit=limit, offset=offset, temporal_constraints=temporal_constraints)
        else:
            res = es_search.searchEntities(ls, aggregated_locations, limit=limit, offset=offset, temporal_constraints=temporal_constraints)

        # data['pages'] = [page_i + 1 for page_i, i in enumerate(range(1, res['hits']['total'], limit))]
        data['total'] += res['hits']['total']
        data['results'] += search_apis.format_results(res, row_cutoff)

        for l in ls:
            entity = {'link': l}
            data['entities'].append(entity)
            if 'geonames' in l:
                # entity information
                name = locationsearch.get(l)['name']
                data['keyword'] = name
                if len(ls) > 1:
                    data['keyword'] = ''

                entity['name'] = name
                entity['external'] = locationsearch.get_external_links(l)
                parents = []
                search_api = url_for('.search')
                for name, p_l in locationsearch.get_parents(l):
                    link = search_api + '?' + urllib.urlencode({'q': name.encode('utf-8'), 'l': p_l})
                    parents.append({'name': name, 'link': p_l, 'search': link})
                entity['parents'] = parents
            else:
                nuts_e = locationsearch.get_nuts_by_geovocab(l)
                data['keyword'] = nuts_e['name']
                entity['name'] = nuts_e['name']
                parents = []
                search_api = url_for('.search')

                for i in range(2, len(nuts_e['_id'])+1):
                    nuts_id = nuts_e['_id'][:i]
                    nuts_c = locationsearch.get_nuts_by_id(nuts_id)
                    p_l = mongo_collections_utils.get_nuts_link(nuts_c)
                    link = search_api + '?' + urllib.urlencode({'l': p_l})
                    parents.append({'name': nuts_c['_id'], 'link': p_l, 'search': link})
                    entity['parents'] = parents

    elif p:
        country_code, code = p.split('#')
        country = locationsearch.get_country(country_code)
        entities = locationsearch.get_postalcode_mappings_by_country(code, country)
        res = es_search.searchEntities(entities, limit=limit, offset=offset, temporal_constraints=temporal_constraints)
        data['total'] += res['hits']['total']
        data['results'] += search_apis.format_results(res, row_cutoff)
        # entity information
        search_api = url_for('.search')
        link = search_api + '?' + urllib.urlencode({'q': country['name'].encode('utf-8'), 'l': country['_id']})
        data['entities'].append({'name': code, 'parents': [{'name': country['name'], 'link': country['_id'], 'search': link}]})
        data['keyword'] = code
    elif q:
        text_res = es_search.searchText(q, limit=limit, offset=offset, temporal_constraints=temporal_constraints)
        data['total'] += text_res['hits']['total']
        data['results'] += search_apis.format_results(text_res, row_cutoff)
        data['keyword'] = q

    return data


@ui.route('/search', methods=['GET', 'POST'])
def search():
    limit = 10
    page = try_page(request.form.get("page", 1))
    data = get_search_results(row_cutoff=True, aggregated_locations=True, limit=limit, offset=10 * (page-1))

    data['currentPage'] = page
    data['pages'] = [page_i + 1 for page_i, i in enumerate(range(1, data['total'], limit))]
    return render('index.jinja', data)


@ui.route('/locationsearch', methods=['GET'])
def locationsearch():
    data = get_search_results(row_cutoff=False, aggregated_locations=True)
    return jsonify(data)


@ui.route('/preview', methods=['GET'])
def preview():
    url = request.args.get("tableid")
    es_search = current_app.config['ELASTICSEARCH']
    doc = es_search.get(url, columns=False)
    res = search_apis.format_table(doc, row_cutoff=False)
    return jsonify({'data': render_template('preview_table.jinja', table=res), 'url': res['url'], 'portal': res['portal']})


@ui.route('/geotagging', methods=['GET'])
def geotagging():
    return render("geolocation.jinja")


@ui.route('/geotagging/service', methods=['GET'])
def geotagging_service():
    url = request.args.get("url")
    textUpload = request.args.get('textUpload')

    title = url[-20:] if url else "Copy & Paste input"

    #with open(testfile) as f:
    #    table = f.read()

    geotagger = current_app.config['GEO_TAGGER']
    if textUpload:
        textUpload = textUpload.encode('utf-8')
    data = geotagger.from_table(url=url, content=textUpload)
    #data['orig'] = decode_utf8(table)
    data['title'] = title
    return render_template("geotagging_results.jinja", data=data)


@ui.route('/api', methods=['GET'])
def apispec():
    return render('apiui.jinja')


@ui.route('/about', methods=['GET'])
def about():
    return render('about.jinja')


@ui.route('/submit', methods=['POST'])
def submit():
    # debug print
    current_app.logger.debug(request.form)
    service = request.form.get('service')

    ### CHECK INPUT PARAMETERS
    ###########################

    # check for url
    url = request.form.get('url')

    # check for file uploads
    file = request.files['uploadBtn']

    # check for copy paste
    textUpload = request.form.get('textUpload')
    ret = store_file(file=file, url=url, textUpload=textUpload)
    if ret:
        return ret

    return redirect(url_for(service, url=url, textUpload=textUpload))


def store_file(file, url, textUpload):
    pass


@ui.route('/get/geotagging', methods=['GET', 'POST'])
def get_geotagging_file():
    #filename = '/home/neumaier/Repos/odgraph/local/testdata/data_gv_at/httpckan.data.ktn.gv.atstoragef20130920T093A193A35.720Zlandtagswahlen2013.csv'
    #filename = '/home/neumaier/Repos/odgraph/local/testdata/data_gv_at/httpservice.stmk.gv.atogdOGDDataABT17statistikSTMK01012015SEX2015.csv'
    #with open(filename) as f:
    #    table = f.read()
    #return table
    return None