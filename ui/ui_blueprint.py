from urlparse import urlparse

import jinja2
from flask import Blueprint, current_app, render_template, request, redirect, url_for, jsonify
from geo_tagger import POSTAL_PATTERN, NUTS_PATTERN
from ui import search_apis

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


@ui.route('/search', methods=['GET'])
def search():
    l = request.args.get("l")
    q = request.args.get("q")
    data = {'total': 0, 'results': []}
    es_search = current_app.config['ELASTICSEARCH']
    if q:
        data['keyword'] = q
    if l:
        res = es_search.searchEntity(l)
        # data['pages'] = [page_i + 1 for page_i, i in enumerate(range(1, res['hits']['total'], limit))]
        data['total'] += res['hits']['total']
        data['results'] += search_apis.format_results(res)
    elif q:
        text_res = es_search.searchText(q)
        data['total'] += text_res['hits']['total']
        data['results'] += search_apis.format_results(text_res)
    return render('index.jinja', data)


@ui.route('/geotagging', methods=['GET'])
def geotagging():
    return render("geolocation.jinja")


@ui.route('/geotagging/service', methods=['GET'])
def geotagging_service():
    url = request.args.get("url")
    #testfile = '/home/neumaier/Repos/odgraph/local/testdata/data_gv_at/httpdata.linz.gv.atkatalogstadtgebaeudeanzahlwohnungen2006tgeanzwg2006.csv'
    #testfile = '/home/neumaier/Repos/odgraph/local/testdata/hunde-wien.csv'
    #testfile = '/home/neumaier/Repos/odgraph/local/testdata/plz.csv'
    title = url[-20:]

    #with open(testfile) as f:
    #    table = f.read()

    geotagger = current_app.config['GEO_TAGGER']
    data = geotagger.from_table(url=url)
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

    if url:
        return redirect(url_for(service, url=url))
    else:
        return redirect(url_for(service))


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