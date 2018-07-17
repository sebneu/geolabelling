# -*- coding: utf-8 -*-

import logging
from tornado.wsgi import WSGIContainer

from flask import Flask, Blueprint

from services import geo_tagger
from ui import search_apis
from ui.export_namespace import get_ns
from ui.export_namespace import rdf_ns
from ui.rest_api import api
from ui.ui_blueprint import ui
from ui.esapi_blueprint import esapi
from ui.utils import utils
from utils.error_handler import ErrorHandler


class ReverseProxied(object):
    '''Wrap the application in this middleware and configure the
    front-end server to add these headers, to let you quietly bind
    this to a URL other than / and to an HTTP scheme that is
    different than what is used locally.

    In nginx:
    location /myprefix {
        proxy_pass http://192.168.0.1:5001;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Scheme $scheme;
        proxy_set_header X-Script-Name /myprefix;
        }

    :param app: the WSGI application
    '''
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        script_name = environ.get('HTTP_X_SCRIPT_NAME', '')
        if script_name:
            environ['SCRIPT_NAME'] = script_name
            path_info = environ['PATH_INFO']
            if path_info.startswith(script_name):
                environ['PATH_INFO'] = path_info[len(script_name):]

        scheme = environ.get('HTTP_X_SCHEME', '')
        if scheme:
            environ['wsgi.url_scheme'] = scheme
        return self.app(environ, start_response)


def help():
    return "The User Interface and API for the data.wu.ac.at Open Data search: http://data.wu.ac.at/odgraphsearch/about"
def name():
    return 'UI'


def setupCLI(pa):
    pa.add_argument('-p','--port', help="Set port of UI (default is 2341)", type=int, dest='port', default=2341)
    pa.add_argument('--prefix', help="Set URL prefix", default='odgraphsearch')


def cli(args, es):
    try:
        config = utils.load_config(args.config)
    except Exception as e:
        ErrorHandler.DEBUG=True
        logging.exception("Exception during config initialisation: " + str(e))
        return

    app = Flask(__name__)

    #get the port
    port = args.port

    dbhost=config['db']['host']
    dbport=config['db']['port']

    url_prefix = args.prefix

    logging.info('Starting ODGraph UI on http://localhost:{}/'.format(port) + url_prefix + '/')

    app.config['SPARQL'] = config.get('sparql', 'http://data.wu.ac.at/odgraphsearch/query/')
    app.config['GEO_TAGGER'] = geo_tagger.GeoTagger(dbhost, dbport)
    app.config['LOCATION_SEARCH'] = search_apis.LocationSearch(dbhost, dbport)
    app.config['ELASTICSEARCH'] = es
    app.config['23TOKEN'] = config.get('twentythree', None)

    blueprint = Blueprint('api', __name__, url_prefix='/' + url_prefix + '/api/v1')
    api.init_app(blueprint)
    api.add_namespace(get_ns)
    api.add_namespace(rdf_ns)

    app.register_blueprint(blueprint)
    app.register_blueprint(ui, url_prefix='/' + url_prefix)

    app.register_blueprint(esapi, url_prefix='/'+url_prefix+'/esapi')

    app.wsgi_app = ReverseProxied(app.wsgi_app)
    tr = WSGIContainer(app)

    app.run(debug=True, port=port,host='0.0.0.0')
