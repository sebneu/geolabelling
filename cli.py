
from ui import search_apis
import argparse
import logging.config
import os
import time

from ui.utils import utils
from ui.utils.error_handler import ErrorHandler as eh, ErrorHandler


import sys  # import sys package, if not already imported
reload(sys)
sys.setdefaultencoding('utf-8')
from indexing import indexer
from ui import server as ui
from services import geonamesearch

submodules=[
    indexer,
    geonamesearch,
    ui
  ]


def start():
    start= time.time()
    pa = argparse.ArgumentParser(description='Open Data search and labelling toolset.', prog='odgraph')

    logg=pa.add_argument_group("Logging")
    logg.add_argument(
        '-d', '--debug',
        help="Print lots of debugging statements",
        action="store_const", dest="loglevel", const=logging.DEBUG,
        default=logging.WARNING
    )
    logg.add_argument(
        '-v', '--verbose',
        help="Be verbose",
        action="store_const", dest="loglevel", const=logging.INFO,
        default=logging.WARNING
    )

    config=pa.add_argument_group("Config")
    config.add_argument('-c','--conf', help="config file")
    
    sp = pa.add_subparsers(title='Modules', description="Available sub modules")
    for sm in submodules:
        smpa = sp.add_parser(sm.name(), help=sm.help())
        sm.setupCLI(smpa)
        smpa.set_defaults(func=sm.cli)

    m=set([])
    for k,v in sys.modules.items():
        if v is not None:
            if '.' in k:
                m.add(k.split('.')[0])
            else:
                m.add(k)

    args = pa.parse_args()

    logging.basicConfig(level=args.loglevel)

    try:
        c = utils.load_config(args.conf)
    except Exception as e:
        ErrorHandler.DEBUG = True
        logging.exception("Exception during config initialisation: " + str(e))
        return

    es = search_apis.ESClient(conf=c)

    try:
        logging.info("CMD ARGS: " + str(args))
        args.func(args , es)
    except Exception as e:
        logging.error("Uncaught exception: " + str(e))

    end = time.time()
    secs = end - start
    msecs = secs * 1000
    logging.info("END MAIN time_elapsed: " + str(msecs))

    


if __name__ == "__main__":
    start()
