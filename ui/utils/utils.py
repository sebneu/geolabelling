import os
import yaml
import structlog
log =structlog.get_logger()


def assure_path_exists(path):
    
    d = os.path.abspath(path)
    if not os.path.exists(d):
        log.info("Creating directory", path=d)
        os.makedirs(d)
    else:
        log.info("Directory exists", path=d)
    return d

def default_conf():
    config={}
    confFile = os.path.join('resources', 'base.yaml')
    log.info("Loading default conf", config=confFile)
    with open(confFile) as f_conf:
            conf = yaml.load(f_conf)
            for key, values in conf.items():
                config[key]={}
                for k, v in values.items():
                    config[key][k]=v
    
    return config

def load_config(confFile):
    log.info("Loading user config", config=confFile)
    with open(confFile) as f_conf:
        conf = yaml.load(f_conf)
    return conf