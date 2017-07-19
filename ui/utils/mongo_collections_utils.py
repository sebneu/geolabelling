def get_nuts_link(res):
    if 'geonames' in res:
        l = res['geonames']
    elif 'dbpedia' in res:
        l = res['dbpedia']
    else:
        l = res['geovocab']
    return l