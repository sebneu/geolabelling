
import requests
import rdflib


def filter_geonames():
    g = rdflib.Graph()
    g.parse('odgraph.nt', format='nt')

    geonames = 0
    other = 0
    osm = 0
    outg = rdflib.Graph()
    for s, p, o in g:
        if str(s).startswith('http://sws.geonames.org/'):
            outg.add((s, p, o))
            geonames += 1
        elif str(s).startswith('http://www.openstreetmap.org/'):
            osm += 1
        else:
            other += 1

    with open('odgraph1.nt', 'w') as f:
        outg.serialize(f, 'nt')

    print 'counts'
    print geonames
    print osm
    print other

def download_dump():
    resp = requests.get('http://data.wu.ac.at/odgraph/api/v1/get/urls?columnlabels=geonames')

    if resp.status_code == 200:
        data = resp.json()

        g = rdflib.Graph()
        print 'total urls:', len(data)
        for i, url in enumerate(data):
            try:
                resp2 = requests.get('http://data.wu.ac.at/odgraph/api/v1/rdf/labels?url=' + url)
                d = resp2.content
                g.parse(data=d, format="nt")

                if i% 100 == 0:
                    print i
            except Exception as e:
                print e
                print d

        with open('odgraph.nt', 'w') as f:
            g.serialize(f, 'nt')
