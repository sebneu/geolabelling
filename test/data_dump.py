
import requests
import rdflib


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
