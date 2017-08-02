import logging
import time

import requests

from ui import search_apis


def iter_datasets(p, sn, es):
    api_url = 'http://adequate-project.semantic-web.at/portalmonitor/api/portal/'+ p + '/' + str(sn)+ '/datasets'
    resp = requests.get(api_url)
    if resp.status_code == 200:
        datasets = resp.json()

        for d in datasets:
            d_id = d['id']
            try:
                d_url = 'http://adequate-project.semantic-web.at/portalmonitor/api/portal/'+ p + '/' + str(sn)+ '/dataset/' + d_id + '/schemadotorg'
                resp = requests.get(d_url)

                if resp.status_code == 200:
                    dataset = resp.json()
                    dataset_name = dataset.get('name', '')
                    dataset_link = dataset.get('@id', '')
                    dataset_description = dataset.get('description', '')
                    publisher = dataset.get('publisher', {}).get('name', '')

                    for dist in dataset.get('distribution', []):
                        if 'contentUrl' in dist:
                            url = dist['contentUrl']

                            try:
                                if es.exists(url):
                                    doc = es.get(url, columns=False, rows=False)
                                    if 'dataset' not in doc['_source']:
                                        name = dist.get('name', '')
                                        fields = {}
                                        if name:
                                            fields['name'] = name
                                        if dataset_name:
                                            fields['dataset_name'] = dataset_name
                                        if dataset_link:
                                            fields['dataset_link'] = dataset_link
                                        if dataset_description:
                                            fields['dataset_description'] = dataset_description
                                        if publisher:
                                            fields['publisher'] = publisher
                                        res = es.update(url, {'dataset': fields})
                                        logging.info(res)
                                        time.sleep(1)
                            except Exception as e:
                                logging.error('Elasticsearch response: ' + str(e))
                                logging.error(e)
            except Exception as e:
                logging.error('Error while retrieving all datasets: ' + d_id)
                logging.error(e)


if __name__ == '__main__':
    p = "www_opendataportal_at"
    sn = 1730
    logging.basicConfig(level=logging.INFO)

    es = search_apis.ESClient()
    iter_datasets(p, sn, es)