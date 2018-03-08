import logging
import time

import requests

from ui import search_apis
from utils import time_tagger


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
                                    dsfields = {}
                                    fields = {'dataset': dsfields}
                                    start, end = time_tagger.get_temporal_information(dist, dataset)
                                    if start and end:
                                        fields['metadata_temp_start'] = start
                                        fields['metadata_temp_end'] = end

                                    #doc = es.get(url, columns=False, rows=False)
                                    #if 'dataset' not in doc['_source']:
                                    name = dist.get('name', '')
                                    if name:
                                        dsfields['name'] = name
                                    if dataset_name:
                                        dsfields['dataset_name'] = dataset_name
                                    if dataset_link:
                                        dsfields['dataset_link'] = dataset_link
                                    if dataset_description:
                                        dsfields['dataset_description'] = dataset_description
                                    if publisher:
                                        dsfields['publisher'] = publisher
                                    res = es.update(url, fields)
                                    logging.info(res)
                                    time.sleep(3)
                            except Exception as e:
                                logging.error('Elasticsearch response: ' + str(e))
                                logging.error(e)
            except Exception as e:
                logging.error('Error while retrieving all datasets: ' + d_id)
                logging.error(e)


if __name__ == '__main__':
    p = "data_gv_at"
    sn = 1730
    logging.basicConfig(level=logging.INFO)

    es = search_apis.ESClient()
    iter_datasets(p, sn, es)