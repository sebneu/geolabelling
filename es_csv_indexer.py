import requests
import logging

def get_content(csv_url):
    # timeout of two seconds
    resp = requests.get(csv_url, timeout=2)
    data = resp.content
    return data


def index_csv(portal_id, snapshot):
    api_url = 'http://data.wu.ac.at/portalwatch/api/v1/portal/' + portal_id + '/' + str(snapshot) + '/resources?format=csv'
    resp = requests.get(api_url)
    if resp.status_code == 200:
        csv_urls = resp.json()

        for csv_url in csv_urls:
            try:
                data = get_content(csv_url)

            except Exception as e:
                logging.debug('Error while loading CSV: ' + csv_url)



if __name__ == '__main__':
    index_csv('data_gv_at', 1724)