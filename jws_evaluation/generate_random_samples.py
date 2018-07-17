import requests
import os
import csv

if __name__ == '__main__':
    api_url = 'http://data.wu.ac.at/odgraph/api/v1'
    portals = ["data_gov_gr", "datos_gob_es", "www_data_gouv_fr", "data_gv_at", "offenedaten_de", "data_overheid_nl", "data_gov_ie", "opingogn_is", "data_gov_sk", "govdata_de", "data_gov_uk"]

    no_urls = 10
    no_rows = 10

    context = [['url', 'filename', 'portal', 'inspected', 'error classes']]

    for p in portals:
        resp = requests.get(api_url + '/random/urls?portal={0}&columnlabels=true&urls={1}'.format(p, str(no_urls)))
        if not os.path.exists(p):
            os.mkdir(p)

        for i, url in enumerate(resp.content.splitlines()):
            csv_file = requests.get(api_url + '/random/dataset?url={0}&rows={1}'.format(url, no_rows))
            if csv_file.status_code == 200:
                filename = p + '/' + str(i) + '.csv'
                with open(filename, 'w') as f:
                    f.write(csv_file.content)
                context.append([url, filename, p, '', ''])

    with open('index.csv', 'w') as f:
        csvw = csv.writer(f)
        csvw.writerows(context)