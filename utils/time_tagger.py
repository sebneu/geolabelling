import subprocess
import datetime
from dateutil import parser
import lxml.etree as etree


def get_heideltime_annotations(value):

    tmp_file = 'tmp/tmp_temporalinfo.txt'
    with open(tmp_file, 'w') as f:
        f.write(value.encode('utf-8'))

    p = subprocess.Popen(['java', '-jar', 'de.unihd.dbs.heideltime.standalone.jar', '-l', 'GERMAN', '../'+tmp_file], cwd='heideltime-standalone', stdout=subprocess.PIPE)
    res, err = p.communicate()
    root = etree.fromstring(res)
    return root


def get_temporal_information(dist, dataset):
    # get temporal information
    dataset_name = dataset.get('name', '')
    dataset_description = dataset.get('description', '')
    keywords = dataset.get('keywords', [])
    dist_name = dist.get('name', '')
    dist_description = dist.get('description', '')

    # priorities to different sources of datetime information: dist > dataset info
    for value in [dist_name, dist_description, dataset_name, dataset_description, ', '.join(keywords)]:
        dates = []
        root = get_heideltime_annotations(value)
        for t in root:
            if t.attrib['type'] == 'DATE':
                v = t.attrib['value']
                date = parser.parse(v)
                dates.append(date)
        if len(dates) > 0:
            start = min(dates)
            end = max(dates)
            return start, end
    return None, None