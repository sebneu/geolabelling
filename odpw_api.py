import requests
import json

#p_id = 'www_opendataportal_at'
p_id = 'data_gv_at'

# get all datasets
r = requests.get('http://data.wu.ac.at/portalwatch/api/v1/portal/' + p_id + '/1723/datasets')
datasets_list = r.json()
print p_id, 'datasets:', len(datasets_list)

datasets = 0
resources = 0

format_m = 0
size_m = 0

contactemail_m = 0

temp_m = 0
spatial_m = 0

for d in datasets_list:
    d_id = d['id']
    # get dataset as dcat
    r2 = requests.get('http://data.wu.ac.at/portalwatch/api/v1/memento/' + p_id + '/' + d_id)
    data = r2.json()
    data = data['raw']

    datasets += 1

    if 'maintainer_email' in data and data['maintainer_email'] != None and len(unicode(data['maintainer_email']).strip()) > 0:
        pass
    elif 'author_email' in data and data['author_email'] != None and len(unicode(data['author_email']).strip()) > 0:
        pass
    else:
        contactemail_m += 1


    for r in data['resources']:
        resources += 1
        if 'format' in r and r['format'] != None and len(unicode(r['format']).strip()) > 0:
            pass
        else:
            format_m += 1

        if 'size' in r and r['size'] != None and len(unicode(r['size']).strip()) > 0:
            pass
        else:
            size_m += 1

print 'datasets:', datasets
print 'resources:', resources
print 'missing formats:', format_m
print 'missing size:', size_m
print 'missing contact', contactemail_m