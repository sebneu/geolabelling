import os
import subprocess
import csv
import json

def from_json(file):
    with open(file) as f:
        data = json.load(f)

    with open('out.txt', 'w') as f:
        for d in data['hits']['hits']:
            url = d['_id']
            publisher = d['_source']['dataset']['publisher'].encode('utf-8')
            title = d['_source']['dataset']['dataset_name'].encode('utf-8')
            dataset_link = d['_source']['dataset']['dataset_link']
            portal = d['_source']['portal']['uri']

            t = """
            <div class="item">
              <div class="ui grid">
                <div class="twelve wide column">
                  <a class="header" href="semantics/{0}">{1}</a>
                  <div class="description">{0}</div>
                </div>
                <div class="right aligned four wide column">
                  <div class="header">{2}</div>
                  <div class="description"><a href="{3}">{4}</a></div>
                </div>
              </div>
            </div>
            """.format(url, title, publisher, dataset_link, portal)
            f.write(t)

def from_csvfiles():
    dir = '/home/neumaier/Documents/odgraph_evaluation/'
    for filename in os.listdir(dir):
        arrFilename = filename.split('-')
        with open('tmp.txt', 'w') as f:
            print '<h3>' + arrFilename[1][:-4] + '</h3>'
            bashCommand = "sort -R " + os.path.join(dir,filename)
            print subprocess.call(bashCommand.split(), cwd='/home/neumaier/Repos/odgraph/ui/templates/semantics', stdout=f)
        with open('tmp2.txt', 'w') as f:
            bashCommand = "head -n 5 tmp.txt"
            print subprocess.call(bashCommand.split(), cwd='/home/neumaier/Repos/odgraph/ui/templates/semantics', stdout=f)

        #print bashCommand
        #print subprocess.Popen(bashCommand.split(), cwd='/home/neumaier/Repos/odgraph/ui/templates/semantics', shell=True)
        #ps = subprocess.Popen(bashCommand.split(), shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        #output = ps.communicate()[0]
        #print output

        print ' <div class="ui relaxed divided list">'
        with open('tmp2.txt') as f:
            csvr = csv.reader(f)
            for line in csvr:
                title = line[0]
                url = line[1]
                print '  <div class="item">'
                print '  <i class="large file middle aligned icon"></i>'
                print '    <div class="content">'
                print '      <a class="header" href="semantics/' + url + '">' + title + '</a>'
                print '     <div class="description">' + url + '</div>'
                print '     </div>'
                print '   </div>'

        print ' </div>'


if __name__ == '__main__':
    from_json('/home/neumaier/Repos/odgraph/local/sampledata/nocolumns_nometadata.json')