import os
import subprocess
import csv


def from_csvfiles():
    dir = '/home/neumaier/Documents/odgraph_evaluation/'
    for filename in os.listdir(dir):
        arrFilename = filename.split('-')
        with open('tmp.txt', 'w') as f:
            print '<h3>' + arrFilename[1][:-4] + '</h3>'
            bashCommand = "sort -R " + os.path.join(dir,filename)
            print subprocess.call(bashCommand.split(), cwd='/home/neumaier/Repos/odgraph/ui/templates/eswc', stdout=f)
        with open('tmp2.txt', 'w') as f:
            bashCommand = "head -n 5 tmp.txt"
            print subprocess.call(bashCommand.split(), cwd='/home/neumaier/Repos/odgraph/ui/templates/eswc', stdout=f)

        #print bashCommand
        #print subprocess.Popen(bashCommand.split(), cwd='/home/neumaier/Repos/odgraph/ui/templates/eswc', shell=True)
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
                print '      <a class="header" href="eswc/' + url + '">' + title + '</a>'
                print '     <div class="description">' + url + '</div>'
                print '     </div>'
                print '   </div>'

        print ' </div>'
