import sys
import csv
import pprint
import re


POSTAL_PATTERN = re.compile('^(([A-Z\d]){2,4}|([A-Z]{1,2}.)?\d{2,5}(\s[A-Z]{2,5})?(.[\d]{1,4})?)$')


codestats = {'min_len': sys.maxint, 'max_len': 0, 'chars': set()}

with open("local/allCountries.txt") as f:
    reader = csv.reader(f, delimiter='\t')
    for i, row in enumerate(reader):
        #if i % 1000 == 0:
        #    print("parsed entries: " + str(i))

        country, c, localname, state, district, town = row[0].decode('utf-8'), row[1].decode('utf-8'), row[
            2].decode('utf-8'), row[3].decode('utf-8'), row[5].decode('utf-8'), row[7].decode('utf-8')

        if not POSTAL_PATTERN.match(c):
            print c

        if len(c) < codestats['min_len']:
            codestats['min_len'] = len(c)
        if len(c) > codestats['max_len']:
            codestats['max_len'] = len(c)
        if not c.isdigit():
            for char in c:
                if not char.isdigit():
                    codestats['chars'].add(char)


pprint.pprint(codestats)