from anycsv.exceptions import NoDelimiterException
from pymongo import MongoClient
from collections import defaultdict
from itertools import tee, islice, izip
import anycsv
import re
import langdetect
import pycountry
from helper.language_specifics import ADDITIONAL_STOPWORDS
from openstreetmap.osm_inserter import get_geonames_url, get_geonames_id
from osm_tagger import OSMTagger

NUTS_PATTERN = re.compile('^[A-Z]{2}\d{0,3}$')
# More general pattern than NUTS
POSTAL_PATTERN = re.compile('^(([A-Z\d]){2,4}|([A-Z]{1,2}.)?\d{2,5}(\s[A-Z]{2,5})?(.[\d]{1,4})?)$')

from nltk.corpus import stopwords
import nltk

def removeStopwords(words, language=None):
    if not language:
        l = langdetect.detect(' '.join(words))
        lang = pycountry.languages.get(alpha_2=l)
        if lang:
            language = lang.name.lower()
        else:
            language = 'german'

    for l in {language, 'english'}:
        try:
            sw = stopwords.words(l) + ADDITIONAL_STOPWORDS
            words = [word for word in words if word not in sw]
        except:
            pass
    return words


def grouper(input_list, n = 2):
    for i in xrange(len(input_list) - (n - 1)):
        yield input_list[i:i+n]


class GeoTagger:
    def __init__(self, host, port):
        client = MongoClient(host, port)
        db = client.geostore
        self.osm_tagger = OSMTagger(client)
        self.keywords = db.keywords
        self.geonames = db.geonames
        self.nuts = db.nuts
        self.postalcodes = db.postalcodes
        self.countries = db.countries

    def from_table(self, filename=None, url=None, content=None, min_matches=0.6, sample_size=300):
        if not filename and not url and not content:
            return None

        sample = []
        cols = []
        col_types = []
        num_cols = 0
        i = 0
        try:
            csvr = anycsv.reader(filename=filename, url=url, content=content)
        except NoDelimiterException:
            csvr = anycsv.reader(filename=filename, url=url, content=content, delimiter=',')

        for i, row in enumerate(csvr):
            if i <= sample_size:
                sample.append(row)
                num_cols = len(row)
            for k, c in enumerate(row):
                if len(cols) == 0:
                    cols = [[] for _ in range(num_cols)]
                    col_types = [defaultdict(int) for _ in range(num_cols)]
                if NUTS_PATTERN.match(c):
                    col_types[k]['NUTS'] += 1
                elif POSTAL_PATTERN.match(c):
                    col_types[k]['POSTAL'] += 1
                cols[k].append(c.strip())
        result = ['' for _ in range(num_cols)]
        disambiguation = [['' for _ in range(sample_size)] for _ in range(num_cols)]
        for col in range(num_cols):
            #  based on col type (90% threshold)
            if 'NUTS' in col_types[col] and col_types[col]['NUTS'] >= i * 0.9:
                disamb, confidence, res_col, source = self.nuts_column(cols[col])
            elif 'POSTAL' in col_types[col] and col_types[col]['POSTAL'] >= i * 0.9:
                disamb, confidence, res_col = self.postalcodes_column(cols[col])
            else:
                disamb, confidence, res_col, source = self.string_column(cols[col])

            if confidence > min_matches:
                disambiguation[col] = disamb
                result[col] = res_col
        return {'disambiguation': disambiguation, 'sample': sample, 'cols': num_cols, 'rows': i, 'tagging': result}


    def from_metadatada(self, data, fields=None, group_of_words=4):
        values = set()
        if fields:
            keys = fields
        else:
            keys = data.keys()
        for d in keys:
            text = data[d]
            text = text.lower()
            for c in [',', ';', '.', '_', '"', '(', ')']:
                text = text.replace(c, ' ')
            text = nltk.word_tokenize(text.lower())
            text = removeStopwords(text)
            for i in range(1, max(len(text), group_of_words)):
                for words in grouper(text, i):
                    v = ' '.join(words)
                    values.add(v)

        disamb, confidence = self.disambiguate_values(values, None)
        aggr = self.aggregated_parents(disamb)

        disamb = [x for x in disamb if x]
        return list(set(disamb) | set(aggr))


    def from_dt(self, dt, orig_country_code=None, min_matches=0.5, regions=list()):
        country_id = self.get_country_by_iso(orig_country_code)

        #cols = [[] for _ in range(dt['no_columns'])]
        col_types = [defaultdict(int) for _ in range(dt['no_columns'])]
        if not 'locations' in dt:
            dt['locations'] = []

        for i, row in enumerate(dt['row']):
            for k, c in enumerate(row['values']['exact']):
                if NUTS_PATTERN.match(c):
                    col_types[k]['NUTS'] += 1
                elif POSTAL_PATTERN.match(c):
                    col_types[k]['POSTAL'] += 1
                #cols[k].append(c.strip())

        for col in range(dt['no_columns']):
            #  based on col type (90% threshold)
            if 'NUTS' in col_types[col] and col_types[col]['NUTS'] >= i * 0.9:
                disamb, confidence, res_col, source = self.nuts_column(dt['column'][col]['values']['exact'])
            elif 'POSTAL' in col_types[col] and col_types[col]['POSTAL'] >= i * 0.9:
                disamb, confidence, res_col = self.postalcodes_column(dt['column'][col]['values']['exact'], country_id)
                source = 'geonames'
            else:
                disamb, confidence, res_col, source = self.string_column(dt['column'][col]['values']['exact'], country_id, regions=regions)

            if confidence > min_matches:
                dt['locations'] += res_col
                dt['column'][col]['entities'] = disamb
                dt['column'][col]['source'] = source
                for row, e in zip(dt['row'], disamb):
                    if not 'entities' in row:
                        row['entities'] = ['' for _ in range(dt['no_columns'])]
                    row['entities'][col] = e

        dt['locations'] = list(set(dt['locations']))
        if len(dt['locations']) == 0:
            del dt['locations']
            return False
        else:
            return True

    def postalcodes_without_country(self, values):
        refs = defaultdict(dict)
        for i, v in enumerate(values):
            val_res = self.postalcodes.find_one({'_id': v})
            if val_res:
                if 'countries' in val_res:
                    for c in val_res['countries']:
                        country = c['country']
                        reg = c.get('region')
                        if reg:
                            refs[country][i] = reg
        # else:
        #        refs['empty'][i] = ''

        max_c = 0
        selected_country = None
        for c in refs:
            if len(refs[c]) > max_c:
                max_c = len(refs[c])
                selected_country = c
        result = []
        for i in range(len(values)):
            result.append(refs[selected_country].get(i, ''))

        confidence = float(max_c) / len(values) if len(values) > 0 else 0.
        aggr = self.aggregated_parents(result)
        return result, confidence, aggr


    def postalcodes_by_country(self, values, country_id):
        match = 0.
        result = []
        for i, v in enumerate(values):
            val_res = self.postalcodes.find_one({'_id': v})
            if val_res:
                if 'countries' in val_res:
                    for c in val_res['countries']:
                        country = c['country']
                        if country_id == country:
                            reg = c.get('region')
                            if reg:
                                match += 1
                                result.append(reg)
                                continue
            result.append('')

        confidence = match / len(values) if len(values) > 0 else 0.
        aggr = self.aggregated_parents(result)
        return result, confidence, aggr


    def postalcodes_column(self, values, country_id=None):
        if country_id:
            return self.postalcodes_by_country(values, country_id)
        else:
            return self.postalcodes_without_country(values)


    def string_column(self, values, country_id=None, regions=list(), min_geonames_matches=0.4):
        disambiguated_col, confidence = self.disambiguate_values(values, country_id)
        aggr = self.aggregated_parents(disambiguated_col)
        source = 'geonames'

        # try OSM tagger if no mappings
        if confidence < min_geonames_matches:
            osm_ids, confidence = self.osm_tagger.label_values(values, regions)
            geonames_regions = set()
            disambiguated_col = []
            for r in osm_ids:
                disambiguated_col.append(r['_id'] if r else '')
                if r:
                    geonames_regions.update([get_geonames_url(x) for x in r['geonames_ids']])
            aggr = self.aggregated_parents(geonames_regions)
            source = 'osm'
        return disambiguated_col, confidence, aggr, source


    def get_numeric_parent_dict(self, values):
        context_parents = defaultdict(int)
        for v in values:
            res = self.postalcodes.find_one({'_id': v})
            if res and 'geonames' in res:
                for n in res['geonames']:
                    for p in self.get_all_parents(n):
                        context_parents[p] += 1
        return context_parents


    def nuts_column(self, values):
        match = 0.
        refs = []
        source = 'geonames'
        for v in values:
            val_res = self.nuts.find_one({'_id': v})
            if val_res:
                match += 1
                if 'geonames' in val_res:
                    id = val_res['geonames']
                #elif 'dbpedia' in val_res:
                #    id = val_res['dbpedia']
                #else:
                #    id = val_res['geovocab']
                    refs.append(id)
                else:
                    refs.append('')
            else:
                refs.append('')

        confidence = match/len(values) if len(values) > 0 else 0.
        aggr = self.aggregated_parents(refs)
        return refs, confidence, aggr, source


    def disambiguate_value(self, value, context_parents, country_id):
        v = value.strip().lower()
        val_res = self.keywords.find_one({'_id': v})
        if not val_res:
            return None

        candidate_score = defaultdict(int)
        if 'geonames' in val_res:
            for candidate in val_res['geonames']:
                geon_c = self.geonames.find_one({'_id': candidate})
                if country_id and geon_c.get('country') != country_id and 'admin_level' not in geon_c:
                    continue
                parents = self.get_all_parents(candidate, names=False, admin_level=True)
                for p in parents:
                    candidate_score[candidate] += context_parents[p] if p in context_parents else 0

        top = sorted(candidate_score.items(), key=lambda x: x[1], reverse=True)
        if len(top) > 0:
            return top[0][0]
        return None


    def disambiguate_values(self, values, country_id):
        context_parents = self.get_parent_dict(values)
        match = 0.
        # disambiguate values
        disambiguated = []
        for v in values:
            id = self.disambiguate_value(v, context_parents, country_id)
            if id:
                match += 1
                disambiguated.append(id)
            else:
                disambiguated.append('')

        confidence = match/len(values) if len(values) > 0 else 0.
        return disambiguated, confidence


    def get_country_by_iso(self, country_code):
        country_id = None
        if country_code:
            country = self.countries.find_one({'iso': country_code})
            if country:
                country_id = country['_id']
        return country_id

    def get_parent_dict(self, values):
        context_parents = defaultdict(int)
        for value in values:
            v = value.strip().lower()
            res = self.keywords.find_one({'_id': v})
            if res and 'geonames' in res:
                for n in res['geonames']:
                    parents = self.get_all_parents(n, names=False, admin_level=True)
                    for p in parents:
                        context_parents[p] += 1
        return context_parents


    def column_tagger(self, disambiguated_values, confidence=0.6):
        total = 0.
        parents = defaultdict(int)
        for v in disambiguated_values:
            if v:
                total += 1
                for p in self.get_all_parents(v, names=False):
                    parents[p] += 1

        parents = {p: parents[p]/total for p in parents}
        return [t for t in parents if parents[t] > confidence]


    def aggregated_parents(self, disambiguated_values):
        parents = set()
        for v in set(disambiguated_values):
            v_p = set(self.get_all_parents(v, names=False))
            v_p.remove(v)
            parents |= v_p
        return list(parents)

    def _get_all_parents(self, geo_id, names_list, ids_list, admin_level, all_ids=list()):
        current = self.geonames.find_one({"_id": geo_id})
        if current and "parent" in current and current['parent'] not in all_ids:
            # admin level only
            def add_to_lists():
                ids_list.append(current['parent'])
                if "name" in current:
                    names_list.append(current["name"])

            all_ids.append(current['parent'])
            if admin_level:
                if 'admin_level' in current:
                    add_to_lists()
            else:
                add_to_lists()
            # recursive call
                self._get_all_parents(current["parent"], names_list, ids_list, admin_level, all_ids)

    def get_all_parents(self, geo_id, names=True, admin_level=False):
        all_names = []
        all_ids = [geo_id]
        self._get_all_parents(geo_id, all_names, all_ids, admin_level)
        if names:
            return all_names
        else:
            return all_ids

    def get_all_subregions(self, geonames_id, iso_country_code):
        """
        Get all subregions of a level 4 geonames region
        :param geonames_id: admin level 4 geonames ID
        :param iso_country_code: iso code for a coutry
        :return: iterable of all subregions
        """
        country = self.get_country_by_iso(iso_country_code)
        q = self.geonames.find({'admin_level': 6, 'parent': geonames_id, "country" : country})

        r_tmp = [get_geonames_id(r['_id']) for r in q]
        regions = set()
        for r in r_tmp:
            regions.add(r)
            q = self.geonames.find({'admin_level': 8, 'parent': r, "country": country})
            for sub_r in q:
                regions.add(get_geonames_id(sub_r['_id']))
        return regions