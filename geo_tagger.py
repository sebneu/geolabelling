

from pymongo import MongoClient
from collections import defaultdict
import anycsv
import re

NUTS_PATTERN = re.compile('^[A-Z]{2}\d{0,3}$')
# More general pattern than NUTS
POSTAL_PATTERN = re.compile('^(([A-Z\d]){2,4}|([A-Z]{1,2}.)?\d{2,5}(\s[A-Z]{2,5})?(.[\d]{1,4})?)$')



class GeoTagger:
    def __init__(self, host, port):
        client = MongoClient(host, port)
        db = client.geostore
        self.keywords = db.keywords
        self.geonames = db.geonames
        self.nuts = db.nuts
        self.postalcodes = db.postalcodes

    def from_table(self, filename=None, url=None, min_matches=0.8, sample_size=100):
        if not filename and not url:
            return None

        sample = []
        cols = []
        col_types = []
        num_cols = 0
        i = 0
        for i, row in enumerate(anycsv.reader(filename=filename, url=url)):
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
                disamb, confidence, res_col = self.nuts_column(cols[col])
            elif 'POSTAL' in col_types[col] and col_types[col]['POSTAL'] >= i * 0.9:
                disamb, confidence, res_col = self.string_column(cols[col], 'postalcodes')
            else:
                disamb, confidence, res_col = self.string_column(cols[col])

            if confidence > min_matches:
                disambiguation[col] = disamb
                result[col] = res_col
        return {'disambiguation': disambiguation, 'sample': sample, 'cols': num_cols, 'rows': i, 'tagging': result}

    def string_column(self, values, collection='keywords'):
        disambiguated_col, confidence = self.disambiguate_values(values, collection)
        aggr = self.column_tagger(disambiguated_col)
        return disambiguated_col, confidence, aggr


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
        for v in values:
            val_res = self.nuts.find_one({'_id': v})
            if val_res:
                match += 1
                if 'geonames' in val_res:
                    id = val_res['geonames']
                elif 'dbpedia' in val_res:
                    id = val_res['dbpedia']
                else:
                    id = val_res['geovocab']
                refs.append(id)
            else:
                refs.append('')

        confidence = match/len(values) if len(values) > 0 else 0.
        return refs, confidence, None


    def disambiguate_value(self, value, context_parents, collection):
        v = value.strip().lower()
        val_res = self.__dict__[collection].find_one({'_id': v})
        if not val_res:
            return None

        candidate_score = defaultdict(int)
        if 'geonames' in val_res:
            for candidate in val_res['geonames']:
                for p in self.get_all_parents(candidate):
                    candidate_score[candidate] += context_parents[p] if p in context_parents else 0

        top = sorted(candidate_score.items(), key=lambda x: x[1], reverse=True)
        if len(top) > 0:
            return top[0][0]
        return None


    def disambiguate_values(self, values, collection):
        context_parents = self.get_parent_dict(values, collection)
        match = 0.
        # disambiguate values
        disambiguated = []
        for v in values:
            id = self.disambiguate_value(v, context_parents, collection)
            if id:
                match += 1
                disambiguated.append(id)
            else:
                disambiguated.append('')

        confidence = match/len(values) if len(values) > 0 else 0.
        return disambiguated, confidence


    def get_parent_dict(self, values, collection):
        context_parents = defaultdict(int)
        for value in values:
            v = value.strip().lower()
            res = self.__dict__[collection].find_one({'_id': v})
            if res and 'geonames' in res:
                for n in res['geonames']:
                    for p in self.get_all_parents(n):
                        context_parents[p] += 1
        return context_parents


    def column_tagger(self, disambiguated_values):
        total = 0.
        parents = defaultdict(int)
        for v in disambiguated_values:
            if v:
                total += 1
                for p in self.get_all_parents(v):
                    parents[p] += 1

        parents = {p: parents[p]/total for p in parents}
        top = sorted(parents.items(), key=lambda x: x[1], reverse=True)
        return [t[0] + ' (' + str(t[1]) + ')' for t in top if t[1] > 0.5][:3]



    def _get_all_parents(self, geo_id, all_names):
        current = self.geonames.find_one({"_id": geo_id})
        if current and "parent" in current:
            if "name" in current:
                all_names.append(current["name"])
            self._get_all_parents(current["parent"], all_names)


    def get_all_parents(self, geo_id):
        names = []
        self._get_all_parents(geo_id, names)
        return names