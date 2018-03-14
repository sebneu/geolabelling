from collections import defaultdict
from postal.parser import parse_address

from openstreetmap.osm_inserter import get_geonames_url

ROAD = ['road']
PLACE = ['city', 'city_district', 'suburb', 'island', 'state_district', 'state', 'state_district', 'country',
         'country_region', 'world_region']


class OSMTagger:
    def __init__(self, client):
        db = client.geostore
        self.osm = db.osm
        self.osm_names = db.osm_names
        self.geonames = db.geonames

    def label_values(self, values, context_columns, regions, geotagger):
        """
        The osm names require a set of potential admin_level 8 regions.
        These could be gathered from other columns, metadata,
        or other parts of the strings within the same column.
        :param values: set of string values, e.g., from a CSV column
        :param regions: set of geonames IDs (not URLs). Only osm names within these regions will be considered
        :return:
        """
        places = defaultdict(list)
        roads = defaultdict(list)
        context = defaultdict(list)
        val_count = 0.

        for i, value in enumerate(values):
            if value.strip():
                val_count += 1
                addr = parse_address(value)

                for parsed in addr:
                    if parsed[1] in ROAD:
                        roads[parsed[0]].append(i)
                        for c in context_columns:
                            if c[i]:
                                context[parsed[0]].append(c[i])
                    if parsed[1] in PLACE:
                        places[parsed[0]].append(i)

        # TODO: min number of potential roads in column
        labelled_roads = self.find_osm_names(roads, context, regions, geotagger)

        match = 0.
        labelled_v = ['' for _ in range(len(values))]
        for r in labelled_roads:
            for i in roads[r]:
                l = labelled_roads[r]
                labelled_v[i] = l
                match += 1

        confidence = match/val_count if val_count > 0 else 0.
        return labelled_v, confidence


    def find_osm_names(self, values, context, regions, geotagger):
        # filter only relevant osm names within given regions
        candidates = {}

        for v in values:
            n = self.osm_names.find_one({'_id': v})
            if n:
                candidates[v] = [self.osm.find_one({'_id': c}) for c in n['osm_id']]
                tmp = context[v] if context[v] else regions
                if tmp:
                    context_regions = [geotagger.get_parent(cr)['_id'] for cr in tmp] + tmp
                    new_candidate_set = []
                    for c in candidates[v]:
                        for x in c['geonames_ids']:
                            geon = get_geonames_url(x)

                            for y in geotagger.get_all_parents(geon, names=False, admin_level=False):
                                if y in context_regions:
                                    new_candidate_set.append(c)
                                    break
                    candidates[v] = list(new_candidate_set)
                    #candidates[v][:] = [c for c in candidates[v] if set(c['geonames_ids']) & set(tmp)]

        context_count = self.get_context_count(candidates)

        for k, v in candidates.items():
            if not v:
                del candidates[k]
            elif len(v) == 1:
                candidates[k] = v[0]
            elif len(v) > 1:
                c_count = []
                for tmp in v:
                    c_count.append(sum(context_count[geo_id] for geo_id in tmp['geonames_ids']))
                max_index = c_count.index(max(c_count))
                candidates[k] = v[max_index]
        return candidates


    def get_context_count(self, candidates):
        # TODO if more than one candidate: try to disambiguate based on others
        context_count = defaultdict(int)
        for street in candidates:
            for c in candidates[street]:
                for geo_id in c['geonames_ids']:
                    context_count[geo_id] += 1
        return context_count


