from collections import defaultdict
from postal.parser import parse_address


ROAD = ['road']
PLACE = ['city', 'city_district', 'suburb', 'island', 'state_district', 'state', 'state_district', 'country',
         'country_region', 'world_region']


class OSMTagger:
    def __init__(self, client):
        db = client.geostore
        self.osm = db.osm
        self.osm_names = db.osm_names
        self.geonames = db.geonames

    def label_values(self, values, context_columns, regions):
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
        labelled_roads = self.find_osm_names(roads, context, regions)

        match = 0.
        labelled_v = ['' for _ in range(len(values))]
        for r in labelled_roads:
            for i in roads[r]:
                l = labelled_roads[r]
                labelled_v[i] = l
                match += 1

        confidence = match/val_count if val_count > 0 else 0.
        return labelled_v, confidence


    def find_osm_names(self, values, context, regions):
        # filter only relevant osm names within given regions
        candidates = {}

        for v in values:
            n = self.osm_names.find_one({'_id': v})
            if n:
                candidates[v] = [self.osm.find_one({'_id': c}) for c in n['osm_id']]
                tmp = context[v] if context[v] else regions
                if tmp:
                    candidates[v][:] = [c for c in candidates[v] if set(c['geonames_ids']) & set(tmp)]

        context_count = self.get_context_count(candidates)

        for c in candidates:
            if not candidates[c]:
                candidates[c] = None
            elif len(candidates[c]) == 1:
                candidates[c] = candidates[c][0]
            elif len(candidates[c]) > 1:
                c_count = []
                for tmp in candidates[c]:
                    c_count.append(max([context_count[geo_id] for geo_id in tmp['geonames_ids']]))
                max_index = c_count.index(max(c_count))
                candidates[c] = candidates[c][max_index]
        return candidates


    def get_context_count(self, candidates):
        # TODO if more than one candidate: try to disambiguate based on others
        context_count = defaultdict(int)
        for c in candidates:
            geo_ids = []
            for tmp in candidates[c]:
                geo_ids += tmp['geonames_ids']
                for geo_id in geo_ids:
                    #parents = get_all_parents(self.geonames, get_geonames_url(geo_id), names=False, admin_level=True)
                    #for p in parents:
                        context_count[geo_id] += 1
        return context_count


