# geolabelling
Geo-semantic labelling of Open Data


## Setup

### GeoNames RDF dump
Download the [GeoNames RDF dump](http://download.geonames.org/all-geonames-rdf.zip) and extract it to the "local" folder.

### MongoDB setup
The GeoNames entities, labels, and additional/external links are all stored in a [MongoDB instance](https://www.mongodb.com/). All commands provide --host and --port parameters to access the MongoDB

### GeoNames data

1. Store all GeoNames entities and their parent relations in collection db.geonames: 
```
$ python geonames_graph.py --host localhost --port 27017 geonames
```
2. Build an index for all labels (and alternative labels) in collection db.keywords:
```
$ python geonames_graph.py --host localhost --port 27017 keywords
```
3. Store country information in db.countries
```
$ python geonames_graph.py --host localhost --port 27017 countries
```

### Wikidata

- Get postal codes from wikidata and store in collection db.postalcodes
```
$ python geonames_graph.py --host localhost --port 27017 wikidata-postalcodes
```
- Get NUTS from wikidata and store in collection db.nuts
```
$ python geonames_graph.py --host localhost --port 27017 wikidata-nuts
```

### OSM data

#### Add admin levels to hierarchy
Adds the admin levels 2, 4 and 6 (according to the [OSM hierarchy](https://wiki.openstreetmap.org/wiki/Tag:boundary=administrative#10_admin_level_values_for_specific_countries)) to the DB. Uses the GeoNames API.
```
$ python geonames_graph.py divisions --country http://sws.geonames.org/3057568/
```

Adds the admin level 8 (districts, city divisions) to the DB
```
$ python geonames_graph.py city-divisions --country http://sws.geonames.org/3057568/
```

#### Get polygons for admin levels from OSM API
```
$ python openstreetmap/osm_inserter.py osm-polygons --level 8 --country http://sws.geonames.org/3057568/
```

#### Export polygons to local directory, e.g., "poly-exports/slovakia/8"
```
$ python openstreetmap/osm_inserter.py poly-export --level 8 --country http://sws.geonames.org/3057568/ --directory poly-exports
```

#### Extract OSM data
First, download the OSM data for countries (e.g. from [geofabrik.de](https://download.geofabrik.de/europe.html)) and place it in the "poly-exports/osm-export/" subdirectory. The script uses the polygons (e.g. poly-exports/slovakia/8) to extract streets and places for the subregions from the download, using [Osmosis](https://wiki.openstreetmap.org/wiki/Osmosis).
The script takes as arguments the path to the repository and the admin level to extract the data. Also the path to Osmosis has to be modified in this script.
```
$ sh openstreetmap/osm_streets.sh /path/to/repo 8
```

#### Insert OSM data in DB
Insert the data from the OSM extracts into the DB
```
$ python openstreetmap/osm_inserter.py insert-osm --country http://sws.geonames.org/3057568/ --level 8
```
