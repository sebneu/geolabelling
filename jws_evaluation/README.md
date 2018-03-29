To show the performance and limitations of our geo-labelling approach we randomly selected ten datasets per data portal using the ElasticSearch's built-in [random function](https://www.elastic.co/guide/en/elasticsearch/guide/current/random-scoring.html).
The files are retrieved and stored using this short [Python script](generate_random_samples.py). It accesses an API which we implemented for this evaluation. The list of all files, their URLs, and respective portal is in [index.csv](index.csv).

We manually assessed the datasets' labels by assigning the one (or more) of the following tags in [index.csv](index.csv) (column "error classes"):

| Error class                 |  index.csv  | description |
| -------------               |-----  |-----|
| Incorrect GeoNames label(s) | `g`   |  Some of the individual GeoNames column labels are incorrect.  |
| Incorrect OSM label(s)      | `o`   |  Some of the individual OpenStreetMap (OSM) column labels are incorrect.  |
| Not assigned                | `m`   |  There are potentially geo-references in the dataset but the respective labels are not in the knowledge graph or the algorithm could not detect the entities.  |
