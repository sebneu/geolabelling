

OSMOSIS=/home/neumaier/Downloads/osmosis-latest/bin/osmosis
POLY_FILES=/home/neumaier/Repos/odgraph/poly-exports/de/6/
OUT_DIR=/home/neumaier/Repos/odgraph/poly-exports/osm-export/de/6/
OSM_FILE=germany-latest.osm.pbf

F_COUNT=$(ls -1q $POLY_FILES | wc -l)
BATCH=2

echo $F_COUNT

CMD="$OSMOSIS --read-pbf-fast workers=4 file=\"$OSM_FILE\" --way-key keyList=highway,amenity --used-node --tee $BATCH "

SKIP=130
echo $SKIP
for filename in $(ls -U $POLY_FILES | tail -n +$SKIP | head -$BATCH)
do
        CMD=$CMD"--bounding-polygon file=$POLY_FILES$filename --write-xml $OUT_DIR$filename.osm "
done
echo $CMD
eval $CMD

