OSMOSIS=/home/neumaier/Downloads/osmosis-latest/bin/osmosis
POLY_FILES=/home/neumaier/Repos/odgraph/poly-exports/at/6/
OUT_DIR=/home/neumaier/Repos/odgraph/poly-exports/osm-export/6/

F_COUNT=$(ls -1q $POLY_FILES | wc -l)
BATCH=93

echo $F_COUNT

CMD="$OSMOSIS --read-pbf-fast workers=4 file=\"austria-latest.osm.pbf\" --way-key keyList=highway,amenity --used-node --tee $BATCH "

SKIP=0
echo $SKIP
for filename in $(ls -U $POLY_FILES | tail -n +$SKIP | head -$BATCH)
do
        CMD=$CMD"--bounding-polygon file=$POLY_FILES$filename --write-xml $OUT_DIR$filename.osm "
done
echo $CMD
eval $CMD

