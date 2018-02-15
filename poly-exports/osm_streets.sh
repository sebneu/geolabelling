BASE=$1
LEVEL=$2
OSMOSIS=/home/neumaier/Downloads/osmosis-latest/bin/osmosis
OSM_FILES="${BASE}/poly-exports/osm-export/*.osm.pbf"


for OSM_FILE in $OSM_FILES
do
    filename=$(basename "$OSM_FILE")
    extension="${filename##*.}"
    COUNTRY="${filename%-latest.osm.pbf}"

    POLY_FILES=${BASE}/poly-exports/${COUNTRY}/${LEVEL}/
    if [ -d "$POLY_FILES" ]
    then
        OUT_DIR=${BASE}/poly-exports/osm-export/${COUNTRY}/${LEVEL}/
        mkdir -p OUT_DIR

        F_COUNT=$(ls -1q $POLY_FILES | wc -l)
        BATCH=100

        echo "TOTAL FILES FOR COUNTRY " $COUNTRY " " $F_COUNT

        if [ $F_COUNT -le $BATCH ]
        then
            BATCH=$F_COUNT
        fi
        x=$((F_COUNT/BATCH))

        for ((i=0; i<=x; i++))
        do
            SKIP=$(($BATCH*$i))
            echo "SKIP " $SKIP
            echo "BATCH " $BATCH
            BATCH_COUNT=$(ls -U $POLY_FILES | tail -n +$SKIP | head -$BATCH | wc -l)
            echo "BATCH COUNT " $BATCH_COUNT


            CMD="$OSMOSIS --read-pbf-fast workers=4 file=\"$OSM_FILE\" --way-key keyList=highway,amenity --used-node --tee $BATCH_COUNT "

            for filename in $(ls -U $POLY_FILES | tail -n +$SKIP | head -$BATCH)
            do
                CMD=$CMD"--bounding-polygon file=$POLY_FILES$filename --write-xml $OUT_DIR$filename.osm "
            done
            echo $CMD
            eval $CMD
        done
    fi
done