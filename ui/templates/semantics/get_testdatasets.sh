#!/usr/bin/env bash

FILES=/home/neumaier/Documents/odgraph_evaluation/*
for f in $FILES
do
    IFS='-'; arrFilename=($f); unset IFS;
    echo '<h3>'${arrFilename[1]}'</h3>'

    sort -R $f | head -n 5 > tmp.txt

    echo ' <div class="ui relaxed divided list">'

    cat tmp.txt | while read line
    do

        IFS=','; arrLine=($line); unset IFS;
        title=${arrLine[0]}
        url=${arrLine[1]}
      echo '  <div class="item">'
      echo '  <i class="large file middle aligned icon"></i>'
      echo '    <div class="content">'
      echo '      <a class="header" href="semantics/'${url//\"}'">'${title//\"}'</a>'
      echo '     <div class="description">'${url//\"}'</div>'
      echo '     </div>'
      echo '   </div>'

    done

     echo ' </div>'
done