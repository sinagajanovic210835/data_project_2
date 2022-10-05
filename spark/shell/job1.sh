! /bin/bash

NMB=`cat ./nmb.txt`
spark/bin/spark-shell -I /sparkapp/first.scala --conf spark.driver.args="$NMB" & NMB=$((NMB + 1)) && \
echo $NMB > /nmb.txt
