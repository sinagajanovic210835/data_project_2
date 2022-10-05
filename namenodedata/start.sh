#! /bin/bash

PERIOD=$1
COUNTER=$2
DIR=$3
hdfs dfs -mkdir /user/test/invoices
hdfs dfs -mkdir /user/test/invoices/$DIR
while [ true ] 
do
    EXPR=$COUNTER'__..'
    NAME=`ls /data1/invoices/ | grep ^"$EXPR"`
    FILE=/data1/invoices/$NAME
    URI=/user/test/invoices/$DIR/  
    hdfs dfs -appendToFile $FILE $URI$DIR && \
    sleep $PERIOD
    echo $FILE $URI$NAME
    if [ `expr $COUNTER % 4` -eq 0 ]
    then
        DIR=$((COUNTER / 4  + 1))
        hdfs dfs -mkdir /user/test/invoices/$DIR
    fi    
    COUNTER=$((COUNTER + 1))    
done
