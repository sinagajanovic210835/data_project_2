#! /bin/bash

PERIOD=$1
PERIOD=$((PERIOD * 60))
BOL=`cat /bool/bool.txt`
COUNTER=1
DIR=1
hdfs dfs -mkdir /user/test/invoices
hdfs dfs -mkdir /user/test/invoices/1
while [ true ] 
do
    EXPR=$COUNTER'__..'
    NAME=`ls /data1/invoices/ | grep ^"$EXPR"`
    DATE=${NAME:(-14)} && DATE=${DATE#'_'} && DATE=${DATE#'_'}






    FILE=/data1/invoices/$NAME
    URI=/user/test/invoices/$DIR/  
    hdfs dfs -appendToFile $FILE $URI$DIR && \   
    if [ `expr $COUNTER % 4` -eq 0 ]
    then
        DIR=$((COUNTER / 4  + 1))
        hdfs dfs -mkdir /user/test/invoices/$DIR
    fi    
    COUNTER=$((COUNTER + 1))
    CDATE=$DATE
    sleep $PERIOD
done
