#! /bin/bash

CDATE=xxxxxxxxx
SEC=`date +%s`
while [ `expr $SEC % 60` -ne 0 ]
do
    SEC=`date +%s`
done
PERIOD=$1
PERIOD=$((PERIOD * 60))
COUNTER=1
DIR=1
hdfs dfs -mkdir /user/test/invoices
hdfs dfs -mkdir /user/test/invoices/1
while [ $COUNTER -lt 2915 ]
do
    EXPR=$COUNTER'__..'
    NAME=`ls /data1/invoices/ | grep ^"$EXPR"`
    DATE=${NAME:(-14)} && DATE=${DATE#'_'} && DATE=${DATE#'_'}
    if [[ $CDATE != $DATE ]]
    then        
        PRODUCT=`ls /data1/products/ | grep ^"$CDATE"`
        COUNTRY=`ls /data1/countries/ | grep ^"$CDATE"`
        if [[ $PRODUCT != "" ]]
        then
            hdfs dfs -mkdir /user/test/products && \
            hdfs dfs -appendToFile /data1/products/$PRODUCT /user/test/products/products.csv & \
            echo true > /bool/bool.txt
        fi
        if [[ $COUNTRY != "" ]]
        then
            hdfs dfs -mkdir /user/test/countries && \
            hdfs dfs -appendToFile /data1/countries/$COUNTRY /user/test/countries/countries.csv & \
            echo true > /bool/bool.txt
        fi
    fi 
    BOOL=`cat /bool/bool.txt`
    if [[ $BOOL == "false" ]]
    then
        hdfs dfs -rm -r /user/test/products & \
        hdfs dfs -rm -r /user/test/countries
    fi
    FILE=/data1/invoices/$NAME
    URI=/user/test/invoices/$DIR/  
    hdfs dfs -appendToFile $FILE $URI$DIR & \
    if [ `expr $COUNTER % 4` -eq 0 ]
    then
        DIR=$((COUNTER / 4  + 1))
        hdfs dfs -mkdir /user/test/invoices/$DIR
    fi    
    COUNTER=$((COUNTER + 1))
    CDATE=$DATE
    sleep $PERIOD
done
