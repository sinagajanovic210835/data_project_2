#! /bin/bash

PERIOD=$1
CDATE=xxxxxxxxx
COUNTER=`cat /bool/cnt.txt`
DIR=1
hdfs dfs -mkdir /user/test/invoices
hdfs dfs -mkdir /user/test/invoices/1
TEST=$((PERIOD * 60))

while [ $COUNTER -lt 2915 ]
do  
    SEC=`date +%s`
    MOD=$((SEC % TEST))
    while [[ $MOD != 0 ]]
    do
        SEC=`date +%s`
        MOD=$((SEC % TEST))
        sleep 1
    done

    BOOL=`cat /bool/bool.txt`
    MSG=
    if [[ $BOOL == "delete" ]]
    then
        hdfs dfs -rm -r /user/test/products
        hdfs dfs -rm -r /user/test/countries
        echo "" > /bool/bool.txt
    fi
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
            hdfs dfs -appendToFile /data1/products/$PRODUCT /user/test/products/products.csv
            MSG+=pro
        fi
        if [[ $COUNTRY != "" ]]
        then
            hdfs dfs -mkdir /user/test/countries && \
            hdfs dfs -appendToFile /data1/countries/$COUNTRY /user/test/countries/countries.csv
            MSG+=cnt
        fi
    fi      
    FILE=/data1/invoices/$NAME
    URI=/user/test/invoices/$DIR/  
    hdfs dfs -appendToFile $FILE $URI$DIR
    if [ `expr $COUNTER % 4` -eq 0 ]
    then
        OLDDIR=$((COUNTER / 4))
        DIR=$((COUNTER / 4  + 1))
        hdfs dfs -mkdir /user/test/invoices/$DIR
        MSG+=inv_
        MSG+=$OLDDIR
    fi    
    COUNTER=$((COUNTER + 1))
    CDATE=$DATE
    echo $COUNTER > /bool/cnt.txt
    echo $MSG > /bool/bool.txt
done
