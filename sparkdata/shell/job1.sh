#! /bin/bash

source /etc/environment && \
BOL=`cat /bool/bool.txt`
if [[ $CNT != "" ]]
then
    sleep 20
    /spark/bin/spark-shell -I /sparkapp/first.scala --conf spark.driver.args="$CNT"
else
    CNT=0   
fi
CNT=$((CNT + 1))
echo CNT=$CNT > /etc/environment
