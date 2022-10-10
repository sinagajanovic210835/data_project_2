#! /bin/bash

source /etc/environment && \

if [[ $CNT != "" ]]
then
    sleep 20    
    BOOL=`cat /bool/bool.txt`
    /spark/bin/spark-shell -I /sparkapp/first.scala --conf spark.driver.args="$CNT $BOOL" && \
    echo false > /bool/bool.txt
    CNT=$((CNT + 1))        
else
    CNT=1  
fi
echo CNT=$CNT > /etc/environment
