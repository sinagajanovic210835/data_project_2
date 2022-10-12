#! /bin/bash
sleep 20
BOOL=`cat /bool/bool.txt`
echo $BOOL
if [[ $BOOL != "" ]]
then     
    /spark/bin/spark-shell -I /sparkapp/SaveHiveTables.scala --conf spark.driver.args="$BOOL" & \
    echo delete > /bool/bool.txt
fi  

