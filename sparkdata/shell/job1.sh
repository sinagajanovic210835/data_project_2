#! /bin/bash
sleep 20
BOOL=`cat /bool/bool.txt`
echo $BOOL
if [[ $BOOL != "" ]]
then     
    /spark/bin/spark-shell -I /sparkapp/SaveHiveTables.scala --conf spark.driver.args="$BOOL" --conf "spark.sql.parquet.writeLegacyFormat=true"   
fi  
echo "" > /bool/bool.txt

