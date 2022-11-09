#! /bin/bash

sleep 10
BOOL=`cat /bool/bool.txt`
echo $BOOL
if [[ $BOOL != "" ]]
then     
    /spark/bin/spark-shell -I /sparkapp/SaveHiveTables.scala --conf spark.driver.args="$BOOL" --conf "spark.sql.parquet.writeLegacyFormat=true" && \
    if [[ "$BOOL" == *invoice* ]]
    then
        sleep 3
         /spark/bin/spark-submit --jars /driver/postgresql-42.3.5.jar /sparkapp/SavePostgres/target/scala-2.12/SavePostgres-assembly-0.1.0-SNAPSHOT.jar
    fi
    echo "" > /bool/bool.txt
fi

