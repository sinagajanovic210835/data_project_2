#! /bin/bash

sleep 15

`cat /bool/bool.txt`

echo "export MESSAGE=" > /bool/bool.txt
echo "export TWITTER=0" >> /bool/bool.txt

if [[ `expr $TWITTER` -eq 3 ]]; then
    PROCESS=`ps | grep $BEARER_TOKEN | head -n1 | awk '{print $1}'`
    kill $PROCESS  
    /spark/bin/spark-submit --jars /driver/postgresql-42.3.5.jar /sparkapp/Analyze/target/scala-2.12/Analyze-assembly-0.1.0-SNAPSHOT.jar
elif [ `expr $TWITTER` -eq 5 ]; then
    rm /bool/entities.csv    
    curl "https://api.twitter.com/2/tweets/sample/stream?tweet.fields=entities" -H "Authorization: Bearer $BEARER_TOKEN" | jq '.data.entities.hashtags[].tag' >> /bool/entities.csv & \
fi

if [[ $MESSAGE != "" ]]; then     
    /spark/bin/spark-shell -I /sparkapp/SaveHiveTables.scala --conf spark.driver.args="$MESSAGE" --conf "spark.sql.parquet.writeLegacyFormat=true" && \
    if [[ "$MESSAGE" == *invoice* ]]; then
        sleep 3
        /spark/bin/spark-submit --jars /driver/postgresql-42.3.5.jar /sparkapp/SavePostgres/target/scala-2.12/SavePostgres-assembly-0.1.0-SNAPSHOT.jar
    fi    
fi
