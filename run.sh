#! /bin/bash

PERIOD=$1
COUNTER=$2
DIR=$3
# PASS=$2
# echo $PASS | sudo -S sudo ls > /dev/null
docker-compose up --build & \
sleep 20 && \
docker cp ./corehdfs/core-site.xml spark-master:/spark/conf/
docker cp ./corehdfs/hdfs-site.xml spark-master:/spark/conf/
docker cp ./corehdfs/hive-site.xml spark-master:/spark/conf/
docker exec -d -e spark-master echo 1 "\>" nmb.txt
docker exec -d spark-master crontab /crontabs/sparkmaster.txt
docker exec -d spark-master crond start
docker exec -d namenode /data1/shells/start.sh $PERIOD $COUNTER $DIR

# spark/bin/spark-shell -I /sparkapp/first.scala --conf spark.driver.args="$NMB" & NMB=$((NMB + 1))
# source /etc/bash.bashrc 
