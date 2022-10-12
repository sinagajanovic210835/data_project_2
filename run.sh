#! /bin/bash

PERIOD=$1
echo -e "*/1\t*\t*\t*\t*\t/sparkapp/shell/job1.sh\n" > ./crontabs/sparkmaster.txt
docker-compose up --build & \
sleep 20 && \
docker cp ./corehdfs/core-site.xml spark-master:/spark/conf/
docker cp ./corehdfs/hdfs-site.xml spark-master:/spark/conf/
docker cp ./corehdfs/hive-site.xml spark-master:/spark/conf/
docker exec -d spark-master touch /etc/environment && \
docker exec -d spark-master crontab /crontabs/sparkmaster.txt
docker exec -d spark-master crond start
docker exec -d namenode /shells/start.sh $PERIOD
