#! /bin/bash

mkdir ./Csv/invoicesByHour
mkdir ./Csv/countriesByDay
mkdir ./Csv/productsByDay && \
python3 changeCsv.py && python3 scheduler.py && python3 productByDay.py && python3 countriesByDay.py && \
cd ./sparkdata/SavePostgres && sbt assembly && cd ../../
PERIOD=$1
echo -e "*/1\t*\t*\t*\t*\t/sparkapp/shell/job1.sh\n" > ./crontabs/sparkmaster.txt
docker-compose up --build & \
sleep 30
sleep 5
docker cp ./corehdfs/core-site.xml spark-master:/spark/conf/
docker cp ./corehdfs/hdfs-site.xml spark-master:/spark/conf/
docker cp ./corehdfs/hive-site.xml spark-master:/spark/conf/
docker exec -d spark-master crontab /crontabs/sparkmaster.txt
docker exec -d spark-master crond start
docker exec -d namenode /shells/start.sh $PERIOD
