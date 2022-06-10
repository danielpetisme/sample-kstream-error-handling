#!/bin/bash

docker-compose build
docker-compose -f ./docker-compose.yml up -d

# echo "Waiting to Kafka to starts up"
# sleep 100

# echo "Producing records"
# i=1
# while [ $i -le 10000 ]
# do
#     echo "$i,$i"
#     echo "$i,$i" | docker exec -i broker kafka-console-producer --bootstrap-server broker:9092 --topic input1 --property "parse.key=true" --property "key.separator=,"
#     echo "$i,$i" | docker exec -i broker kafka-console-producer --bootstrap-server broker:9092 --topic input2 --property "parse.key=true" --property "key.separator=,"
#     sleep 1
#     ((i++))
# done
#  
# sleep 10
# echo "Consuming one record"
# docker exec broker kafka-console-consumer --bootstrap-server broker:9092 --topic output --from-beginning --property "print.key=true" --property "key.separator=," --max-messages 1