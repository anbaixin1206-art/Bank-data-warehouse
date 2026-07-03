#!/bin/bash
KAFKA_HOME=/mnt/d/bigdata-lab/software/kafka_2.13-3.9.0

echo "Starting Zookeeper..."
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
sleep 5

echo "Starting Kafka Broker..."
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
sleep 10

echo "Verifying..."
ss -tlnp | grep -E "2181|9092"
echo "Kafka started!"
