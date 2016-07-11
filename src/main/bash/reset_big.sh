#!/usr/bin/env bash

# requires id.sh file, which sets MYID env variable - zookeepers myid
# also if we are on an instance which hosts only Zookeeper, without Kafka, zkonly file should be present
source ./id.sh

KAFKA_DIR=/opt/kafka_perf/kafka/latest
KAFKA_DATA=/mnt/vol1/kf
ZK_DIR=/opt/kafka_perf/zookeeper/latest
ZK_DATA=/mnt/vol1/zk

export ZOO_LOG_DIR=/var/log/zookeeper
mkdir -p $ZOO_LOG_DIR

# shutdown zookeeper
$ZK_DIR/bin/zkServer.sh stop
sleep 5

# shutdown kafka
if [ ! -f zkonly ]; then
    #$KAFKA_DIR/bin/kafka-server-stop.sh
    for pid in `ps aux | grep java | awk '{print $2}' | tr '\n' ' '`; do kill -s 9 $pid; done
fi

# remove Kafka's data dir
if [ ! -f zkonly ]; then
    rm -rf $KAFKA_DATA
fi

# remove Zookeeper's data
rm -rf $ZK_DATA

# create folders for new data
if [ ! -f zkonly ]; then
    mkdir -p $KAFKA_DATA
fi
mkdir -p $ZK_DATA

# create Zk's myid
echo $MYID > $ZK_DATA/myid

# start Zookeeper
$ZK_DIR/bin/zkServer.sh start

# start Kafka
if [ ! -f zkonly ]; then
    $KAFKA_DIR/bin/kafka-server-start.sh -daemon $KAFKA_DIR/config/server.properties
fi
