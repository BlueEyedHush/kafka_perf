#!/usr/bin/env bash

# requires id.sh file, which sets MYID env variable - zookeepers myid
# also if we are on an instance which hosts only Zookeeper, without Kafka, zkonly file should be present
source ./id.sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ZKDELALL="$DIR/../python/zkDelAll.py"
KAFKA_DIR=/opt/kafka_perf/kafka/latest
KAFKA_DATA=/mnt/vol1/kf

# shutdown kafka
if [ ! -f zkonly ]; then
    #$KAFKA_DIR/bin/kafka-server-stop.sh
    for pid in `ps aux | grep [k]afka.logs.dir | awk '{print $2}' | tr '\n' ' '`; do kill -s 9 $pid; done
fi

# remove all data from zookeeper
if [ -f zkonly ]; then
    python $ZKDELALL /
fi

# remove Kafka's data dir
if [ ! -f zkonly ]; then
    rm -rf $KAFKA_DATA
fi

# create folders for new data
if [ ! -f zkonly ]; then
    mkdir -p $KAFKA_DATA
fi

# give ZK a little bit more time to replicate
sleep 3

# start Kafka
if [ ! -f zkonly ]; then
    $KAFKA_DIR/bin/kafka-server-start.sh -daemon $KAFKA_DIR/config/server.properties
fi
