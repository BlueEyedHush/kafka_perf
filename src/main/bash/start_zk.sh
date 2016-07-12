#!/usr/bin/env bash

ZK_DIR=/opt/kafka_perf/zookeeper/latest
ZK_DATA=/mnt/vol1/zk

export ZOO_LOG_DIR=/var/log/zookeeper
mkdir -p $ZOO_LOG_DIR


# shutdown zookeeper just in case
$ZK_DIR/bin/zkServer.sh stop

# start Zookeeper
$ZK_DIR/bin/zkServer.sh start
