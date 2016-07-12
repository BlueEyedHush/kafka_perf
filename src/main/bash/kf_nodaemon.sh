#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/env.sh

mkdir -p $KAFKA_LOG_DIR
$KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/server.properties 2>&1 | tee $KAFKA_LOG_DIR/kafka.log
