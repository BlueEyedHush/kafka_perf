#!/bin/bash

LOG_DIR=/var/log/kafka
mkdir -p $LOG_DIR
kafka/latest/bin/kafka-server-start.sh kafka/latest/config/server.properties 2>&1 | tee $LOG_DIR/kafka.log

