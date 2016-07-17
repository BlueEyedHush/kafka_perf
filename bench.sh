#!/usr/bin/env bash

nohup fab $* > ./bench.log 2>&1 < /dev/null &