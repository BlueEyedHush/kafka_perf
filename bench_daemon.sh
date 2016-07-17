#!/usr/bin/env bash

echo -e "\n\n\n------------------- "`date`"------------------- \n\n\n"
export PYTHONUNBUFFERED=true
nohup fab $* > ./bench.log 2>&1 < /dev/null &