#!/usr/bin/env bash

echo -e "\n\n\n------------------- "`date`"------------------- \n\n\n" >> ./bench.log
fab $* 2>&1 | tee -a ./bench.log