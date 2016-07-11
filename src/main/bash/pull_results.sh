#!/usr/bin/env bash

HOSTS="root@128.142.134.55 root@128.142.242.119 root@188.184.165.208"

i=0

for host in $HOSTS; do
    scp "$*" ${host}:/tmp/results tmp
    mv tmp/results tmp/$i
    ((i+=1))
done

