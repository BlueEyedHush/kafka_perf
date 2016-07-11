#!/usr/bin/env bash

# requires key.sh, which sets KEYFILE variable, pointing to the public key

HOSTS="root@128.142.134.55 root@128.142.242.119 root@188.184.165.208"

source ./keyfile.sh

i=0

for host in $HOSTS; do
    scp -i${KEYFILE} ${host}:/tmp/results tmp
    mv tmp/results tmp/$i
    ((i+=1))
done

