#!/usr/bin/env bash

# usage: <this>.sh t1 t10 t100

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ANALYZER=$DIR/../python/analyzer/analyzer.py

PREFIX=$DIR/../../../tmp
TARGET_FILE=$PREFIX/results

echo -e "\n" >> $TARGET_FILE

for fname in $*; do
    FILES=$PREFIX/0/$fname*" "$PREFIX/1/$fname*" "$PREFIX/2/$fname*
    PERCENTILE=`python $ANALYZER $FILES`
    TCOUNT=${fname:1}
    echo "$TCOUNT $PERCENTILE" >> "$TARGET_FILE"
done