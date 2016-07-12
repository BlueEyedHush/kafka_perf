#!/usr/bin/env bash

# usage: <this>.sh t1 t10 t100

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ANALYZER=$DIR/../python/analyzer/analyzer.py

PREFIX=$DIR/../../../tmp
TARGET_FILE=$PREFIX/results

echo -e "\n" >> $TARGET_FILE

for fname in $*; do
    PERCENTILE=`python $ANALYZER $PREFIX/{0..2}/$fname`
    TCOUNT=${fname:1}
    echo "$TCOUNT $PERCENTILE" >> "$TARGET_FILE"
done