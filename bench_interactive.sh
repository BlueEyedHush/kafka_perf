#!/usr/bin/env bash

# setup logging
SUITE_NAME=`date +%d%m%y_%H%M`
SUITE_DIR=./logs/$SUITE_NAME
SUMMARY_FILE=$SUITE_DIR/summary.out

mkdir -p $SUITE_DIR

# arguments
ARGS="suite_log_dir=$SUITE_DIR"
for arg in $*; do
    COMMAS_ESCAPED=$(echo "$arg" | sed -e 's/,/\\,/g')
    ARGS=$ARGS",$COMMAS_ESCAPED"
done

echo -e "\n\n\n------------------- "`date`"------------------- \n\n\n" >> $SUMMARY_FILE
export PYTHONUNBUFFERED=true
fab run_test_suite:"$ARGS" 2>&1 | tee -a $SUMMARY_FILE