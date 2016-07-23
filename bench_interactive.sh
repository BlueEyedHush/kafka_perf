#!/usr/bin/env bash

# setup logging
SUITE_NAME=`date +%d%m%y_%H%M`
SUITE_DIR=./logs/$SUITE_NAME
SUMMARY_FILE=$SUITE_DIR/summary.out

mkdir -p $SUITE_DIR

# arguments
TASK_NAME=$1
shift

if [[ $TASK_NAME == run_test_suite ]]; then
    ARGS="suite_log_dir=$SUITE_DIR"
else
    ARGS=""
fi

for arg in $*; do
    COMMAS_ESCAPED=$(echo "$arg" | sed -e 's/,/\\,/g')
    ARGS=$ARGS",$COMMAS_ESCAPED"
done

if [[ $ARGS = ,* ]]; then # if args start with comma, strip it
    ARGS=${ARGS:1}
fi

echo -e "\n\n\n------------------- "`date`"------------------- \n\n\n" >> $SUMMARY_FILE
export PYTHONUNBUFFERED=true

if [[ -z $ARGS ]]; then
    FAB_ARGS=$TASK_NAME
else
    FAB_ARGS=$TASK_NAME:"$ARGS"
fi

fab -I $FAB_ARGS 2>&1 | tee -a $SUMMARY_FILE