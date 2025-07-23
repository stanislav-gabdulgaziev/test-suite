#!/bin/bash
# Get the parameters.
SCALE=$1
LOCATION=$2
CONN_IMPALA_SHELL=$3

if [ X"$CONN_IMPALA_SHELL" = "X" ]; then
        CONN_IMPALA_SHELL="localhost:21050"

echo "Creating external tables."

runcommand "impala-shell -i  $CONN_IMPALA_SHELL -f create_alltables_csv.sql --var=DB=tpcds_text_${SCALE} --var=LOCATION=${LOCATION}"

echo "Database tpcds_text_${SCALE} is created"