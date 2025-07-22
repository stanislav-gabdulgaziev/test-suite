#!/bin/bash
# Get the parameters.
SCALE=$1
LOCATION=$2
CONN_IMPALA_SHELL=$3
FORMAT=$4

if [ X"$CONN_IMPALA_SHELL" = "X" ]; then
        CONN_IMPALA_SHELL="impala-shell -i localhost:21050"

if [ X"$FORMAT" = "X" ]; then
        FORMAT="row format delimited fields terminated by '|' stored as textfile"

echo "Creating external tables."

CONN_IMPALA_SHELL="beeline -n hive -u 'jdbc:hive2://localhost:10000'"

runcommand "$CONN_IMPALA_SHELL  -i settings/load-flat.sql -f create_alltables_csv.sql --var=DB=tpcds_text_${SCALE} --var=FORMAT=${FORMAT} --var=LOCATION=${LOCATION}"

echo "Database tpcds_text_${SCALE} is created"