#!/bin/bash

# Check if:
# 1. the file whose name is given in argument and extension is .txt is in the folder ${WORK_DIR}/data/schedule_temp/
# 2. there is a difference between the csv file in ${WORK_DIR}/data/schedule/ and the txt file in ${WORK_DIR}/data/schedule_temp/
# 3. the database duckDB is not locked

# Check arguments
if [ $# != 1 ]
then
    echo "One argument is required."
    echo "Missing argument 1: Path of the file to test"
    # Return a code that will abort retry of the airflow sensor.
    exit 2
else
    filename=$1
fi

# Check 1.
if [ -f ${WORK_DIR}/data/schedule_temp/$filename.txt ]
then

    # Check 2.
    if [ -f ${WORK_DIR}/data/schedule/$filename.csv ] && \
        ! cmp -s ${WORK_DIR}/data/schedule_temp/$filename.txt ${WORK_DIR}/data/schedule/$filename.csv
    then
        echo "Files ${WORK_DIR}/data/schedule_temp/$filename.txt and ${WORK_DIR}/data/schedule/$filename.csv are different."
        echo "Upload content in the database"
        exit 0
    elif [ ! -f ${WORK_DIR}/data/schedule/$filename.csv ]
    then
        echo "No reference filename ${WORK_DIR}/data/schedule/$filename.csv."
        echo "Upload content in the database"
        exit 0
    else
        # Both files exist and are identical.
        # Return a code that will abort retry of the airflow sensor.
        echo "Files ${WORK_DIR}/data/schedule_temp/$filename.txt and ${WORK_DIR}/data/schedule/$filename.csv are identical."
        echo "No upload to the database. Skip DAG branch."
        exit 99
    fi
fi

# Otherwise, return a code that will keep the retry rule of the airflow sensor
echo "Default stop"
exit 1