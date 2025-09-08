#!/bin/bash

# Check if:
# 1. the file given in argument is in the folder ${WORK_DIR}/data/schedule_temp/
# 2. there is a difference between the file given in argument in ${WORK_DIR}/data/schedule/ and in ${WORK_DIR}/data/schedule_temp/
# 3. the database duckDB is not locked

# Check arguments
if [ $# != 1 ]
then
    echo "One argument is required."
    echo "Missing argument 1: Path of the file to test"
    # Return a code that will abort retry of the airflow sensor.
    exit 2
else
    file=$1
fi

# Check 1.
if [ -f ${WORK_DIR}/data/schedule_temp/$file ]
then

    # Check 2.
    if [ -f ${WORK_DIR}/data/schedule/$file ] && \
        ! cmp -s ${WORK_DIR}/data/schedule_temp/$file ${WORK_DIR}/data/schedule/$file
    then
        echo "Files ${WORK_DIR}/data/schedule_temp/$file and ${WORK_DIR}/data/schedule/$file are different."
        echo "Upload content of ${WORK_DIR}/data/schedule_temp/$file in the database"
        exit 0
    elif [ ! -f ${WORK_DIR}/data/schedule/$file ]
    then
        echo "No reference file ${WORK_DIR}/data/schedule/$file are different."
        echo "Upload content of ${WORK_DIR}/data/schedule_temp/$file in the database"
        exit 0
    else
        # Both files exist and are identical.
        # Return a code that will abort retry of the airflow sensor.
        echo "Files ${WORK_DIR}/data/schedule_temp/$file and ${WORK_DIR}/data/schedule/$file are identical."
        echo "No upload to the database"
        exit 2
    fi
fi

# Otherwise, return a code that will keep the retry rule of the airflow sensor
echo "Default stop"
exit 1


