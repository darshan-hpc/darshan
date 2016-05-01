#!/bin/bash

PROG=stdio-test

# set log file path; remove previous log if present
export DARSHAN_LOGFILE=$DARSHAN_TMP/${PROG}.darshan
rm -f ${DARSHAN_LOGFILE}

# compile
$DARSHAN_CC $DARSHAN_TESTDIR/test-cases/src/${PROG}.c -o $DARSHAN_TMP/${PROG}
if [ $? -ne 0 ]; then
    echo "Error: failed to compile ${PROG}" 1>&2
    exit 1
fi

# execute
$DARSHAN_RUNJOB $DARSHAN_TMP/${PROG} -f $DARSHAN_TMP/${PROG}.tmp.dat
if [ $? -ne 0 ]; then
    echo "Error: failed to execute ${PROG}" 1>&2
    exit 1
fi

# parse log
$DARSHAN_PATH/bin/darshan-parser $DARSHAN_LOGFILE > $DARSHAN_TMP/${PROG}.darshan.txt
if [ $? -ne 0 ]; then
    echo "Error: failed to parse ${DARSHAN_LOGFILE}" 1>&2
    exit 1
fi

# check results
# in this case we want to confirm that the STDIO counters were triggered
# TODO: change this to check aggregate counters; for now we tail -n 1 to get
# one counter because there is no reduction operator for the stdio module yet
STDIO_OPENS=`grep STDIO_FOPENS $DARSHAN_TMP/${PROG}.darshan.txt |tail -n 1 |cut -f 5`
if [ ! "$STDIO_OPENS" -gt 0 ]; then
    echo "Error: STDIO open count of $STDIO_FOPENS is incorrect" 1>&2
    exit 1
fi

exit 0
