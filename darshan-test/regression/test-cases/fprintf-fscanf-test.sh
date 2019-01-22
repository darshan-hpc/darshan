#!/bin/bash

PROG=fprintf-fscanf-test

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

STDIO_OPENS=`grep STDIO_OPENS $DARSHAN_TMP/${PROG}.darshan.txt | grep -vE "^#" | grep -vE "STDIN|STDOUT|STDERR" | cut -f 5`
if [ ! "$STDIO_OPENS" -eq 4 ]; then
    echo "Error: STDIO open count of $STDIO_OPENS is incorrect" 1>&2
    exit 1
fi

# this will check fprintf counting
STDIO_BYTES_WRITTEN=`grep STDIO_BYTES_WRITTEN $DARSHAN_TMP/${PROG}.darshan.txt | grep -vE "^#" | grep -vE "STDIN|STDOUT|STDERR" | cut -f 5`
if [ ! "$STDIO_BYTES_WRITTEN" -eq 15 ]; then
    echo "Error: STDIO bytes written count of $STDIO_BYTES_WRITTEN is incorrect" 1>&2
    exit 1
fi

# this will check fscanf counting
STDIO_BYTES_READ=`grep STDIO_BYTES_READ $DARSHAN_TMP/${PROG}.darshan.txt | grep -vE "^#" | grep -vE "STDIN|STDOUT|STDERR" | cut -f 5`
if [ ! "$STDIO_BYTES_READ" -eq 15 ]; then
    echo "Error: STDIO bytes read count of $STDIO_BYTES_READ is incorrect" 1>&2
    exit 1
fi


exit 0
