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

# TODO: look for "head" commands in all of these test cases and remove them
# later.  Right now they are needed to make sure we only check 1 out of 4
# possible records because reduction is not implemented yet

# check results

# in this case we want to confirm that the STDIO counters were triggered
STDIO_OPENS=`grep STDIO_OPENS $DARSHAN_TMP/${PROG}.darshan.txt |head -n 1 |cut -f 5`
if [ ! "$STDIO_OPENS" -gt 0 ]; then
    echo "Error: STDIO open count of $STDIO_OPENS is incorrect" 1>&2
    exit 1
fi
STDIO_BYTES_WRITTEN=`grep STDIO_BYTES_WRITTEN $DARSHAN_TMP/${PROG}.darshan.txt |head -n 1 |cut -f 5`
if [ ! "$STDIO_BYTES_WRITTEN" -eq 6 ]; then
    echo "Error: STDIO open count of $STDIO_BYTES_WRITTEN is incorrect" 1>&2
    exit 1
fi


# make sure that some of the floating point counters are valid
# use bc for floating point comparison
STDIO_F_OPEN_START_TIMESTAMP=`grep STDIO_F_OPEN_START_TIMESTAMP $DARSHAN_TMP/${PROG}.darshan.txt |head -n 1 |cut -f 5`
if [ ! $(echo "$STDIO_F_OPEN_START_TIMESTAMP > 0" | bc -l) ]; then
    echo "Error: counter is incorrect" 1>&2
    exit 1
fi
STDIO_F_OPEN_END_TIMESTAMP=`grep STDIO_F_OPEN_END_TIMESTAMP $DARSHAN_TMP/${PROG}.darshan.txt |head -n 1 |cut -f 5`
if [ ! $(echo "$STDIO_F_OPEN_END_TIMESTAMP > 0" | bc -l) ]; then
    echo "Error: counter is incorrect" 1>&2
    exit 1
fi
STDIO_F_META_TIME=`grep STDIO_F_META_TIME $DARSHAN_TMP/${PROG}.darshan.txt |head -n 1 |cut -f 5`
if [ ! $(echo "$STDIO_F_META_TIME > 0" | bc -l) ]; then
    echo "Error: counter is incorrect" 1>&2
    exit 1
fi
STDIO_F_WRITE_TIME=`grep STDIO_F_WRITE_TIME $DARSHAN_TMP/${PROG}.darshan.txt |head -n 1 |cut -f 5`
if [ ! $(echo "$STDIO_F_WRITE_TIME > 0" | bc -l) ]; then
    echo "Error: counter is incorrect" 1>&2
    exit 1
fi
STDIO_F_CLOSE_START_TIMESTAMP=`grep STDIO_F_CLOSE_START_TIMESTAMP $DARSHAN_TMP/${PROG}.darshan.txt |head -n 1 |cut -f 5`
if [ ! $(echo "$STDIO_F_CLOSE_START_TIMESTAMP > 0" | bc -l) ]; then
    echo "Error: counter is incorrect" 1>&2
    exit 1
fi
STDIO_F_CLOSE_END_TIMESTAMP=`grep STDIO_F_CLOSE_END_TIMESTAMP $DARSHAN_TMP/${PROG}.darshan.txt |head -n 1 |cut -f 5`
if [ ! $(echo "$STDIO_F_CLOSE_END_TIMESTAMP > 0" | bc -l) ]; then
    echo "Error: counter is incorrect" 1>&2
    exit 1
fi


exit 0
