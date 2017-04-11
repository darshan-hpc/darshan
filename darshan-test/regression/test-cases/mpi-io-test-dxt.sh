#!/bin/bash

PROG=mpi-io-test

# set log file path; remove previous log if present
export DARSHAN_LOGFILE=$DARSHAN_TMP/${PROG}-dxt.darshan
rm -f ${DARSHAN_LOGFILE}

# compile
$DARSHAN_CC $DARSHAN_TESTDIR/test-cases/src/${PROG}.c -o $DARSHAN_TMP/${PROG}
if [ $? -ne 0 ]; then
    echo "Error: failed to compile ${PROG}" 1>&2
    exit 1
fi

# enable dxt tracing
export DXT_ENABLE_IO_TRACE=

# execute
$DARSHAN_RUNJOB $DARSHAN_TMP/${PROG} -f $DARSHAN_TMP/${PROG}.tmp.dat
if [ $? -ne 0 ]; then
    echo "Error: failed to execute ${PROG}" 1>&2
    exit 1
fi

# parse log
$DARSHAN_PATH/bin/darshan-dxt-parser $DARSHAN_LOGFILE > $DARSHAN_TMP/${PROG}-dxt.darshan.txt
if [ $? -ne 0 ]; then
    echo "Error: failed to parse ${DARSHAN_LOGFILE}" 1>&2
    exit 1
fi

# TODO: check results

# also, ensure that darshan-parser doesn't complain if given a log file that
# has DXT data present
$DARSHAN_PATH/bin/darshan-parser $DARSHAN_LOGFILE > /dev/null
if [ $? -ne 0 ]; then
    echo "Error: darshan-parser failed to handle ${DARSHAN_LOGFILE}" 1>&2
    exit 1
fi

unset DXT_ENABLE_IO_TRACE

exit 0
