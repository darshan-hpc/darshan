#!/bin/bash

PROG=cxxpi

# set log file path; remove previous log if present
export DARSHAN_LOGFILE=$DARSHAN_TMP/${PROG}.darshan
rm -f ${DARSHAN_LOGFILE}

# compile
$DARSHAN_CXX $DARSHAN_TESTDIR/test-cases/src/${PROG}.cxx -o $DARSHAN_TMP/${PROG}
if [ $? -ne 0 ]; then
    echo "Error: failed to compile ${PROG}" 1>&2
    exit 1
fi

# execute
$DARSHAN_RUNJOB $DARSHAN_TMP/${PROG}
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
# in this case we want to confirm that the open counts are zero; cxxpi does not do any IO
POSIX_OPENS=`grep POSIX_OPENS $DARSHAN_TMP/${PROG}.darshan.txt |grep -vE "^#" |cut -f 5`
if [ "$POSIX_OPENS"x != ""x ]; then
    echo "Error: Found unexpected POSIX open count of $POSIX_OPENS" 1>&2
    exit 1
fi


exit 0
