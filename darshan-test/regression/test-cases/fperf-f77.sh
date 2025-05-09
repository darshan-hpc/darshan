#!/bin/bash

PROG=fperf-f77

# set log file path; remove previous log if present
export DARSHAN_LOGFILE=$DARSHAN_TMP/${PROG}.darshan
rm -f ${DARSHAN_LOGFILE}

# compile
$DARSHAN_F77 $DARSHAN_TESTDIR/test-cases/src/fperf.f -o $DARSHAN_TMP/${PROG}
if [ $? -ne 0 ]; then
    echo "Error: failed to compile ${PROG}" 1>&2
    exit 1
fi

# execute
$DARSHAN_RUNJOB $DARSHAN_TMP/${PROG} -fname $DARSHAN_TMP/${PROG}.tmp.dat
if [ $? -ne 0 ]; then
    echo "Error: failed to execute ${PROG}" 1>&2
    exit 1
fi

# parse log
$DARSHAN_UTIL_PATH/bin/darshan-parser $DARSHAN_LOGFILE > $DARSHAN_TMP/${PROG}.darshan.txt
if [ $? -ne 0 ]; then
    echo "Error: failed to parse ${DARSHAN_LOGFILE}" 1>&2
    exit 1
fi

# check results
# in this case we want to confirm that both the MPI and low-level (POSIX or DFS) open counters were triggered
FILE_OPENS=`grep POSIX_OPENS $DARSHAN_TMP/${PROG}.darshan.txt |grep -vE "^#" |cut -f 5`
if [ -z "$FILE_OPENS" ]; then
    FILE_OPENS=`grep DFS_OPENS $DARSHAN_TMP/${PROG}.darshan.txt |grep -vE "^#" |cut -f 5`
fi
if [ ! "$FILE_OPENS" -gt 0 ]; then
    echo "Error: file open count of $FILE_OPENS is incorrect" 1>&2
    exit 1
fi
MPIIO_OPENS=`grep COLL_OPENS $DARSHAN_TMP/${PROG}.darshan.txt |grep -vE "^#" |cut -f 5`
if [ ! "$MPIIO_OPENS" -gt 0 ]; then
    echo "Error: MPI-IO open count of $MPIIO_OPENS is incorrect" 1>&2
    exit 1
fi

exit 0
