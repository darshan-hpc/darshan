#!/bin/bash

PROG=mpi-io-test

# set log file path; remove previous log if present
export DARSHAN_LOGFILE=$DARSHAN_TMP/${PROG}.darshan.gz
rm -f ${DARSHAN_LOGFILE}

# compile
$DARSHAN_CC test-cases/src/${PROG}.c -o $DARSHAN_TMP/${PROG}
if [ $? != 0 ]; then
    echo "Error: failed to compile ${PROG}" 1>&2
    exit 1
fi

# execute
$DARSHAN_RUNJOB $DARSHAN_TMP/${PROG} -f $DARSHAN_TMP/${PROG}.tmp.dat
if [ $? != 0 ]; then
    echo "Error: failed to execute ${PROG}" 1>&2
    exit 1
fi

# parse log
$DARSHAN_PATH/bin/darshan-parser $DARSHAN_LOGFILE > $DARSHAN_TMP/${PROG}.darshan.txt
if [ $? != 0 ]; then
    echo "Error: failed to parse ${DARSHAN_LOGFILE}" 1>&2
    exit 1
fi

# check results
# in this case we want to confirm that both the MPI and POSIX open counters were triggered
MPI_OPENS=`grep CP_INDEP_OPENS $DARSHAN_TMP/${PROG}.darshan.txt |cut -f 4`
if [ ! $MPI_OPENS > 0 ]; then
    echo "Error: MPI open count of $MPI_OPENS is incorrect" 1>&2
    exit 1
fi
POSIX_OPENS=`grep CP_POSIX_OPENS $DARSHAN_TMP/${PROG}.darshan.txt |cut -f 4`
if [ ! $POSIX_OPENS > 0 ]; then
    echo "Error: POSIX open count of $POSIX_OPENS is incorrect" 1>&2
    exit 1
fi


exit 0
