#!/bin/bash
#
#  (C) 2010 by Argonne National Laboratory.
#      See COPYRIGHT in top-level directory.
#

# usage: fsstats-runner.bash <hostname> <path>

# make this configurable or something...
FSSTATS_PATH=/home/harms/darshan/trunk/test

if [ "${#}" != 4 ]
then
    echo "Error: bad arguments"
    exit 1
fi
HOST=${1}
FS_PATH=${2}
CHKPNT=${3}
RESTART=${4}

# the output file name will be the path that was scanned, but with equals
# signs in place of the slashes
OUT_FILE=`echo $FS_PATH | sed -e 's/\//=/g'`
OUT_FILE="${OUT_FILE}.csv"

# launch remote command
if [ $RESTART -eq 1 ];
then
    ssh -oBatchMode=yes -n $HOST "${FSSTATS_PATH}/fsstats -r $CHKPNT -c $CHKPNT -o /tmp/pfsstats-$$.csv $FS_PATH >& /dev/null"
else
    ssh -oBatchMode=yes -n $HOST "${FSSTATS_PATH}/fsstats -c $CHKPNT -o /tmp/pfsstats-$$.csv $FS_PATH >& /dev/null"
fi

if [ "${?}" != 0 ]
then
    exit 1
fi

# retrieve output file
scp -oBatchMode=yes $HOST:/tmp/pfsstats-$$.csv $OUT_FILE >& /dev/null
if [ "${?}" != 0 ]
then
    exit 1
fi

# delete file on remote host
ssh -oBatchMode=yes -n $HOST "rm -f /tmp/pfsstats-$$.csv >& /dev/null"
if [ "${?}" != 0 ]
then
    exit 1
fi

exit 0
