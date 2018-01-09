#!/bin/bash

# convert DXT env setting
if [ -n "${DXT_ENABLE_IO_TRACE+defined}" ]; then
	DXT_ENV="--env DXT_ENABLE_IO_TRACE=$DXT_ENABLE_IO_TRACE"
fi

# submit job and get job id
jobid=`qsub --env DARSHAN_LOGFILE=$DARSHAN_LOGFILE $DXT_ENV --mode c16 --proccount $DARSHAN_DEFAULT_NPROCS -A radix-io -t 10 -n 1 --output $DARSHAN_TMP/$$-tmp.out --error $DARSHAN_TMP/$$-tmp.err --debuglog $DARSHAN_TMP/$$-tmp.debuglog "$@"`
if [ $? -ne 0 ]; then
	echo "Error: failed to qsub $@"
	exit 1
fi

output="foo"
rc=0

# loop as long as qstat succeeds and shows information about job
while [ -n "$output" -a "$rc" -eq 0 ]; do
	sleep 5
	output=`qstat $jobid`
	rc=$?
done

# look for return code
grep "exit code of 0" $DARSHAN_TMP/$$-tmp.debuglog >& /dev/null
if [ $? -ne 0 ]; then
	exit 1
else
	exit 0
fi
