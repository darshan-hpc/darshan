#!/bin/bash

# submit job and get job id
jobid=`qsub --mode c16 --proccount $DARSHAN_DEFAULT_NPROCS -A SSSPPg -t 10 -n 1 --output $DARSHAN_TMP/$$-tmp.out --error $DARSHAN_TMP/$$-tmp.err --debuglog $DARSHAN_TMP/$$-tmp.debuglog "$@"`
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
grep "exit code of 0" $DARSHAN_TMP/$$-tmp.debuglog
if [ $? -ne 0 ]; then
	exit 1
else
	exit 0
fi
