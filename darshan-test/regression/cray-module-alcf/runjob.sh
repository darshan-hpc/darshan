#!/bin/bash

# convert DXT env setting
if [ -n "${DXT_ENABLE_IO_TRACE+defined}" ]; then
        DXT_ENV="--env DXT_ENABLE_IO_TRACE=$DXT_ENABLE_IO_TRACE"
fi

# submit job and get job id
jobid=`qsub --env DARSHAN_LOGFILE=$DARSHAN_LOGFILE --env DARSHAN_DEFAULT_NPROCS=$DARSHAN_DEFAULT_NPROCS $DXT_ENV --proccount $DARSHAN_DEFAULT_NPROCS -A CSC250STDM12 -q debug-cache-quad -t 20 -n 1 --run_project --output $DARSHAN_TMP/$$-tmp.out --error $DARSHAN_TMP/$$-tmp.err --debuglog $DARSHAN_TMP/$$-tmp.debuglog $DARSHAN_TESTDIR/$DARSHAN_PLATFORM/cobalt-submit.sh "$@"`

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
	# sleep to give time for exit code line to appear in log file
	sleep 5
	grep "exit code of 0" $DARSHAN_TMP/$$-tmp.debuglog >& /dev/null
	if [ $? -ne 0 ]; then
		exit 1
	else
		exit 0
	fi
else
        exit 0
fi
