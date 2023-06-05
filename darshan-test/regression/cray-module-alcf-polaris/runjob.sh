#!/bin/bash

PROJ=radix-io

# can't pass args to scripts with PBS, so we assign to an env
# var and reference that in the submit script
export DARSHAN_SCRIPT_ARGS="$@"

# set list of env vars to pass through to PBS job
ENV_VAR_LIST="DARSHAN_LOGFILE,DARSHAN_DEFAULT_NPROCS,DARSHAN_SCRIPT_ARGS"
if [ -n "${DXT_ENABLE_IO_TRACE+defined}" ]; then
	ENV_VAR_LIST="$ENV_VAR_LIST,DXT_ENABLE_IO_TRACE"
fi

# submit job and wait for it to return
jobid=`qsub -A $PROJ -q debug -l select=1,walltime=0:10:00,filesystems=home:grand:eagle -v $ENV_VAR_LIST -o $DARSHAN_TMP/$$-tmp.out -e $DARSHAN_TMP/$$-tmp.err $DARSHAN_TESTDIR/$DARSHAN_PLATFORM/pbs-submit.sh`

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

# extract final job exit code using qstat
job_exit=`qstat -f -x $jobid | grep Exit_status | tr -d '[:blank:]' | cut -d= -f2`
if [ $job_exit -ne 0 ]; then
	exit 1
else
	exit 0
fi
