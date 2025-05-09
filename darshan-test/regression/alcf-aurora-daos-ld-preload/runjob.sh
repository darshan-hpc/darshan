#!/bin/bash

PROJ=radix-io

# can't pass args to scripts with PBS, so we assign to an env
# var and reference that in the submit script
export DARSHAN_SCRIPT_ARGS="$@"

# set list of env vars to pass through to PBS job
ENV_VAR_LIST="DARSHAN_LOGFILE,DARSHAN_DEFAULT_NPROCS,DARSHAN_SCRIPT_ARGS,DARSHAN_RUNTIME_PATH,DAOS_POOL,DAOS_CONT"
if [ -n "${DXT_ENABLE_IO_TRACE+defined}" ]; then
	ENV_VAR_LIST="$ENV_VAR_LIST,DXT_ENABLE_IO_TRACE"
fi

# submit job and wait for it to return
jobid=`qsub -A $PROJ -q debug -l select=1,walltime=0:10:00,filesystems=home:daos_user,daos=daos_user -v $ENV_VAR_LIST -o $DARSHAN_TMP/$$-tmp.out -e $DARSHAN_TMP/$$-tmp.err $DARSHAN_TESTDIR/$DARSHAN_PLATFORM/pbs-submit.sh`

if [ $? -ne 0 ]; then
        echo "Error: failed to qsub $@"
        exit 1
fi

# qstat seems to return errors a lot here... so use a retry loop
retries=0
max_retries=5
while true; do
    sleep 5
    qstat_output=$(qstat -f -x "$jobid")
    if [[ $? -ne 0 ]]; then
        echo "qstat failed (attempt $((retries + 1)) of $max_retries)"
        ((retries++))
        if [[ $retries -ge $max_retries ]]; then
            echo "qstat failed $max_retries times. Giving up."
            exit 1
        fi
        continue
    fi

    # reset retry counter on successful qstat
    retries=0

    # determine if job finished, and break out of loop if so
    job_state=$(echo "$qstat_output" | grep job_state | tr -d '[:blank:]' | cut -d= -f2)
    if [[ "$job_state" == "F" ]]; then
        break
    fi
done

job_exit=$(echo "$qstat_output" | grep Exit_status | tr -d '[:blank:]' | cut -d= -f2)
if [ $job_exit -ne 0 ]; then
    exit 1
else
    exit 0
fi
