#!/bin/bash

# submit job and wait for it to return
sbatch --wait -N 1 -t 10 -p debug -C cpu --output $DARSHAN_TMP/$$-tmp.out --error $DARSHAN_TMP/$$-tmp.err $DARSHAN_TESTDIR/$DARSHAN_PLATFORM/slurm-submit.sl "$@"

# exit with return code of this job submission
exit $?
