#!/bin/bash

# General notes
#######################
# Script to set up whatever is needed to run an MPI job. This script should
# produce the string of the command line prefix to use for job execution.
# This could (for example) point to a wrapper script that will submit a job
# to a scheduler and wait for its completion.

# Notes specific to this platform (ws)
########################
# This particular version of the setup-runjob script just uses mpiexec,
# assuming that is in the path already.

echo mpiexec -n $DARSHAN_DEFAULT_NPROCS
exit 0
