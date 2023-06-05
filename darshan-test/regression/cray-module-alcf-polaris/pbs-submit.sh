#!/bin/bash

nprocs=$DARSHAN_DEFAULT_NPROCS
nnodes=`wc -l < $PBS_NODEFILE`
ppn=$((nprocs / nnodes))

mpiexec -n $nprocs --ppn $ppn --env LD_DEBUG=all $DARSHAN_SCRIPT_ARGS
EXIT_STATUS=$?

exit $EXIT_STATUS
