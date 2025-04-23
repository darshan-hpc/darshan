#!/bin/bash

nprocs=$DARSHAN_DEFAULT_NPROCS
nnodes=`wc -l < $PBS_NODEFILE`
ppn=$((nprocs / nnodes))

mpiexec -n $nprocs --ppn $ppn --env LD_PRELOAD=$DARSHAN_RUNTIME_PATH/lib/libdarshan.so $DARSHAN_SCRIPT_ARGS
EXIT_STATUS=$?

exit $EXIT_STATUS
