#!/bin/bash

nprocs=$DARSHAN_DEFAULT_NPROCS
nnodes=$COBALT_PARTSIZE
ppn=$((nprocs / nnodes))

aprun -n $nprocs -N $ppn $@
EXIT_STATUS=$?

exit $EXIT_STATUS
