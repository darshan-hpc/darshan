#!/bin/bash

module use /soft/modulefiles
module load daos/base

launch-dfuse.sh ${DAOS_POOL}:${DAOS_CONT}
mount | grep dfuse

nprocs=$DARSHAN_DEFAULT_NPROCS
nnodes=`wc -l < $PBS_NODEFILE`
ppn=$((nprocs / nnodes))

mpiexec -n $nprocs --ppn $ppn --env LD_PRELOAD=$DARSHAN_RUNTIME_PATH/lib/libdarshan.so $DARSHAN_SCRIPT_ARGS
EXIT_STATUS=$?

clean-dfuse.sh ${DAOS_POOL}:${DAOS_CONT}

exit $EXIT_STATUS
