#!/bin/bash

# ensure the sample library libabcxyz.so is found
export LD_LIBRARY_PATH=.

export darshan_install=$(cd $(dirname $(which darshan-config)); cd ..; pwd -L)
export darshan_libpath=$darshan_install/lib/libdarshan.so


# enable and configure darshan to write logs into current directory
#export LD_PRELOAD=$PWD/../../darshan-runtime/lib/libdarshan.so
export LD_PRELOAD=$darshan_libpath
export DARSHAN_LOG_DIR_PATH=$PWD
#export DXT_ENABLE_IO_TRACE=4



mpirun -oversubscribe -np 1 ./app_write A
mpirun -oversubscribe -np 1 ./app_write B
mpirun -oversubscribe -np 1 ./app_write Z
mpirun -oversubscribe -np 1 ./app_read A
mpirun -oversubscribe -np 4 ./app_readAB_writeC
mpirun -oversubscribe -np 1 ./app_read C
