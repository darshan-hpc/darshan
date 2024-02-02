#!/bin/bash
#
# Run configure for runtime and utils

basedir=$PWD

if [ -z "${DARSHAN_INSTALL_PREFIX}" ]; then
    DARSHAN_INSTALL_PREFIX=$basedir/install
fi
if [ -z "${DARSHAN_LOG_PATH}" ]; then
    DARSHAN_LOG_PATH=$basedir/logs
fi

# unload any darshan module and use GNU compilers
module unload darshan
module swap PrgEnv-nvhpc PrgEnv-gnu
# configure failures unless this is also loaded
module load cudatoolkit-standalone

cd build/darshan-runtime
../../darshan-runtime/configure --enable-apmpi-mod --prefix=$DARSHAN_INSTALL_PREFIX --with-jobid-env=PBS_JOBID --with-log-path=$DARSHAN_LOG_PATH --disable-cuserid CC=cc

cd ../darshan-util
../../darshan-util/configure --enable-apmpi-mod --prefix=$DARSHAN_INSTALL_PREFIX

cd ../../

module list
