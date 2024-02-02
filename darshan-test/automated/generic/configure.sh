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

cd build/darshan-runtime
../../darshan-runtime/configure --enable-apmpi-mod --prefix=$DARSHAN_INSTALL_PREFIX --with-jobid-env=DARSHAN_JOBID --with-log-path=$DARSHAN_LOG_PATH CC=mpicc

cd ../darshan-util
../../darshan-util/configure --enable-apmpi-mod --enable-apxc-mod --prefix=$DARSHAN_INSTALL_PREFIX

cd ../../
