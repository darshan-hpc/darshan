#!/bin/bash
#
# Create build, install and log directories.
#

basedir=$PWD

if [ -z "${DARSHAN_LOG_PATH}" ]; then
    DARSHAN_LOG_PATH=$basedir/logs
fi

git submodule update --init

./prepare.sh

mkdir $DARSHAN_LOG_PATH

mkdir -p build/darshan-runtime

mkdir -p build/darshan-util
