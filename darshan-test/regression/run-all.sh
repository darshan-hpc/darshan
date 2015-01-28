#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: run-all.sh <darshan_install_path> <tmp_path> <platform>"
    echo "Example: ./run-all.sh ~/darshan-install /tmp/test ws"
    exit 1
fi

# set variables for use by other sub-scripts
export DARSHAN_PATH=$1
export DARSHAN_TMP=$2
export DARSHAN_PLATFORM=$3

# check darshan path
if [ ! -x $DARSHAN_PATH/bin/darshan-parser ]; then
    echo "Error: $DARSHAN_PATH doesn't contain a valid Darshan install."
    exit 1
fi

# check and/or create tmp path
if [ ! -d $DARSHAN_TMP ]; then
    mkdir -p $DARSHAN_TMP
fi

if [ ! -d $DARSHAN_TMP ]; then
    echo "Error: unable to find or create $DARSHAN_TMP"
    exit 1
fi
if [ ! -w $DARSHAN_TMP ]; then
    echo "Error: unable to write to $DARSHAN_TMP"
    exit 1
fi

# make sure that we have sub-scripts for the specified platform
if [ ! -d $DARSHAN_PLATFORM ]; then
    echo "Error: unable to find scripts for platform $DARSHAN_PLATFORM"
    exit 1
fi

exit 0
