#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: run-all.sh <darshan_install_path> <tmp_path> <platform>" 1>&2
    echo "Example: ./run-all.sh ~/darshan-install /tmp/test ws" 1>&2
    exit 1
fi

# set variables for use by other sub-scripts
export DARSHAN_PATH=$1
export DARSHAN_TMP=$2
export DARSHAN_PLATFORM=$3
# number of procs that most test jobs will use
export DARSHAN_DEFAULT_NPROCS=4

DARSHAN_TESTDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
export DARSHAN_TESTDIR

# check darshan path
if [ ! -x $DARSHAN_PATH/bin/darshan-parser ]; then
    echo "Error: $DARSHAN_PATH doesn't contain a valid Darshan install." 1>&2
    exit 1
fi

# check and/or create tmp path
if [ ! -d $DARSHAN_TMP ]; then
    mkdir -p $DARSHAN_TMP
fi

if [ ! -d $DARSHAN_TMP ]; then
    echo "Error: unable to find or create $DARSHAN_TMP" 1>&2
    exit 1
fi
if [ ! -w $DARSHAN_TMP ]; then
    echo "Error: unable to write to $DARSHAN_TMP" 1>&2
    exit 1
fi

# make sure that we have sub-scripts for the specified platform
if [ ! -d $DARSHAN_TESTDIR/$DARSHAN_PLATFORM ]; then
    echo "Error: unable to find scripts for platform $DARSHAN_PLATFORM" 1>&2
    exit 1
fi

# set up environment for tests according to platform
source $DARSHAN_TESTDIR/$DARSHAN_PLATFORM/env.sh

failure_count=0

for i in `ls $DARSHAN_TESTDIR/test-cases/*.sh`; do
    echo Running ${i}...
    $i
    if [ $? -ne 0 ]; then
        echo "Error: failed to execute test case $i"
	failure_count=$((failure_count+1))
    fi
    echo Done.
done

if [ "$failure_count" -eq 0 ]; then
	exit 0
else
	echo $failure_count tests failed
	exit 1
fi
