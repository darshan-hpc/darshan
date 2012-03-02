#!/bin/bash
#
# Create  build, install and log directories.
#

status=0

mkdir -p install
status=$((status + $?))

mkdir -p logs
status=$((status + $?))

mkdir -p build/darshan-runtime
status=$((status + $?))

mkdir -p build/darshan-utils
status=$((status + $?))

exit $status
