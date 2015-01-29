#!/bin/bash

$DARSHAN_CC test-cases/src/mpi-io-test.c -o $DARSHAN_TMP/mpi-io-test
if [ $? != 0 ]; then
    echo "Error: failed to compile mpi-io-test" 1>&2
    exit 1
fi

exit 0
