#!/bin/bash

# General notes
#######################
# Script to set up the F90 compiler to use for subsequent test cases.  This
# script may load optional modules (as in a Cray PE), set LD_PRELOAD
# variables (as in a dynamically linked environment), or generate mpicc
# wrappers (as in a statically linked environment).

# The script should produce a single string to stdout, which is the command
# line to use for invoking the F90 compiler

# Notes specific to this platform (ws)
########################
# This particular version of the setup-cc script assumes that mpicc is
# present in the path already, and that the F90 compiler to use for
# subsequent tests should be generated from this using darshan-gen-fortran.pl.

$DARSHAN_PATH/bin/darshan-gen-fortran.pl `which mpif90` --output $DARSHAN_TMP/mpif90
if [ $? -ne 0 ]; then
    echo "Error: failed to generate f90 compiler." 1>&2
    exit 1
fi

echo $DARSHAN_TMP/mpif90
exit 0
