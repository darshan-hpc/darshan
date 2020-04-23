#!/bin/bash

# General notes
#######################

# Script to set up the environment for tests on this platform.  Must export
# the following environment variables:
# 
# DARSHAN_CC: command to compile C programs
# DARSHAN_CXX: command to compile C++ programs
# DARSHAN_F90: command to compile Fortran90 programs
# DARSHAN_F77: command to compile Fortran77 programs
# DARSHAN_RUNJOB: command to execute a job and wait for its completion

# This script may load optional modules (as in a Cray PE), set LD_PRELOAD
# variables (as in a dynamically linked environment), or generate mpicc
# wrappers (as in a statically linked environment).

# Notes specific to this platform (workstation-cc-wrapper)
########################
# This particular env script assumes that mpicc and its variants for other 
# languages are already in the path.  The compiler scripts to be used in
# these test cases will be generated using darshan-gen-*.pl scripts.

# The runjob command is just mpiexec, no scheduler

$DARSHAN_PATH/bin/darshan-gen-cc.pl `which mpicc` --output $DARSHAN_TMP/mpicc
if [ $? -ne 0 ]; then
    echo "Error: failed to generate c compiler." 1>&2
    exit 1
fi
export DARSHAN_CC=$DARSHAN_TMP/mpicc

$DARSHAN_PATH/bin/darshan-gen-cxx.pl `which mpicxx` --output $DARSHAN_TMP/mpicxx
if [ $? -ne 0 ]; then
    echo "Error: failed to generate c compiler." 1>&2
    exit 1
fi
export DARSHAN_CXX=$DARSHAN_TMP/mpicxx

$DARSHAN_PATH/bin/darshan-gen-fortran.pl `which mpif77` --output $DARSHAN_TMP/mpif77
if [ $? -ne 0 ]; then
    echo "Error: failed to generate f77 compiler." 1>&2
    exit 1
fi
export DARSHAN_F77=$DARSHAN_TMP/mpif77

$DARSHAN_PATH/bin/darshan-gen-fortran.pl `which mpif90` --output $DARSHAN_TMP/mpif90
if [ $? -ne 0 ]; then
    echo "Error: failed to generate f90 compiler." 1>&2
    exit 1
fi
export DARSHAN_F90=$DARSHAN_TMP/mpif90

export DARSHAN_RUNJOB="mpiexec -n $DARSHAN_DEFAULT_NPROCS"
