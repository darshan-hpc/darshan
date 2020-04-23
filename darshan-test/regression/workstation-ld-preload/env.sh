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

# Notes specific to this platform (workstation-ld-preload)
########################
# This particular env script assumes that mpicc and its variants for other 
# languages are already in the path, and that they will produce dynamic
# executables by default.  Test programs are compiled using the existing
# scripts, and LD_PRELOAD is set to enable instrumentation.

# The runjob command is just mpiexec, no scheduler

export DARSHAN_CC=mpicc
export DARSHAN_CXX=mpicxx
export DARSHAN_F77=mpif77
export DARSHAN_F90=mpif90
FULL_MPICC_PATH=`which mpicc`

# This is a hack.  In order to instrument Fortran programs with LD_PRELOAD,
# we must prepend libfmpich.so to the LD_PRELOAD variable, but with a fully
# resolve path.  To find a path we locate mpicc and speculate that
# libfmich.so can be found in ../lib.
export LD_PRELOAD=`dirname $FULL_MPICC_PATH`/../lib/libfmpich.so:$DARSHAN_PATH/lib/libdarshan.so:$LD_PRELOAD

export DARSHAN_RUNJOB="mpiexec -n $DARSHAN_DEFAULT_NPROCS"

# in case of using LD_PRELOAD, it is possible errors with Darshan symbols
# (e..g, forgetting to resolve a symbol Darshan wraps using dlsym) can
# cause arbitrary binaries to crash. We check the output of the true
# command and ls to try to identify failures like this and exit
true_out=`true`
if [ $? -ne 0 -o -n "$true_out" ]; then
    echo -n "$true_out"
    echo "true command test failed"
    exit 1
fi
ls_out=`ls 1>/dev/null`
if [ $? -ne 0 -o -n "$ls_out" ]; then
    echo -n "$ls_out"
    echo "ls command test failed"
    exit 1
fi
