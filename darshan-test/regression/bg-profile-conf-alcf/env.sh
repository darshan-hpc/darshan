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

# Notes specific to this platform (bg-profile-conf-alcf)
########################
# This particular env script assumes that mpicc and its variants for other 
# languages are already in the path, and that they will produce static 
# executables by default.  Darshan instrumentation is added by specifying
# a profiling configuration file using environment variables.

# the RUNJOB command is the most complex part here.  We use a script that submits
# a cobalt job, waits for its completion, and checks its return status

export DARSHAN_CC=mpicc
export DARSHAN_CXX=mpicxx
export DARSHAN_F77=mpif77
export DARSHAN_F90=mpif90

export MPICC_PROFILE=$DARSHAN_PATH/share/mpi-profile/darshan-bg-cc
export MPICXX_PROFILE=$DARSHAN_PATH/share/mpi-profile/darshan-bg-cxx
export MPIF90_PROFILE=$DARSHAN_PATH/share/mpi-profile/darshan-bg-f
export MPIF77_PROFILE=$DARSHAN_PATH/share/mpi-profile/darshan-bg-f

export DARSHAN_RUNJOB="bg-profile-conf-alcf/runjob.sh"
