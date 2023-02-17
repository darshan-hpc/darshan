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

# Notes specific to this platform (cray-module-olcf-crusher)
########################
# Use Cray's default compiler wrappers and LD_PRELOAD the darshan library
# associated with this install
#
# RUNJOB is responsible for submitting a Slurm job, waiting for its
# completion, and checking its return status

export DARSHAN_CC=cc
export DARSHAN_CXX=CC
export DARSHAN_F77=ftn
export DARSHAN_F90=ftn

export DARSHAN_RUNJOB=$DARSHAN_TESTDIR/$DARSHAN_PLATFORM/runjob.sh

export LD_PRELOAD=$DARSHAN_RUNTIME_PATH/lib/libdarshan.so:$LD_PRELOAD
