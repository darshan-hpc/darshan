#!/bin/bash

# General notes
#######################
# Script to set up the C++ compiler to use for subsequent test cases.  This
# script may load optional modules (as in a Cray PE), set LD_PRELOAD
# variables (as in a dynamically linked environment), or generate mpicxx
# wrappers (as in a statically linked environment).

# The script should produce a single string to stdout, which is the command
# line to use for invoking the C compiler

# Notes specific to this platform (ws)
########################
# This particular version of the setup-cxx script assumes that mpicxx is
# present in the path already, and that the C++ compiler to use for
# subsequent tests should be generated from this using darshan-gen-cxx.pl.

$DARSHAN_PATH/bin/darshan-gen-cxx.pl `which mpicxx` --output $DARSHAN_TMP/mpicxx
if [ $? -ne 0 ]; then
    echo "Error: failed to generate c compiler." 1>&2
    exit 1
fi

echo $DARSHAN_TMP/mpicxx
exit 0
