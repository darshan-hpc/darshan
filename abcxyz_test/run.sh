#!/bin/bash


# ensure the sample library libabcxyz.so is found
export LD_LIBRARY_PATH=.

# enable and configure darshan to write logs into current directory
export LD_PRELOAD=$PWD/../darshan-runtime/lib/libdarshan.so
export DARSHAN_LOG_DIR_PATH=$PWD


# execute the example application
./abcxyz
