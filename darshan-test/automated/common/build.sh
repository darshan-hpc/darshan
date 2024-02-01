#!/bin/bash
#
# Build darshan runtime and util code
#

cd build/darshan-runtime
make
make install

cd ../../build/darshan-util
make
make install

cd ../../
