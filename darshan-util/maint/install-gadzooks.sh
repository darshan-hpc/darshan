#!/bin/bash

PREFIX=/soft/apps/darshan-x.y.z
LOGDIR=/pvfs-surveyor/logs/darshan

# Configure, Make and Install Darshan suite
cd ../
./configure --prefix=$PREFIX CFLAGS="-O2" && make && make install
