#!/bin/bash

PREFIX=/soft/apps/darshan-x.y.z
LOGDIR=/intrepid-fs0/logs/darshan/

# Configure, Make and Install Darshan suite
cd ..
./configure --prefix=$PREFIX CFLAGS="-O2" && make && make install
