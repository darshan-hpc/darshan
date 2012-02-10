#!/bin/bash

PREFIX=/soft/apps/darshan-x.y.z
LOGDIR=/pvfs-surveyor/logs/darshan

#darshan 
cd ../
./configure --prefix=$PREFIX CFLAGS="-O2" && make && make install
