#!/bin/bash

PREFIX=/soft/apps/darshan-x.y.z
LOGDIR=/pvfs-surveyor/logs/darshan

# Configure, Make and Install Darshan suite
./configure --with-mem-align=16 --with-log-path=$LOGDIR --prefix=$PREFIX --with-jobid-env=COBALT_JOBID CFLAGS="-O2" && make && make install

