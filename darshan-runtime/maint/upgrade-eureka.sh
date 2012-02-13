#!/bin/bash

PREFIX=/soft/apps/darshan-x.y.z
LOGDIR=/intrepid-fs0/logs/darshan-eureka

# Configure, Make and Install Darshan suite
cd ../
./configure --with-mem-align=16 --with-log-path=$LOGDIR --prefix=$PREFIX --with-jobid-env=COBALT_JOBID CFLAGS="-O2" CC=/soft/apps/mpich2-1.3.1-gnu/bin/mpicc && make && make install
