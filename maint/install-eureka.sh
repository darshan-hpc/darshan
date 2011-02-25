#!/bin/bash

PREFIX=/soft/apps/darshan-2.1.0
LOGDIR=/intrepid-fs0/logs/darshan/

# Configure, Make and Install Darshan suite
./configure --with-mem-align=16 --with-log-path=$LOGDIR --prefix=$PREFIX --with-jobid-env=COBALT_JOBID CFLAGS="-O2" && make && make install

#gnuplot (new version with additional histogram support)
#cd extern
#tar -xvzf gnuplot-4.2.4.tar.gz
#cd gnuplot-4.2.4
./configure --prefix $PREFIX && make &&  make install

