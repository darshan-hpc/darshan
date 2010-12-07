#!/bin/bash

PREFIX=/soft/apps/darshan-2.0.2
LOGDIR=/intrepid-fs0/logs/darshan/

#darshan 
cd ../
./configure --with-mem-align=16 --with-log-path=$LOGDIR --prefix=$PREFIX --with-zlib-for-mpi=/soft/apps/zlib-1.2.3/ --with-jobid-env=COBALT_JOBID CFLAGS="-O2" && make && make install

# log dir already exists
#mkdir -p $LOGDIR
#$PREFIX/bin/darshan-mk-log-dirs.pl

