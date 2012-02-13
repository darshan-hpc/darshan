#!/bin/bash

PREFIX=/soft/apps/darshan-x.y.z
LOGDIR=/intrepid-fs0/logs/darshan/

#darshan 
cd ../
./configure --disable-ld-preload --with-mem-align=16 --with-log-path=$LOGDIR --prefix=$PREFIX --with-zlib=/soft/apps/zlib-1.2.3/ --with-jobid-env=COBALT_JOBID CFLAGS="-O2" CC=/bgsys/drivers/ppcfloor/comm/default/bin/mpicc && make && make install

mkdir -p $LOGDIR
$PREFIX/bin/darshan-mk-log-dirs.pl
