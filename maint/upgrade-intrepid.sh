#!/bin/bash

PREFIX=/soft/apps/darshan-2.0.0-pre1
LOGDIR=/pvfs-surveyor/darshan_logs

#darshan 
cd ../
./configure --with-mem-align=16 --with-log-path=$LOGDIR --prefix=$PREFIX --with-zlib-for-mpi=/soft/apps/zlib-1.2.3/ CFLAGS="-O2" && make && make install

# log dir already exists
#mkdir -p $LOGDIR
#$PREFIX/bin/darshan-mk-log-dirs.pl

