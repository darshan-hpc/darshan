#!/bin/bash

PREFIX=/soft/apps/unsupported/darshan-1.1.0
LOGDIR=/pvfs-surveyor/darshan_logs

#darshan 
cd ../
./configure --with-mem-align=16 --with-log-path=$LOGDIR --prefix=$PREFIX --with-zlib-for-mpi=/soft/apps/zlib-1.2.3/ CFLAGS="-O2" && make && make install

# log dir already exists
#mkdir -p $LOGDIR
#$PREFIX/bin/darshan-mk-log-dirs.pl

