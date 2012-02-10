#!/bin/bash

PREFIX=/soft/apps/darshan-x.y.z
LOGDIR=/intrepid-fs0/logs/darshan/

#darshan 
cd ../
./configure --prefix=$PREFIX CFLAGS="-O2" && make && make install

