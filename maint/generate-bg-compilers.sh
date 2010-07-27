#!/bin/bash
#
# Generates compiler scripts for all BG compilers
#

BGPATH=/bgsys/drivers/ppcfloor/comm/xxx/bin
INSTALL=/soft/apps/darshan-2.0.0/bin

for compiler_type in default fast xl;
do
   compiler_path=${BGPATH/xxx/$type}
   for compiler in $(ls $compiler_path);
   do
       if [ $compiler != "mpich2version" -a $compiler != "parkill" ]; then
           ../darshan-gen-cc.pl --output=$INSTALL/$compiler_type/$compiler $compiler_path/$compiler"
       fi
   done 
done
