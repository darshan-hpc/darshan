#!/bin/bash
#
# Generates compiler scripts for all BG compilers
#

PREFIX=/soft/apps/darshan-x.x.x
BGPATH=/bgsys/drivers/ppcfloor/comm/xxx/bin

for compiler_type in default fast xl;
do
   compiler_path=${BGPATH/xxx/$compiler_type}
   compiler_opt=""
   for compiler in $(ls $compiler_path);
   do
       if [ $compiler != "mpich2version" -a \
            $compiler != "parkill" ]; then
           mkdir -p $PREFIX/bin/$compiler_type
           if [ $(expr match $compiler ".*xl.*") -gt 0 -o \
                $compiler_type = "fast" -o \
                $compiler_type = "xl" ]; then
               compiler_opt="--trim --xl";
           fi
           if [ $(expr match $compiler ".*cxx") -gt 0 ]; then
               $PREFIX/bin/darshan-gen-cxx.pl $compiler_opt --output=$PREFIX/bin/$compiler_type/$compiler $compiler_path/$compiler
           elif [ $(expr match $compiler ".*f77") -gt 0 -o \
                  $(expr match $compiler ".*f90") -gt 0 -o \
                  $(expr match $compiler ".*f95") -gt 0 -o \
                  $(expr match $compiler ".*f2003") -gt 0 ]; then
               $PREFIX/bin/darshan-gen-fortran.pl $compiler_opt --output=$PREFIX/bin/$compiler_type/$compiler $compiler_path/$compiler
           else
               $PREFIX/bin/darshan-gen-cc.pl $compiler_opt --output=$PREFIX/bin/$compiler_type/$compiler $compiler_path/$compiler
           fi
       fi
   done 
done
