#!/bin/bash
#
#  Run the test program through Valgrind to expose memory leaks and buffer
#  overflows on a variety of different file locations and geometries
#

### Make some files to test.  Assume $SCRATCH points at Lustre
for stripe in 1 2 4 8 16 32
do
    if [ ! -f $SCRATCH/stripe${stripe} ]; then
        lfs setstripe -c $stripe $SCRATCH/stripe${stripe}
    fi
done

set -x

valgrind --tool=memcheck \
         --leak-check=yes \
         --show-reachable=yes \
         --num-callers=20 \
         --track-fds=yes \
         --read-var-info=yes \
         ./darshan-tester \
         $SCRATCH/stripe4 \
         $SCRATCH/stripe32 \
         $SCRATCH/stripe1 \
         $SCRATCH/stripe16 \
         $SCRATCH/stripe8 \
         $HOME/.bashrc \
         $SCRATCH/stripe2

set +x
