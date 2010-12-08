#!/bin/bash

# notes:
# - install darshan to /home/carns/working/darshan/install
# - edit maint/generate-bg-compilers.sh to use that path
# - run generate-bg-compilers.sh
# - rm -rf out
# - run this script

compile_examples() {
    if [ -z $1 ]
    then
        echo "No parameters passed to function."
        exit 1
    fi
    if [ -z $2 ]
    then
        echo "Not enough parameters passed to function."
        exit 1
    fi

    PATH=/home/carns/working/darshan/install/bin/$1:$PATH

    mpicc $CFLAGS $LDFLAGS $TEST_PROG_DIR/mpi-io-test.c $LIBS -o $2/mpi-io-test.mpicc.$1
    mpixlc $CFLAGS $LDFLAGS $TEST_PROG_DIR/mpi-io-test.c $LIBS -o $2/mpi-io-test.mpxlc.$1
    mpixlc_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/mpi-io-test.c $LIBS -o $2/mpi-io-test.mpxlc_r.$1

    mpicxx $CFLAGS $LDFLAGS $TEST_PROG_DIR/cxxpi.cxx $LIBS -o $2/cxxpi.mpicxx.$1
    mpixlcxx $CFLAGS $LDFLAGS $TEST_PROG_DIR/cxxpi.cxx $LIBS -o $2/cxxpi.mpxlcxx.$1
    mpixlcxx_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/cxxpi.cxx $LIBS -o $2/cxxpi.mpxlcxx_r.$1

    mpif77 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $FLIBS -o $2/fperf.mpif77.$1
    mpixlf77 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $FLIBS -o $2/fperf.mpxlf77.$1
    mpixlf77_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $FLIBS -o $2/fperf.mpxlf77_r.$1

    mpif90 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $FLIBS -o $2/fperf.mpif90.$1
    mpixlf90 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $FLIBS -o $2/fperf.mpxlf90.$1
    mpixlf90_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $FLIBS -o $2/fperf.mpxlf90_r.$1

    mpixlf95 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $FLIBS -o $2/fperf.mpxlf95.$1
    mpixlf95_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $FLIBS -o $2/fperf.mpxlf95_r.$1

    mpixlf2003 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $FLIBS -o $2/fperf.mpxlf2003.$1
    mpixlf2003_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $FLIBS -o $2/fperf.mpxlf2003_r.$1
}


mkdir -p out
mkdir -p out/normal
mkdir -p out/fpmpi
TEST_PROG_DIR=/home/carns/working/darshan-examples

# tests with darshan enabled
CFLAGS="-g"
LDFLAGS=""
LIBS=""
FLIBS=""
compile_examples default out/normal
compile_examples fast out/normal
compile_examples xl out/normal

# tests with another pmpi library taking precedence
CFLAGS="-g"
LDFLAGS="-L/home/carns/working/fpmpi/build/lib"
LIBS="-lfpmpi"
FLIBS="-lfmpich.cnk -lfpmpi"
compile_examples default out/fpmpi
compile_examples fast out/fpmpi
compile_examples xl out/fpmpi

for i in `ls out/normal`; do echo $i; nm out/normal/$i |grep -c darshan; done
for i in `ls out/fpmpi`; do echo $i; nm out/fpmpi/$i |grep -c darshan; done
