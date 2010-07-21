#!/bin/bash

mkdir -p out
TEST_PROG_DIR=/home/carns/working/darshan-examples

CFLAGS="-g"
LDFLAGS=""
LIBS=""
PATH=/home/carns/working/darshan/install/bin/fast:$PATH

mpicc $CFLAGS $LDFLAGS $TEST_PROG_DIR/mpi-io-test.c $LIBS -o out/mpi-io-test.mpicc.fast
mpixlc $CFLAGS $LDFLAGS $TEST_PROG_DIR/mpi-io-test.c $LIBS -o out/mpi-io-test.mpxlc.fast
mpixlc_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/mpi-io-test.c $LIBS -o out/mpi-io-test.mpxlc_r.fast

mpicxx $CFLAGS $LDFLAGS $TEST_PROG_DIR/cxxpi.cxx $LIBS -o out/cxxpi.mpicxx.fast
mpixlcxx $CFLAGS $LDFLAGS $TEST_PROG_DIR/cxxpi.cxx $LIBS -o out/cxxpi.mpxlcxx.fast
mpixlcxx_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/cxxpi.cxx $LIBS -o out/cxxpi.mpxlcxx_r.fast

mpif77 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpif77.fast
mpixlf77 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf77.fast
mpixlf77_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf77_r.fast

mpif90 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpif90.fast
mpixlf90 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf90.fast
mpixlf90_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf90_r.fast

mpixlf95 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf95.fast
mpixlf95_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf95_r.fast

mpixlf2003 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf2003.fast
mpixlf2003_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf2003_r.fast


CFLAGS="-g"
LDFLAGS="-L/home/carns/working/fpmpi/build/lib"
LIBS="-lfpmpi"
PATH=/home/carns/working/darshan/install/bin/fast:$PATH

mpicc $CFLAGS $LDFLAGS $TEST_PROG_DIR/mpi-io-test.c $LIBS -o out/mpi-io-test.mpicc.fpmpi.fast
mpixlc $CFLAGS $LDFLAGS $TEST_PROG_DIR/mpi-io-test.c $LIBS -o out/mpi-io-test.mpxlc.fpmpi.fast
mpixlc_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/mpi-io-test.c $LIBS -o out/mpi-io-test.mpxlc_r.fpmpi.fast

mpicxx $CFLAGS $LDFLAGS $TEST_PROG_DIR/cxxpi.cxx $LIBS -o out/cxxpi.mpicxx.fpmpi.fast
mpixlcxx $CFLAGS $LDFLAGS $TEST_PROG_DIR/cxxpi.cxx $LIBS -o out/cxxpi.mpxlcxx.fpmpi.fast
mpixlcxx_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/cxxpi.cxx $LIBS -o out/cxxpi.mpxlcxx_r.fpmpi.fast

LIBS="-lfmpich.cnk -lfpmpi"

mpif77 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpif77.fpmpi.fast
mpixlf77 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf77.fpmpi.fast
mpixlf77_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf77_r.fpmpi.fast

mpif90 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpif90.fpmpi.fast
mpixlf90 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf90.fpmpi.fast
mpixlf90_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf90_r.fpmpi.fast

mpixlf95 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf95.fpmpi.fast
mpixlf95_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf95_r.fpmpi.fast

mpixlf2003 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf2003.fpmpi.fast
mpixlf2003_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf2003_r.fpmpi.fast


CFLAGS="-g"
LDFLAGS=""
LIBS=""
PATH=/home/carns/working/darshan/install/bin:$PATH

mpicc $CFLAGS $LDFLAGS $TEST_PROG_DIR/mpi-io-test.c $LIBS -o out/mpi-io-test.mpicc
mpixlc $CFLAGS $LDFLAGS $TEST_PROG_DIR/mpi-io-test.c $LIBS -o out/mpi-io-test.mpxlc
mpixlc_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/mpi-io-test.c $LIBS -o out/mpi-io-test.mpxlc_r

mpicxx $CFLAGS $LDFLAGS $TEST_PROG_DIR/cxxpi.cxx $LIBS -o out/cxxpi.mpicxx
mpixlcxx $CFLAGS $LDFLAGS $TEST_PROG_DIR/cxxpi.cxx $LIBS -o out/cxxpi.mpxlcxx
mpixlcxx_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/cxxpi.cxx $LIBS -o out/cxxpi.mpxlcxx_r

mpif77 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpif77
mpixlf77 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf77
mpixlf77_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf77_r

mpif90 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpif90
mpixlf90 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf90
mpixlf90_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf90_r

mpixlf95 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf95
mpixlf95_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf95_r

mpixlf2003 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf2003
mpixlf2003_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf2003_r


CFLAGS="-g"
LDFLAGS="-L/home/carns/working/fpmpi/build/lib"
LIBS="-lfpmpi"
PATH=/home/carns/working/darshan/install/bin:$PATH

mpicc $CFLAGS $LDFLAGS $TEST_PROG_DIR/mpi-io-test.c $LIBS -o out/mpi-io-test.mpicc.fpmpi
mpixlc $CFLAGS $LDFLAGS $TEST_PROG_DIR/mpi-io-test.c $LIBS -o out/mpi-io-test.mpxlc.fpmpi
mpixlc_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/mpi-io-test.c $LIBS -o out/mpi-io-test.mpxlc_r.fpmpi

mpicxx $CFLAGS $LDFLAGS $TEST_PROG_DIR/cxxpi.cxx $LIBS -o out/cxxpi.mpicxx.fpmpi
mpixlcxx $CFLAGS $LDFLAGS $TEST_PROG_DIR/cxxpi.cxx $LIBS -o out/cxxpi.mpxlcxx.fpmpi
mpixlcxx_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/cxxpi.cxx $LIBS -o out/cxxpi.mpxlcxx_r.fpmpi

LIBS="-lfmpich.cnk -lfpmpi"

mpif77 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpif77.fpmpi
mpixlf77 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf77.fpmpi
mpixlf77_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf77_r.fpmpi

mpif90 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpif90.fpmpi
mpixlf90 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf90.fpmpi
mpixlf90_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf90_r.fpmpi

mpixlf95 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf95.fpmpi
mpixlf95_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf95_r.fpmpi

mpixlf2003 $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf2003.fpmpi
mpixlf2003_r $CFLAGS $LDFLAGS $TEST_PROG_DIR/fperf.f $LIBS -o out/fperf.mpxlf2003_r.fpmpi


for i in `ls out/`; do echo $i; nm out/$i |grep -c darshan; done
