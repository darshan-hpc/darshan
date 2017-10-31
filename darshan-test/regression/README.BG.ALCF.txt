See README.txt for general instructions.  This file contains notes for testing on the Blue Gene platform 
(more specifically: cetus.alcf.anl.gov).  This example assumes that you are using the MPICH profile conf
method to add instrumentation.

To run regression tests:

- compile and install both darshan-runtime and darshan-util in the same directory
  examples:

  # darshan runtime
  ../configure --with-mem-align=16 --with-log-path=/projects/SSSPPg/carns/darshan-logs --prefix=/home/carns/working/darshan/install-cetus --with-jobid-env=COBALT_JOBID --with-zlib=/soft/libraries/alcf/current/gcc/ZLIB --host=powerpc-bgp-linux CC=/bgsys/drivers/V1R2M2/ppc64/comm/bin/gcc/mpicc
  make install

  # darshan util
  ../configure --prefix=/home/carns/working/darshan/install-cetus
  make install

- start a screen session by running "screen"
  note: this is suggested because the tests may take a while to complete depending on scheduler 
  availability

- within the screen session, set your path to point to a stock set of MPI compiler scripts
  export PATH=/bgsys/drivers/V1R2M2/ppc64/comm/bin/gcc:$PATH

- run regression tests
  ./run-all.sh /home/carns/working/darshan/install-cetus /projects/SSSPPg/carns/darshan-test bg-profile-conf-alcf
  note: the f90 test is expected to fail due to a known problem in the profiling interface for the 
    F90 MPICH implementation on Mira.

