See README.txt for general instructions.  This file contains notes for testing on the Cray platform @ the ALCF 
(more specifically: theta.alcf.anl.gov).  This example assumes that you are using the Cray module
method to add instrumentation.

To run regression tests:

- unload any existing darshan module in the environment and switch to gnu compilers
  module unload darshan
  module switch PrgEnv-intel PrgEnv-gnu

- compile and install both darshan-runtime and darshan-util in the same directory
  examples:

  # darshan runtime
  ../configure --with-mem-align=64 --with-log-path=/projects/SSSPPg/carns/darshan-logs --prefix=/home/carns/working/darshan/install-theta --with-jobid-env=COBALT_JOBID --disable-cuserid --host=x86_64 CC=cc
  make install

  # darshan util
  ../configure --prefix=/home/carns/working/darshan/install-theta
  make install

- start a screen session by running "screen"
  note: this is suggested because the tests may take a while to complete depending on scheduler 
  availability

- run regression tests
  ./run-all.sh /home/carns/working/darshan/install-cetus /projects/SSSPPg/carns/darshan-test cray-module-alcf

