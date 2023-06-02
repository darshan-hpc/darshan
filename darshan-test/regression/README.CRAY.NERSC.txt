See README.txt for general instructions.  This file contains notes for testing on Cray platforms @ NERSC
(more specifically: perlmutter.nersc.gov).  This example assumes that you are using the Cray module
method to add instrumentation.

To run regression tests:

- unload any existing darshan module in the environment and switch to gnu compilers
  module unload darshan
  module switch PrgEnv-intel PrgEnv-gnu

- compile and install both darshan-runtime and darshan-util in the same directory
  examples:

  # darshan runtime
  ../configure --with-mem-align=8 --with-log-path=/pscratch/sd/s/ssnyder/darshan-logs --prefix=/global/homes/s/ssnyder/working/darshan/install --with-jobid-env=SLURM_JOB_ID --disable-cuserid CC=cc
  make install

  # darshan util
  ../configure --prefix=/global/homes/s/ssnyder/working/darshan/install
  make install

- start a screen session by running "screen"
  note: this is suggested because the tests may take a while to complete depending on scheduler 
  availability

- run regression tests
  ./run-all.sh /global/homes/s/ssnyder/working/darshan/install /pscratch/sd/s/ssnyder/darshan-test cray-module-nersc-perlmutter

