## Quick Start

    $ cd darshan-serial/darshan-runtime
    $ autoreconf -ivf
    $ ./configure --with-log-path-by-env=PWD --with-jobid-env=RANDOM --with-mem-align=8 --disable-mpiio-mod --disable-stdio-mod --disable-pnetcdf-mod --disable-dxt-mod --disable-bgq-mod --disable-lustre-mod --without-mpi CC=gcc
    $ make

This will install serial Darshan in darshan-serial/install.  Then to use it,

    $ LD_PRELOAD=/path/to/darshan-serial/darshan-runtime/lib/libdarshan.so DARSHAN_ENABLE_NONMPI=1 ./path/to/my.exe

Your `my.exe` will run and create a Darshan log in `$PWD`.  Regular old
`darshan-parser` can be used to parse the logfile.

## Non-MPI Darshan

This repository contains a version of Darshan that supports profiling in the
absence of MPI at either runtime or compile time.  It can be built in two ways:

1. **With MPI**, but capable of profiling non-MPI applications if linked against
   them.  This is useful if a single build should work with both MPI and non-MPI
   applications.
2. **Without MPI** and capable of only profiling non-MPI applications.  This is
   useful for building Darshan on systems that simply don't have MPI at all.

At present, mode #2 only supports the POSIX module.  All other modules should be
disabled at configure-time using:

    ./configure --disable-mpiio-mod \
                --disable-stdio-mod \
                --disable-pnetcdf-mod \
                --disable-bgq-mod \
                --disable-lustre-mod \
                --disable-dxt-mod \
                --without-mpi \
                ...

Then to profile an application,

    LD_PRELOAD=$PWD/lib/libdarshan.so DARSHAN_ENABLE_NONMPI=1 ./path/to/my.exe

Do _not_ define `DARSHAN_ENABLE_NONMPI=1` globally, as this will create Darshan
logs for _everything_ including basic commands like `ls`.
