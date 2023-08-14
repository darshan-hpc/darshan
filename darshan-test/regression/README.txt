This directory contains regression tests for both the runtime and util
components of Darshan, assuming that Darshan is already compiled and
installed in a known path.

The master script must be executed with three arguments:

1) path to darshan installation
2) path to temporary directory (for building executables, collecting logs, 
   etc. during test)
3) platform type; options include:
   - workstation-cc-wrapper (for static/dynamic instrumentation on a standard
     workstation using Darshan compiler wrappers)
   - workstation-profile-conf-static (for static instrumentation using MPI
     profiling configuration hooks on a standard workstation)
   - workstation-profile-conf-dynamic (for dynamic instrumentation using MPI
     profiling configuration hooks on a standard workstation)
   - workstation-ld-preload (for dynamic instrumentation via LD_PRELOAD on a
     standard workstation)
   - cray-module-alcf-theta (for instrumentation using a Darshan
     Cray module on the Theta system @ ALCF only)
   - cray-module-alcf-polaris (for instrumentation using a Darshan
     Cray module on the Polaris system @ ALCF only)
   - cray-module-nersc-perlmutter (for instrumentation using a Darshan
     Cray module on the Perlmutter system @ NERSC only)
   - cray-module-olcf-frontier (for instrumentation using a Darshan
     Cray module on the Frontier system @ OLCF only)
   - cray-module-olcf-crusher (for instrumentation using a Darshan
     Cray module on the Crusher system @ OLCF only)

The platform type should map to a subdirectory containing scripts
that describe how to perform platform-specific tasks (like loading or
generating darshan wrappers and executing jobs).

