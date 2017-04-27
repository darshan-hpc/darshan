This directory contains regression tests for both the runtime and util
components of Darshan, assuming that Darshan is already compiled and
installed in a known path.

The master script must be executed with three arguments:

1) path to darshan installation
2) path to temporary directory (for building executables, collecting logs, 
   etc. during test)
3) platform type; options include:
   - workstation-static (for static instrumentation on a standard workstation)
   - workstation-dynamic (for dynamic instrumentation on a standard workstation)
   - workstation-profile-conf (for static instrumentation using MPI profiling
     configuration hooks on a standard workstation)
   - bg-profile-conf-alcf (for static instrumentation using MPI profiling configuration
     hooks on BGQ platforms @ the ALCF only)
   - cray-module-alcf (for static instrumentation using a Darshan Cray module on
     Cray systems @ the ALCF only)

The platform type should map to a subdirectory containing scripts
that describe how to perform platform-specific tasks (like loading or
generating darshan wrappers and executing jobs).

