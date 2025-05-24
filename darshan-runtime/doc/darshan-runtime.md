# Darshan-runtime installation and usage

Table of Contents

0. [Introduction](#sec_intro)
1. [Requirements](#sec_req)
2. [Conventional installation](#sec_install)
   1. [Compilation](#sec_compile)
   2. [Environment preparation](#sec_env)
3. [Spack installation](#sec_spack)
4. [Instrumenting applications](#sec_instrumenting)
   1. [Option 1: Instrumenting MPI applications at compile time](#sec_option1)
   2. [Option 2: Instrumenting MPI applications at runtime](#sec_option2)
   3. [Option 3: Instrumenting non-MPI applications at runtime](#sec_option3)
   4. [Using other profiling tools at the same time as Darshan](#sec_profile_tools)
5. [Using the Darshan eXtended Tracing (DXT) module](#sec_dxt)
6. [Using AutoPerf instrumentation modules](#sec_autoperf)
7. [Configuring Darshan library at runtime](#sec_config_runtime)
   1. [Darshan library config settings](#sec_config_settings)
   2. [Example Darshan configuration](#sec_config_example)
8. [Darshan installation recipes](#sec_recipes)
   1. [Cray platforms (XE, XC, or similar)](#sec_recipes_cray)
   2. [Linux clusters using MPICH](#sec_recipes_mpich)
   3. [Linux clusters using Intel MPI](#sec_recipes_intel_mpi)
   4. [Linux clusters using Open MPI](#sec_recipes_open_mpi)
9. [Debugging](#sec_debug)
   1. [No log file](#sec_no_log)


## 0. Introduction
<a name="sec_intro"></a>

This document describes `darshan-runtime`, which is the instrumentation
portion of the Darshan characterization tool.  It should be installed on the
system where you intend to collect I/O characterization information.

Darshan instruments applications via either compile time wrappers or
dynamic library preloading.  An application that has been instrumented
with Darshan will produce a single log file each time it is executed.
This log summarizes the I/O access patterns used by the application.

The `darshan-runtime` instrumentation has traditionally only supported MPI
applications (specifically, those that call `MPI_Init()` and `MPI_Finalize()`),
but, as of version 3.2.0, Darshan also supports instrumentation of non-MPI
applications. Regardless of whether MPI is used, Darshan provides detailed
statistics about POSIX level file accesses made by the application.
In the case of MPI applications, Darshan additionally captures details on MPI-IO
and HDF5 level access, as well as limited information about PnetCDF access.
Note that instrumentation of non-MPI applications is currently only supported
in Darshan's shared library, which applications must `LD_PRELOAD`.

Starting in version 3.0.0, Darshan also exposes an API that can be used to develop
and add new instrumentation modules (for other I/O library interfaces or to gather
system-specific data, for instance), as detailed in
[Modularized I/O characterization using Darshan 3.x](http://www.mcs.anl.gov/research/projects/darshan/docs/darshan-modularization.html).
Newly contributed modules include a module for gathering system-specific parameters
for jobs running on BG/Q systems, a module for gathering Lustre striping data for
files on Lustre file systems, and a module for instrumenting stdio (i.e., stream I/O
functions like `fopen()`, `fread()`, etc).

Starting in version 3.1.3, Darshan also allows for full tracing of application I/O
workloads using the newly developed Darshan eXtended Tracing (DxT) instrumentation
module. This module can be selectively enabled at runtime to provide high-fidelity
traces of an application's I/O workload, as opposed to the coarse-grained I/O summary
data that Darshan has traditionally provided. Currently, DxT only traces at the POSIX
and MPI-IO layers. Initial [performance results](./DXT-overhead.pdf) demonstrate the
low overhead of DxT tracing, offering comparable performance to Darshan's traditional
coarse-grained instrumentation methods.

Starting in version 3.4.5, Darshan facilitates real-time collection of comprehensive
application I/O workload traces through the newly integrated Darshan LDMS data module,
known as the `darshanConnector`. Leveraging the Lightweight Distributed Metric Service
(LDMS) streams API, the `darshanConnector` collects, transports and/or stores traces
of application I/O operations instrumented by Darshan at runtime. This module can only
be enabled if the LDMS library is included in the Darshan build process. For more
information about LDMS or LDMS streams please refer to the official
[LDMS documentation](https://ovis-hpc.readthedocs.io/projects/ldms/en/latest/rst_man/index.html).

This document provides generic installation instructions, but "recipes" for
several common HPC systems are provided at the end of the document as well.

More information about Darshan can be found at the
[Darshan web site](http://www.mcs.anl.gov/darshan).

## 1. Requirements
<a name="sec_req"></a>

* C compiler (preferably GCC-compatible)
* zlib development headers and library

## 2. Conventional installation
<a name="sec_install"></a>

### 2.1 Compilation
<a name="sec_compile"></a>

Configure and build example (with MPI support)
```csh
tar -xvzf darshan-<version-number>.tar.gz
cd darshan-<version-number>/
./prepare.sh
cd darshan-runtime/
./configure --with-log-path=/darshan-logs --with-jobid-env=PBS_JOBID CC=mpicc
make
make install
```

Configure and build example (without MPI support)
```csh
tar -xvzf darshan-<version-number>.tar.gz
cd darshan-<version-number>/
./prepare.sh
cd darshan-runtime/
./configure --with-log-path=/darshan-logs --with-jobid-env=PBS_JOBID --without-mpi CC=gcc
make
make install
```

Explanation of configure arguments:
* `--with-mem-align=NUM`: This value is system-dependent and will be used by
  Darshan to determine if the buffer for a read or write operation is aligned
  in memory (default is 8).
* `--with-jobid-env=NAME` (mandatory): this specifies the environment variable
  that Darshan should check to determine the jobid of a job.  Common values are
  `PBS_JOBID` or `COBALT_JOBID`.  If you are not using a scheduler (or your
  scheduler does not advertise the job ID) then you can specify `NONE` here.
  Darshan will fall back to using the pid of the rank 0 process if the
  specified environment variable is not set.
* `--with-username-env=NAME`: this specifies the environment variable that Darshan
  should check to determine the username for a job. If not specified, Darshan
  will use internal mechanisms to try to determine the username.
  * NOTE: Darshan relies on the `LOGNAME` environment variable to determine a username, but this method isn't always reliable (e.g., on Slurm systems, `LOGNAME` can be wiped when specifying additional environment variables using the `--export` option to `srun`). This configure option allows specification of an additional environment variable to extract a username from (e.g., `SLURM_JOB_USER`).
* `--with-log-path=DIR` (this, or `--with-log-path-by-env`, is mandatory): This
  specifies the parent directory for the directory tree where Darshan logs will
  be placed.
  * NOTE: after installation, any user can display the configured path with the `darshan-config --log-path` command
* `--with-log-path-by-env=NAME1,NAME2,...`: specifies a comma separated list of
  environment variables to check at runtime for log path location before the
  one set by `--with-log-path=DIR` at configure time.
* `--with-log-hints=hint1=x;hint2=y,...`: specifies hints to use when writing
  the Darshan log file.  See `./configure --help` for details.
* `--with-mod-mem=NUM`: specifies the maximum amount of memory (in MiB) that
  active Darshan instrumentation modules can collectively consume.
* `--with-zlib=DIR`: specifies an alternate location for the zlib development
  header and library.
* `--without-mpi`: disables MPI support when building Darshan - MPI support is
  assumed if not specified.
* `--enable-mmap-logs`: enables the use of Darshan's mmap log file mechanism.
* `--enable-cuserid`: enables use of cuserid() at runtime.
* `--disable-ld-preload`: disables building of the Darshan `LD_PRELOAD` library
* `--enable-group-readable-logs`: sets Darshan log file permissions to allow
  group read access.
* `--disable-exit-wrapper`: disables wrapping of `_exit()` calls as last ditch
  shutdown hook for the Darshan library when used in non-MPI mode.
* `CC=`: specifies the C compiler to use for compilation.

Configure arguments for controlling which Darshan modules to use:
* `--disable-posix-mod`: disables compilation and use of Darshan's POSIX module
  (default=enabled)
* `--disable-mpiio-mod`: disables compilation and use of Darshan's MPI-IO
  module (default=enabled)
* `--disable-stdio-mod`: disables compilation and use of Darshan's STDIO module
  (default=enabled)
* `--disable-dxt-mod`: disables compilation and use of Darshan's DXT module
  (default=enabled)
* `--enable-hdf5-mod`: enables compilation and use of Darshan's HDF5 module
  (default=disabled)
* `--with-hdf5=DIR`: installation directory for HDF5
  * NOTE: Users must call `--enable-hdf5-mod` to enable HDF5 modules, `--with-hdf5` is only used to additionally provide an HDF5 install prefix.
  * NOTE: HDF5 instrumentation only works on HDF5 library versions >=1.8, and further requires that the HDF5 library used to build Darshan and the HDF5 library being linked in either both be version >=1.10 or both be version <1.10
  * NOTE: This option does not work with the profile configuration instrumentation method described in [Using a profile configuration](#sec_use_profile_config) of Section [Instrumenting applications](#sec_instrumenting).
* `--enable-pnetcdf-mod`: enables compilation and use of Darshan's PnetCDF
  module (default=disabled)
* `--with-pnetcdf=DIR`: installation directory for PnetCDF
  * NOTE: Users must call `--enable-pnetcdf-mod` to enable PnetCDF modules, `--with-pnetcdf` is only used to additionally provide a PnetCDF install prefix.
  * NOTE: PnetCDF instrumentation only works on PnetCDF library versions >=1.8
* `--disable-lustre-mod`: disables compilation and use of Darshan's Lustre
  module (default=enabled)
* `--enable-daos-mod`: enables compilation and use of Darshan's DAOS module
  (default=disabled)
* `--with-daos=DIR`: installation directory for DAOS
  * NOTE: Users must call `--enable-daos-mod` to enable DAOS modules, `--with-daos` is only used to additionally provide a DAOS install prefix.
* `--enable-mdhim-mod`: enables compilation and use of Darshan's MDHIM module
  (default=disabled)
* `--enable-ldms-mod`:  enables compilation and use of Darshan’s LDMS runtime module (default=disabled)
* `--with-ldms=DIR`: installation directory for LDMS
  * NOTE: Users must use the configuration flags `--enable-ldms-mod` and `--with-ldms=DIR` to enable runtime data collection via LDMS.
  * NOTE: To collect runtime I/O information from Darshan, you will need to configure, initialize, and connect to an LDMS streams daemon. For detailed instructions please visit [Running An LDMS Streams Daemon for Darshan](https://ovis-hpc.readthedocs.io/projects/ldms/en/latest/streams/ldms-streams-apps.html#darshan).
  * NOTE: If LDMS is not installed on the system, please visit “Getting the Source” and “Building the Source” in the [LDMS Quick Start Guide](https://ovis-hpc.readthedocs.io/projects/ldms/en/latest/intro/quick-start.html).

### 2.2 Environment preparation
<a name="sec_env"></a>

Once `darshan-runtime` has been installed, you must prepare a location
in which to store the Darshan log files and configure an instrumentation method.

This step can be safely skipped if you configured `darshan-runtime` using the
`--with-log-path-by-env` option.  A more typical configuration uses a static
directory hierarchy for Darshan log
files.

The `darshan-mk-log-dirs.pl` utility will configure the path specified at
configure time to include
subdirectories organized by year, month, and day in which log files will be
placed. The deepest subdirectories will have sticky permissions to enable
multiple users to write to the same directory.  If the log directory is
shared system-wide across many users then the following script should be run
as root.

```csh
darshan-mk-log-dirs.pl
```

#### A note about finding log paths after installation

Regardless of whether a Darshan installation is using the --with-log-path or
--with-log-path-by-env option, end users can display the path (and/or
environment variables) at any time by running `darshan-config --log-path`
on the command line.

#### A note about log directory permissions
All log files written by Darshan have permissions set to only allow
read access by the owner of the file.  You can modify this behavior,
however, by specifying the --enable-group-readable-logs option at
configure time.  One notable deployment scenario would be to configure
Darshan and the log directories to allow all logs to be readable by both the
end user and a Darshan administrators group.   This can be done with the
following steps:

* set the --enable-group-readable-logs option at configure time
* create the log directories with darshan-mk-log-dirs.pl
* recursively set the group ownership of the log directories to the Darshan
administrators group
* recursively set the setgid bit on the log directories

## 3. Spack installation
<a name="sec_spack"></a>

You can also install Darshan via [Spack](https://spack.io) as an alternative
to manual download, compilation, and installation.  This may be
especially convenient for single-user installs.  Darshan is divided
into two separate packages for the command line utilities and runtime
instrumentation.  You can install either or both as follows:

```csh
spack install darshan-util
spack install darshan-runtime
```

#### NOTE
Darshan will generally compile and install fine using a variety of
compilers, but we advise using a gcc compiler in Spack to compile Darshan
(regardless of what compiler you will use for your applications) to
ensure maximum runtime compatibility.

You can use the `spack info darshan-runtime` query to view the full list of
variants available for the darshan-runtime Spack package.  For example, adding a `+slurm` to
the command line (`spack install darshan-runtime+slurm`) will cause Darshan
to be compiled with support for gathering job ID information from the Slurm
scheduler.

The following commands will load the Darshan packages once they have been
installed:

```csh
spack load -r darshan-util
spack load -r darshan-runtime
```

Note that the spack install of darshan-runtime will use an environment
variable named `$DARSHAN_LOG_DIR_PATH` to indicate where it should store log
files.  This variable is set to the user's home directory by default when
the package is loaded, but it may be overridden.

On Cray systems, you can also perform an additional step to load a
Cray-specific module file. This will make a module called `darshan`
available as described later in this document in the Cray platform recipe.
It enables automatic instrumentation when using the standard Cray compiler
wrappers.

```csh
module use `spack location -i darshan-runtime`/share/craype-2.x/modulefiles
```

## 4. Instrumenting applications
<a name="sec_instrumenting"></a>

#### NOTE
More specific installation "recipes" are provided later in this document for
some platforms.  This section of the documentation covers general techniques.

Once Darshan has been installed and a log path has been prepared, the next
step is to actually instrument applications. The preferred method is to
instrument applications at compile time.

### 4.1. Option 1: Instrumenting MPI applications at compile time
<a name="sec_option1"></a>

This method is applicable to C, Fortran, and C++ MPI applications
(regardless of whether they are static or dynamically linked) and is the most
straightforward method to apply transparently system-wide.  It works by
injecting additional libraries and options into the linker command line to
intercept relevant I/O calls.

On Cray platforms you can enable the compile time instrumentation by simply
loading the Darshan module.  It can then be enabled for all users by placing
that module in the default environment. As of Darshan 3.2.0 this will
instrument both static and dynamic executables, while in previous versions
of Darshan this was only sufficient for static executables.  See the Cray
installation recipe for more details.

For other general MPICH-based MPI implementations, you can generate
Darshan-enabled variants of the standard mpicc/mpicxx/mpif90/mpif77
wrappers using the following commands:

```csh
darshan-gen-cc.pl `which mpicc` --output mpicc.darshan
darshan-gen-cxx.pl `which mpicxx` --output mpicxx.darshan
darshan-gen-fortran.pl `which mpif77` --output mpif77.darshan
darshan-gen-fortran.pl `which mpif90` --output mpif90.darshan
```

The resulting *.darshan wrappers will transparently inject Darshan
instrumentation into the link step without any explicit user intervention.
They can be renamed and placed in an appropriate
PATH to enable automatic instrumentation.  This method also works correctly
for both static and dynamic executables as of Darshan 3.2.0.

For other systems you can enable compile-time instrumentation by either
manually adding the appropriate link options to your command line or
modifying your default MPI compiler script.  The `darshan-config` command
line tool can be used to display the options that you should use:

```csh
# Linker options to use for dynamic linking (default on most platforms)
#   These arguments should go *before* the MPI libraries in the underlying
#   linker command line to ensure that Darshan can be activated.  It should
#   also ideally go before other libraries that may issue I/O function calls.
darshan-config --dyn-ld-flags

# linker options to use for static linking
#   The first set of arguments should go early in the link command line
#   (before MPI, while the second set should go at the end of the link command
#   line
darshan-config --pre-ld-flags
darshan-config --post-ld-flags
```

#### Using a profile configuration
<a name="sec_use_profile_config"></a>

The MPICH MPI implementation supports the specification of a profiling library
configuration that can be used to insert Darshan instrumentation without
modifying the existing MPI compiler script. You can enable a profiling
configuration using environment variables or command line arguments to the
compiler scripts:

Example for MPICH 3.1.1 or newer:
```csh
export MPICC_PROFILE=$DARSHAN_PREFIX/share/mpi-profile/darshan-cc
export MPICXX_PROFILE=$DARSHAN_PREFIX/share/mpi-profile/darshan-cxx
export MPIFORT_PROFILE=$DARSHAN_PREFIX/share/mpi-profile/darshan-f
```

Examples for command line use:
```csh
mpicc -profile=$DARSHAN_PREFIX/share/mpi-profile/darshan-c <args>
mpicxx -profile=$DARSHAN_PREFIX/share/mpi-profile/darshan-cxx <args>
mpif77 -profile=$DARSHAN_PREFIX/share/mpi-profile/darshan-f <args>
mpif90 -profile=$DARSHAN_PREFIX/share/mpi-profile/darshan-f <args>
```

Note that unlike the previously described methods in this section, this
method *will not* automatically adapt to static and dynamic linking options.
The example profile configurations show above only support dynamic linking.

Example profile configurations are also provided with a "-static" suffix if
you need examples for static linking.

### 4.2. Option 2: Instrumenting MPI applications at runtime
<a name="sec_option2"></a>

This method is applicable to pre-compiled dynamically linked executables
as well as interpreted languages such as Python.  You do not need to
change your compile options in any way.  This method works by injecting
instrumentation at runtime.  It will not work for statically linked
executables.

To use this mechanism, set the `LD_PRELOAD` environment variable to the full
path to the Darshan shared library. The preferred method of inserting Darshan
instrumentation in this case is to set the `LD_PRELOAD` variable specifically
for the application of interest. Typically this is possible using
command line arguments offered by the `mpirun` or `mpiexec` scripts or by
the job scheduler:

```csh
mpiexec -n 4 -env LD_PRELOAD /home/carns/darshan-install/lib/libdarshan.so mpi-io-test
```
or
```csh
srun -n 4 --export=LD_PRELOAD=/home/carns/darshan-install/lib/libdarshan.so mpi-io-test
```

For sequential invocations of MPI programs, the following will set `LD_PRELOAD` for process duration only:

```csh
env LD_PRELOAD=/home/carns/darshan-install/lib/libdarshan.so mpi-io-test
```

Other environments may have other specific options for controlling this behavior.
Please check your local site documentation for details.

It is also possible to just export `LD_PRELOAD` as follows, but it is recommended
against doing that to prevent Darshan and MPI symbols from being pulled into
unrelated binaries:

```csh
export LD_PRELOAD=/home/carns/darshan-install/lib/libdarshan.so
```

#### NOTE
For SGI systems running the MPT environment, it may be necessary to set the `MPI_SHEPHERD`
environment variable equal to `true` to avoid deadlock when preloading the Darshan shared
library.

### 4.3. Option 3: Instrumenting non-MPI applications at runtime
<a name="sec_option3"></a>

Similar to the process described in the previous section, Darshan relies on the
`LD_PRELOAD` mechanism for instrumenting dynamically-linked non-MPI applications.
This allows Darshan to instrument dynamically-linked binaries produced by non-MPI
compilers (e.g., gcc or clang), extending Darshan instrumentation to new contexts
(like instrumentation of arbitrary Python programs or instrumenting serial
file transfer utilities like `cp` and `scp`).

The only additional step required of Darshan non-MPI users is to also set the
DARSHAN_ENABLE_NONMPI environment variable to signal to Darshan that non-MPI
instrumentation is requested:

```csh
export DARSHAN_ENABLE_NONMPI=1
```

As described in the previous section, it may be desirable to users to limit the
scope of Darshan's instrumentation by only enabling `LD_PRELOAD` on the target
executable:

```csh
env LD_PRELOAD=/home/carns/darshan-install/lib/libdarshan.so io-test
```

#### NOTE
Recall that Darshan instrumentation of non-MPI applications is only possible with
dynamically-linked applications.

### 4.4. Using other profiling tools at the same time as Darshan
<a name="sec_profile_tools"></a>

As of Darshan version 3.2.0, Darshan does not necessarily interfere with
other profiling tools (particularly those using the PMPI profiling
interface).  Darshan itself does not use the PMPI interface, and instead
uses dynamic linker symbol interception or --wrap function interception for
static executables.

As a rule of thumb most profiling tools should appear in the linker command
line *before* -ldarshan if possible.

## 5. Using the Darshan eXtended Tracing (DXT) module
<a name="sec_dxt"></a>

Darshan's DXT module provides full tracing of MPI-IO and POSIX read/write APIs.
While the DXT module is able to capture finer-grained details compared to traditional
Darshan instrumentation, it may exhibit higher runtime and memory overheads.
For this reason, DXT support is disabled by default in Darshan, but users can opt-in
to DXT instrumentation at runtime by setting their environment as follows:

```csh
export DXT_ENABLE_IO_TRACE=1
```

DXT will trace each I/O operation to files instrumented by Darshan's MPI-IO and
POSIX modules, using a default memory limit of 2 MiB for each module (`DXT_POSIX`
and `DXT_MPIIO`). Memory usage and a number of other aspects of DXT tracing can
be configured as described in Section [Configuring Darshan library at runtime](#sec_config_runtime).

## 6. Using AutoPerf instrumentation modules
<a name="sec_autoperf"></a>

AutoPerf offers two additional Darshan instrumentation modules that may be enabled for MPI applications.

* APMPI: Instrumentation of over 70 MPI-3 communication routines, providing operation counts, datatype sizes, and timing information for each application MPI rank.
* APXC: Instrumentation of Cray XC environments to provide network and compute counters of interest, via PAPI.

Users can request Darshan to build the APMPI and APXC modules by passing
`--enable-apmpi-mod` and `--enable-apxc-mod` options to configure, respectively. Note that these options can be requested independently (i.e., you can build Darshan with APMPI support but not APXC support, and vice versa).

The only prerequisite for the APMPI module is that Darshan be configured with a MPI-3 compliant compiler. For APXC, the user must obviously be using a Cray XC system and must make the PAPI interface available to Darshan (i.e., by running `module load papi`, before building Darshan).

If using the APMPI module, users can additionally specify the `--enable-apmpi-coll-sync` configure option to force Darshan to synchronize before calling underlying MPI routines and to capture additional timing information on how synchronized processes are. Users should note this option will impose additional overheads, but can be useful to help diagnose whether applications are spending a lot of time synchronizing as part of collective communication calls. For this reason, we do not recommend users setting this particular option for production Darshan deployments.

#### NOTE
The AutoPerf instrumentation modules are provided as Git submodules to Darshan's main repository, so if building Darshan source that has been cloned from Git, it is necessary to first retrieve the AutoPerf submodules by running the following command:

```csh
git submodule update --init
```

## 7. Configuring Darshan library at runtime
<a name="sec_config_runtime"></a>

To fine tune Darshan library settings (e.g., internal memory usage, instrumentation
scope, etc.), Darshan provides a couple of mechanisms:

* user environment variable overrides
* a configuration file, which users must specify the path to using the
`DARSHAN_CONFIG_PATH` environment variable

For settings that are specified via a config file and via an environment variable,
the environment settings will take precedence.

#### NOTE
Users of facility-provided Darshan installs should be mindful that these
installs could define their own default Darshan config file. In this case,
users should double check that `DARSHAN_CONFIG_PATH` environment variable
is not already set, and if it is, users should consider copying the
default config file as a starting point before applying their own settings.

### 7.1. Darshan library config settings
<a name="sec_config_settings"></a>

The Darshan library honors the following settings to modify behavior at runtime:

Darshan library config settings
| environment variable setting | config file setting | description |
| ----------- | ----------- | ----------- |
| DARSHAN_DISABLE=1 | N/A | Disables Darshan instrumentation. |
| DARSHAN_ENABLE_NONMPI=1 | N/A | Enables Darshan's non-MPI mode, required for applications that do not call MPI_Init and MPI_Finalize. |
| DARSHAN_CONFIG_PATH=<path> | N/A | Specifies the path to a Darshan config file to load settings from. |
| DARSHAN_DUMP_CONFIG=1 | DUMP_CONFIG | Prints the Darshan configuration to stderr at runtime. |
| DARSHAN_DISABLE_SHARED_REDUCTION=1 | DISABLE_SHARED_REDUCTION | Disables the step in Darshan aggregation in which files that were accessed by all ranks are collapsed into a single cumulative file record at rank 0. This option retains more per-process information at the expense of creating larger log files. |
| DARSHAN_INTERNAL_TIMING=1 | INTERNAL_TIMING | Enables internal instrumentation that will print the time required to startup and shutdown Darshan to stderr at runtime. |
| DARSHAN_MODMEM=<val> | MODMEM <val> | Specifies the amount of memory (in MiB) Darshan instrumentation modules can collectively consume (if not specified, a default 4 MiB quota is used). Overrides any `--with-mod-mem` configure argument. |
| DARSHAN_NAMEMEM=<val> | NAMEMEM <val> | Specifies the amount of memory (in MiB) Darshan can consume for storing record names (if not specified, a default 1 MiB quota is used). Overrides any `--with-name-mem` configure argument. |
| DARSHAN_MEMALIGN=<val> | MEMALIGN <val> | Specifies a value for system memory alignment. Overrides any `--with-mem-align` configure argument (default is 8 bytes). |
| DARSHAN_JOBID=<string> | JOBID <string> | Specifies the name of the environment variable to use for the job identifier, such as PBS_JOBID. Overrides `--with-jobid-env` configure argument. |
| DARSHAN_LOGHINTS=<string> | LOGHINTS <string> | Specifies the MPI-IO hints to use when storing the Darshan output file. The format is a semicolon-delimited list of key=value pairs, for example: hint1=value1;hint2=value2. Overrides any `--with-log-hints` configure argument. |
| DARSHAN_LOGPATH=<path> | LOGPATH <path> | Specifies the path to write Darshan log files to. Note that this directory needs to be formatted using the darshan-mk-log-dirs script.  Overrides any `--with-log-path` configure argument. |
| DARSHAN_MMAP_LOGPATH=<path> | MMAP_LOGPATH <path> | If Darshan's mmap log file mechanism is enabled, this variable specifies what path the mmap log files should be stored in (if not specified, log files will be stored in `/tmp`). |
| DARSHAN_LOGFILE=<path> | N/A | Specifies the path (directory + Darshan log file name) to write the output Darshan log to. This overrides the default Darshan behavior of automatically generating a log file name and adding it to a log file directory formatted using darshan-mk-log-dirs script. |
| DARSHAN_MOD_DISABLE=<mod_csv> | MOD_DISABLE <mod_csv> | Specifies a list of comma-separated Darshan module names to disable at runtime. |
| DARSHAN_MOD_ENABLE=<mod_csv> | MOD_ENABLE <mod_csv> | Specifies a list of comma-separated Darshan module names to enable at runtime. |
| DARSHAN_APP_EXCLUDE=<regex_csv> | APP_EXCLUDE <regex_csv> | Specifies a list of comma-separated regexes that match application names that should not be instrumented. This is useful if Darshan is LD_PRELOADed, in which case logs may be generated for many unintended applications. |
| DARSHAN_APP_INCLUDE=<regex_csv> | APP_INCLUDE <regex_csv> | Specifies a list of comma-separated regexes that match application names that should be instrumented. This setting is used to override any APP_INCLUDE rules. |
| DARSHAN_RANK_EXCLUDE=<rank_csv> | RANK_EXCLUDE <rank_csv> | Specifies a list of comma-separated ranks (or rank ranges) that should not be instrumented. Rank ranges are formatted like "start:end" (if start or end are not specified, the first or last rank is assumed, respectively). Note that the Darshan library will still run on all processes of an application, this setting just controls whether specific ranks are capturing instrumentation data. |
| DARSHAN_RANK_INCLUDE=<rank_csv> | RANK_INCLUDE <rank_csv> | Specifies a list of comma-separated ranks (or rank ranges) that should be instrumented. This setting is used to override any RANK_INCLUDE rules. |
| DARSHAN_DXT_SMALL_IO_TRIGGER=<val> | DXT_SMALL_IO_TRIGGER <val> | Specifies a floating point percentage (i.e., ".8" would be 80%) indicating a threshold of small I/O operation accesses (defined as accesses smaller than 10 KiB), with DXT trace data being discarded for files that exhibit  a percentage of small I/O operations less than this threshold. |
| DARSHAN_DXT_UNALIGNED_IO_TRIGGER=<val> | DXT_UNALIGNED_IO_TRIGGER <val> | Specifies a floating point percentage (i.e., ".8" would be 80%) indicating a threshold of unaligned I/O operation accesses (defined as accesses not aligned to the file alignment value determined by Darshan), with DXT trace data being discarded for files that exhibit a percentage of unaligned I/O operations less than this threshold.  | N/A | MAX_RECORDS <val> <mod_csv> | Specifies the number of records to pre-allocate for each instrumentation module given in a comma-separated list.  Most modules default to tracing 1024 file records per-process. |
| N/A | NAME_EXCLUDE <regex_csv> <mod_csv> | Specifies a list of comma-separated regexes that match record names that should not be instrumented for instrumentation modules given in a comma-separated module list. |
| N/A | NAME_INCLUDE <regex_csv> <mod_csv> | Specifies a list of comma-separated regexes that match record names that should be instrumented for instrumentation modules given in a comma-separated module list. This setting is used to override any NAME_EXCLUDE rules. |
| DXT_ENABLE_IO_TRACE=1 | N/A | (DEPRECATED) Setting this environment variable enables the DXT (Darshan eXtended Tracing) modules at runtime for all files instrumented by Darshan. Replaced by MODULE_ENABLE setting. |
| DARSHAN_EXCLUDE_DIRS=<path_csv> | N/A | (DEPRECATED) Specifies a list of comma-separated paths that Darshan will not instrument at runtime (in addition to Darshan's default exclusion list). Replaced by NAME_EXCLUDE setting. |
| DARSHAN_LDMS_ENABLE= | N/A | Switch to initialize LDMS. If not set, no runtime I/O data will be collected. This only needs to be exported (i.e. setting to a value/string is optional). |
| DARSHAN_LDMS_ENABLE_<mod_name>= | N/A | Specifies the module data that will be collected during runtime using LDMS streams API. These only need to be exported (i.e.  setting to a value/string is optional). |

#### NOTE

 - Config file settings must be specified one per-line, with settings and
   their parameters separated by any whitespace.
 - Settings that take a comma-separated list of modules can use "*" as a
   wildcard to represent all modules.
 - Some config file settings (specifically, `MOD_DISABLE`/`ENABLE`,
   `APP_EXCLUDE`/`INCLUDE`, `RANK_EXCLUDE`/`INCLUDE`, `NAME_EXCLUDE`/`INCLUDE`,
   and `MAX_RECORDS`) may be repeated multiple times rather than
   providing comma-separated values, to ease readability.
 - Improperly formatted config settings are ignored, with Darshan falling
   back to its default configuration.
 - All settings that take regular expressions as input expect them to be
   formatted according to the POSIX `regex.h` interface -- refer to the
   [regex.h manpage](https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/regex.h.html)
   for more details on regex syntax.

### 7.2. Example Darshan configuration
<a name="sec_config_example"></a>

An example configuration file with annotations is given below
(note that comments are allowed by prefixing a line with `#`):

```csh
# enable DXT modules, which are off by default
MOD_ENABLE      DXT_POSIX,DXT_MPIIO

# allocate 4096 file records for POSIX and MPI-IO modules
# (darshan only allocates 1024 per-module by default)
MAX_RECORDS     4096      POSIX,MPI-IO

# the '*' specifier can be used to apply settings for all modules
# in this case, we want all modules to ignore record names
# prefixed with "/home" (i.e., stored in our home directory),
# with a superseding inclusion for files with a ".out" suffix)
NAME_EXCLUDE    ^/home        *
NAME_INCLUDE    .out$         *

# bump up Darshan's default memory usage to 8 MiB
MODMEM  8

# avoid generating logs for git and ls binaries
APP_EXCLUDE     git,ls

# exclude instrumentation for all ranks first
RANK_EXCLUDE    0:
# then selectively re-include ranks 0-3 and 12:15
RANK_INCLUDE    0:3
RANK_INCLUDE    12:15

# only retain DXT traces for files that were accessed
# using small I/O ops 20+% of the time
DXT_SMALL_IO_TRIGGER    .2
```

This configuration could be similarly set using environment variables,
though note that both `MAX_RECORDS` and `NAME_EXCLUDE`/`INCLUDE`
settings do not have environment variable counterparts:

```csh
export DARSHAN_MOD_ENABLE="DXT_POSIX,DXT_MPIIO"
export DARSHAN_MODMEM=8
export DARSHAN_APP_EXCLUDE="git,ls"
export DARSHAN_RANK_EXCLUDE="0:"
export DARSHAN_RANK_INCLUDE="0:3,12:15"
export DARSHAN_DXT_SMALL_IO_TRIGGER=.2
```

## 8. Darshan installation recipes
<a name="sec_recipes"></a>

The following recipes provide examples for prominent HPC systems.
These are intended to be used as a starting point.  You will most likely have to adjust paths and options to
reflect the specifics of your system.

### 8.1. Cray platforms (XE, XC, or similar)
<a name="sec_recipes_cray"></a>

This section describes how to compile and install Darshan,
as well as how to use a software module to enable and disable Darshan
instrumentation on Cray systems.

#### Building and installing Darshan

Please set your environment to use the GNU programming environment before
configuring or compiling Darshan.  Although Darshan can be built with a
variety of compilers, the GNU compiler is recommended because it will
produce a Darshan library that is interoperable with the widest range
of compilers and linkers.  On most Cray systems you can enable the GNU
programming environment with a command similar to "module swap PrgEnv-intel
PrgEnv-gnu".  Please see your site documentation for information about
how to switch programming environments.

The following example shows how to configure and build Darshan on a Cray
system using the GNU programming environment.  Adjust the
--with-log-path and --prefix arguments to point to the desired log file path
and installation path, respectively.

```csh
module swap PrgEnv-pgi PrgEnv-gnu
./configure \
 --with-log-path=/shared-file-system/darshan-logs \
 --prefix=/soft/darshan-3.3.0 \
 --with-jobid-env=SLURM_JOBID \
 --with-username-env=SLURM_JOB_USER \
 CC=cc
make install
module swap PrgEnv-gnu PrgEnv-pgi
```


#### Rationale
The job ID is set to `SLURM_JOBID` for use with a Slurm based scheduler.
An additional environment variable for querying a job's username (`SLURM_JOB_USER`)
is provided as a fallback in case the default environment variable `LOGNAME`
is not properly set (e.g., as is the case when using Slurm's `--export` option to `srun`).
The `CC` variable is configured to point the standard MPI compiler.

If instrumentation of the HDF5 library is desired, additionally load an
acceptable HDF5 module (e.g., `module load cray-hdf5-parallel`) prior to
building and use the `--enable-hdf5-mod` configure argument.
We additionally recommend that you modify Darshan's generated Cray software
module to include a dependency on the HDF5 software module used -- this is
necessary to ensure Darshan library dependencies are satisfied at
application link and run time.

```csh
prereq cray-hdf5-parallel
```

Note that the Darshan-enabled Cray compiler wrappers will always prefer
user-supplied HDF5 libraries over the library used to build Darshan.
However, due to ABI changes in the HDF5 library, the two HDF5 libraries
used must be compatible. Specifically, the HDF5 library versions need to
be either both greater than or equal to 1.10 or both less than 1.10. If
users use an HDF5 version that is incompatible with Darshan, either
link or runtime errors will occur and the user will have to  switch
HDF5 versions or unload the Darshan module.

#### Optional RDTSCP timers for Theta
Darshan's default mechanism (`clock_gettime()`) for retrieving timing
information may introduce more overhead than expected for statically linked
executables on some platforms.  The Theta system at the ALCF (as of July
2021) is a notable example.  It uses static linking by default (which
prevents the use of the standard vDSO optimization for `clock_gettime()`
calls), and it's CPU architecture exhibits relatively high system call
overhead. For Theta and other similar platforms you can explicitly request
that Darshan use the `RDTSCP` instruction in place of `clock_gettime()` for
timing purposes.  `RDTSCP` is a non-portable, Intel-specific instruction.
It must be enabled explicitly at configure time, and the base clock
frequency of the compute node CPU must be specified.

This mechanism can be activated on Theta by adding the
`--enable-rdtscp=1300000000` to the configure command line (the KNL CPUs on
Theta have a base frequency of 1.3 GHz).

Note that timer overhead is unlikely to be a factor in overall performance
unless the application has an edge case workload with frequent sequential
I/O operations, such as small I/O accesses to cached data on a single
process.

As in any Darshan installation, the darshan-mk-log-dirs.pl script can then be
used to create the appropriate directory hierarchy for storing Darshan log
files in the --with-log-path directory.

Note that Darshan is not currently capable of detecting the stripe size
(and therefore the Darshan FILE_ALIGNMENT value) on Lustre file systems.
If a Lustre file system is detected, then Darshan assumes an optimal
file alignment of 1 MiB.

#### Enabling Darshan instrumentation

Darshan will automatically install example software module files in the
following locations (depending on how you specified the --prefix option in
the previous section):

```csh
/soft/darshan-3.3.0/share/craype-1.x/modulefiles/darshan
/soft/darshan-3.3.0/share/craype-2.x/modulefiles/darshan
```

Select the one that is appropriate for your Cray programming environment
(see the version number of the craype module in `module list`).

If you are using the Cray Programming Environment version 1.x, then you
must modify the corresponding modulefile before using it.  Please see
the comments at the end of the file and choose an environment variable
method that is appropriate for your system.  If this is not done, then
the compiler may fail to link some applications when the Darshan module
is loaded.

If you are using the Cray Programming Environment version 2.x then you can
likely use the modulefile as is.  Note that it pulls most of its
configuration from the lib/pkgconfig/darshan-runtime.pc file installed with
Darshan.

The modulefile that you select can be copied to a system location, or the
install location can be added to your local module path with the following
command:

```csh
module use /soft/darshan-3.3.0/share/craype-<VERSION>/modulefiles
```

From this point, Darshan instrumentation can be enabled for all future
application compilations by running "module load darshan".

### 8.2. Linux clusters using MPICH
<a name="sec_recipes_mpich"></a>

Most MPICH installations produce dynamic executables by default.  To
configure Darshan in this environment you can use the following example.  We
recommend using mpicc with GNU compilers to compile Darshan.

```csh
./configure --with-log-path=/darshan-logs --with-jobid-env=PBS_JOBID CC=mpicc
```

The darshan-gen-* scripts described earlier in this document can be used
to create variants of the standard mpicc/mpicxx/mpif77/mpif90 scripts
that are Darshan enabled.  These scripts will work correctly for both
dynamic and statically linked executables.

### 8.3. Linux clusters using Intel MPI
<a name="sec_recipes_intel_mpi"></a>

Most Intel MPI installations produce dynamic executables by default.  To
configure Darshan in this environment you can use the following example:

```csh
./configure --with-log-path=/darshan-logs --with-jobid-env=PBS_JOBID CC=mpicc
```

#### Rationale
There is nothing unusual in this configuration except that you should use
the underlying GNU compilers rather than the Intel ICC compilers to compile
Darshan itself.

You can enable Darshan instrumentation at compile time by adding
`darshan-config --dyn-ld-flags` options to your linker command line.

Alternatively you can use `LD_PRELOAD` runtime instrumentation method to
instrument executables that have already been compiled.

### 8.4. Linux clusters using Open MPI
<a name="sec_recipes_open_mpi"></a>

Follow the generic instructions provided at the top of this document for
compilation, and make sure that the `CC` used for compilation is based on a
GNU compiler.

You can enable Darshan instrumentation at compile time by adding
`darshan-config --dyn-ld-flags` options to your linker command line.

Alternatively you can use `LD_PRELOAD` runtime instrumentation method to
instrument executables that have already been compiled.

## 9. Debugging
<a name="sec_debug"></a>

### 9.1. No log file
<a name="sec_no_log"></a>

In cases where Darshan is not generating a log file for an application, some common things to check are:

* Make sure you are looking in the correct place for logs.  Confirm the
  location with the `darshan-config --log-path` command.

* Check stderr to ensure Darshan isn't indicating any internal errors (e.g., invalid log file path)

For statically linked executables:

* Ensure that Darshan symbols are present in the underlying executable by running `nm` on it:
```csh
> nm test | grep darshan
0000000000772260 b darshan_core
0000000000404440 t darshan_core_cleanup
00000000004049b0 T darshan_core_initialize
000000000076b660 d darshan_core_mutex
00000000004070a0 T darshan_core_register_module
```

For dynamically linked executables:

* Ensure that the Darshan library is present in the list of shared libraries
  to be used by the application, and that it appears before the MPI library:
```csh
> ldd mpi-io-test
	linux-vdso.so.1 (0x00007ffd83925000)
	libdarshan.so => /home/carns/working/install/lib/libdarshan.so (0x00007f0f4a7a6000)
	libmpi.so.12 => /home/carns/working/src/spack/opt/spack/linux-ubuntu19.10-skylake/gcc-9.2.1/mpich-3.3.2-h3dybprufq7i5kt4hcyfoyihnrnbaogk/lib/libmpi.so.12 (0x00007f0f4a44f000)
	libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007f0f4a241000)
        ...
```

General:

* Ensure that the linker is correctly linking in Darshan's runtime libraries:
  * A common mistake is to explicitly link in the underlying MPI libraries (e.g., `-lmpich` or `-lmpichf90`)
in the link command, which can interfere with Darshan's instrumentation
    * These libraries are usually linked in automatically by the compiler
    * MPICH's `mpicc` compiler's `-show` flag can be used to examine the invoked link command, for instance
  * The linker's `-y` option can be used to verify that Darshan is properly intercepting MPI_Init
function (e.g. by setting `CFLAGS='-Wl,-yMPI_Init'`), which it uses to initialize its runtime structures
```csh
/usr/common/software/darshan/3.0.0-pre3/lib/libdarshan.a(darshan-core-init-finalize.o): definition of MPI_Init
```
