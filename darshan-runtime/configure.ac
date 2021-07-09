dnl **************************************************************
dnl DARSHAN AUTOCONF SCRIPT
dnl
dnl Process this file with autoconf to produce a configure script.
dnl You may need to use autoheader as well if changing any DEFINEs

AC_PREREQ([2.69])
AC_INIT([Darshan Runtime],[3.3.1],[darshan-users@lists.mcs.anl.gov],
        [darshan-runtime],[https://www.mcs.anl.gov/research/projects/darshan])
AC_CONFIG_HEADERS(darshan-runtime-config.h)
AH_TOP([/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */
])
AC_CONFIG_SRCDIR([lib/darshan.h])
AC_CONFIG_AUX_DIR(../maint/scripts)
AC_CONFIG_MACRO_DIRS([../maint/config])

AM_INIT_AUTOMAKE([1.13 foreign tar-pax])
AM_SILENT_RULES([yes])
AM_MAINTAINER_MODE([enable])

AC_ARG_ENABLE([darshan-runtime],
   [AS_HELP_STRING([--disable-darshan-runtime],
                   [Build without Darshan runtime libraries])],
   [enable_darshan_runtime=${enableval}], [enable_darshan_runtime=yes]
)

dnl Check for MPI
AC_ARG_WITH([mpi],
   [AS_HELP_STRING([--without-mpi], [Build without support for MPI])],
   [], [with_mpi=check]
)

# if CC is set and MPI requested, ensure CC can compile MPI programs
# if CC is not set and MPI requested, find an MPI compiler to use
# if CC is not set and MPI is not requested, find a C compiler to use
AX_PROG_CC_MPI([test x"$with_mpi" != xno], [use_mpi=yes], [use_mpi=no])

if test "x$use_mpi" = xno ; then
   if test "x$with_mpi" = xyes ; then
      AC_MSG_ERROR([CC doesn't appear to be a valid MPI compiler and MPI support was requested. See INSTALL document or try adding CC=mpicc to your configure command line.])
    fi
   ENABLE_MPI=no
else
   ENABLE_MPI=yes
   AC_DEFINE([HAVE_MPI], [], [Define if build is MPI-enabled])
fi

# LT_INIT must be called after CC has been set
LT_INIT([disable-static])

# Note that the "disable-static" option to LT_INIT is intentional.  The
# darshan-runtime package generally installs both dynamic and static
# libraries to accomodate whatever linking method is used by the application
# to be instrumented.  The dynamic library is built using libtool, while the
# static library is built with explicit rules that specify different
# options. The following logic is a safety check to make sure that
# the user did not configure with --enable-static; that option would cause
# libtool to build a duplicate/conflicting static library.
if test "x$enable_static" = xyes ; then
    AC_MSG_ERROR([please do not use the --enable-static or --disable-shared options to override darshan-runtime library settings. Static libraries are already built by default; see Darshan documentation for details.])
fi

if test "x$enable_darshan_runtime" = xyes ; then

   dnl check for GCC compiler support
   AC_ARG_WITH([gcc],
      [AS_HELP_STRING([--without-gcc],
                      [Build without a GCC-compatible compiler])],
      [], [with_gcc=yes]
   )
   if test "x$with_gcc" = xyes ; then
      AC_COMPILE_IFELSE(
         [AC_LANG_PROGRAM([[
             #ifndef __GNUC__
             #error GCC ERROR
             #endif]])], [],
         [AC_MSG_ERROR([CC doesn't appear to be a GNU-compatible compiler, which is strongly recommended for building Darshan. To force use of a non-GCC compatible compiler, add --without-gcc to your configure command line.])]
      )
   fi

   dnl runtime libraries require zlib
   CHECK_ZLIB

   AC_ARG_ENABLE([ld-preload],
      [AS_HELP_STRING([--disable-ld-preload],
                      [Disables support for LD_PRELOAD library])],
      [], [enable_ld_preload=yes]
   )
   if test "x$enable_ld_preload" = xno && test "x$ENABLE_MPI" = xno ; then
      AC_MSG_ERROR([Can not disable building of LD_PRELOAD library when building Darshan without MPI support.])
   fi

   AC_ARG_ENABLE([cuserid],
      [AS_HELP_STRING([--disable-cuserid],
                      [Disables attempted use of cuserid() at run time])],
      [], [enable_cuserid=yes]
   )
   if test "x$enable_cuserid" = "xno" ; then
      AC_DEFINE([__DARSHAN_DISABLE_CUSERID], 1,
                [Define if cuserid() should be disabled])
   fi

   AC_ARG_ENABLE([group-readable-logs],
      [AS_HELP_STRING([--enable-group-readable-logs],
                      [Set log files to be group readable])],
      [], [enable_group_readable_logs=no]
   )
   if test "x$enable_group_readable_logs" = "xyes" ; then
      AC_DEFINE([__DARSHAN_GROUP_READABLE_LOGS], 1,
                [Define if Darshan should set log files to be group readable])
   fi

   AC_ARG_ENABLE([mmap-logs],
      [AS_HELP_STRING([--enable-mmap-logs],
                      [Enables ability to mmap I/O data to log file])],
      [], [enable_mmap_logs=no]
   )
   if test "x$enable_mmap_logs" = "xyes" ; then
      AC_DEFINE([__DARSHAN_ENABLE_MMAP_LOGS], 1,
                [Define if Darshan should mmap data structures to log file])
      AC_CONFIG_FILES(share/darshan-mmap-epilog.sh)
   fi

   AC_ARG_WITH([mem-align],
      [AS_HELP_STRING([--with-mem-aligned@<:@=NUM@:>@],
                      [Memory alignment in bytes @<:@default=8@:>@])],
      [], [with_mem_align=no]
   )
   if test "x$with_mem_align" = xyes || test "x$with_mem_align" = xno ; then
      AC_DEFINE([__DARSHAN_MEM_ALIGNMENT], [8], [Memory alignment in bytes])
      with_mem_align=8
   else
      AC_DEFINE_UNQUOTED([__DARSHAN_MEM_ALIGNMENT], [$with_mem_align],
                         [Memory alignment in bytes])
   fi

   AC_ARG_WITH([log-path-by-env],
      [AS_HELP_STRING([--with-log-path-by-env=NAME1,NAME2,...],
                      [Comma separated list of environment variables to check for
                       log path location before the one set by --with-log-path=DIR at configure time])],
      [], [with_log_path_by_env=no]
   )
   if test "x$with_log_path_by_env" = xyes; then
      AC_MSG_ERROR(--with-log-path-by-env must be given at least one variable name)
   elif test "x$with_log_path_by_env" != xno; then
      AC_DEFINE_UNQUOTED([__DARSHAN_LOG_ENV], "$with_log_path_by_env",
                         [Comma separated list of env. variables to use for log path])
      __DARSHAN_LOG_ENV="$with_log_path_by_env"
      GOT_LOG_PATH=yes
   fi

   AC_ARG_WITH([log-hints],
      [AS_HELP_STRING([--with-log-hints=hint1=x;hint2=y,...],
                      [Semicolon-separated list of MPI-IO hints for log file write])],
      [], [with_log_hints=no]
   )
   if test "x$with_log_hints" = xyes; then
      AC_MSG_ERROR(--with-log-hints must be given an argument)
   elif test "x$with_log_hints" != xno; then
      AC_DEFINE_UNQUOTED([__DARSHAN_LOG_HINTS], "$with_log_hints",
                         [Semicolon-separated list of MPI-IO hints for log file write])
      __DARSHAN_LOG_HINTS="$with_log_hints"
      GOT_LOG_HINTS=yes
   fi

   AC_ARG_WITH([log-path],
      [AS_HELP_STRING([--with-log-path=DIR],
                      [Location to store log files at run time])],
      [], [with_log_path=no]
   )
   if test "x$with_log_path" = xyes; then
      AC_MSG_ERROR(--with-log-path must be given a pathname)
   elif test "x$with_log_path" != xno; then
      AC_DEFINE_UNQUOTED([__DARSHAN_LOG_PATH], "$with_log_path",
                         [Location to store log files at run time])
      __DARSHAN_LOG_PATH="$with_log_path"
      GOT_LOG_PATH=yes
   fi

   AC_ARG_WITH([jobid-env],
      [AS_HELP_STRING([--with-jobid-env=NAME],
                      [Name of environment variable that stores the jobid
                       (specify "NONE" if no appropriate environment variable
                       is available: Darshan will use the pid of rank 0)])],
      [], [with_jobid_env=no]
   )
   if test "x$with_jobid_env" = xyes; then
      AC_MSG_ERROR(--with-jobid-env must be given a name)
   elif test "x$with_jobid_env" != xno; then
      AC_DEFINE_UNQUOTED([__DARSHAN_JOBID], "$with_jobid_env",
                         [Name of the environment variable that stores the jobid])
      GOT_JOBID=yes
   fi

   AC_ARG_WITH([mod-mem],
      [AS_HELP_STRING([--with-mod-mem=NUM],
                      [Maximum runtime memory consumption per process
                       (in MiB) across all instrumentation modules])],
      [], [with_mod_mem=no]
   )
   if test "x$with_mod_mem" = xyes; then
      AC_MSG_ERROR(--with-mod-mem must be given a number)
   elif test "x$with_mod_mem" != xno; then
      AC_DEFINE_UNQUOTED([__DARSHAN_MOD_MEM_MAX], $with_mod_mem,
                         [Maximum runtime memory consumption per process (in MiB) across all instrumentation modules])
   fi

   #
   # Check for specific module enable/disable options
   #

   # NULL module
   AC_ARG_ENABLE([null-mod],
      [AS_HELP_STRING([--disable-null-mod],
                      [Disables compilation of NULL module])],
      [], [enable_null_mod=yes]
   )

   # POSIX module
   AC_ARG_ENABLE([posix-mod],
      [AS_HELP_STRING([--disable-posix-mod],
                      [Disables compilation and use of POSIX module])],
      [], [enable_posix_mod=yes]
   )

   # STDIO module
   AC_ARG_ENABLE([stdio-mod],
      [AS_HELP_STRING([--disable-stdio-mod],
                      [Disables compilation and use of STDIO module])],
      [], [enable_stdio_mod=yes]
   )

   # DXT module
   AC_ARG_ENABLE([dxt-mod],
      [AS_HELP_STRING([--disable-dxt-mod],
                      [Disables compilation and use of DXT module])],
      [], [enable_dxt_mod=yes]
   )

   # MPI-IO module
   AC_ARG_ENABLE([mpiio-mod],
      [AS_HELP_STRING([--disable-mpiio-mod],
                      [Disables compilation and use of MPI-IO module (requires MPI)])],
      [], [enable_mpiio_mod=$ENABLE_MPI]
   )
   if test "x$enable_mpiio_mod" = xyes ; then
      if test "x$ENABLE_MPI" = xno ; then
         AC_MSG_ERROR([--enable-mpiio-mod is used but MPI feature is disabled])
      fi
   fi

   # AUTOPERF MPI module
   AC_ARG_ENABLE([apmpi-mod],
      [AS_HELP_STRING([--enable-apmpi-mod],
                      [Enables compilation and use of AUTOPERF MPI module (requires MPI)])],
      [], [enable_apmpi_mod=no]
   )
   if test "x$enable_apmpi_mod" = xyes ; then
      if test "x$ENABLE_MPI" = xno ; then
         AC_MSG_ERROR([--enable-apmpi-mod Autoperf MPI module requires MPI support])
      fi
      abssrcdir=$(readlink -f ${srcdir})
      AC_CHECK_HEADER([${abssrcdir}/../modules/autoperf/apmpi/darshan-apmpi-log-format.h],
                      [],
                      [AC_MSG_ERROR([The Autoperf MPI module is not present])],
                      [-]) # this last part tells it to only check for presence

      # APMPI only support MPI v3 or higher
      AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
         #include <mpi.h>
         #if MPI_VERSION < 3
         #error MPI_VERSION < 3 detected
         #endif]])], [],
         [AC_MSG_ERROR([APMPI module requires MPI version 3+.])]
      )
   fi

   AC_ARG_ENABLE([apmpi-coll-sync],
      [AS_HELP_STRING([--enable-apmpi-coll-sync],
                      [Enable sync time calculation for MPI collectives])],
      [], [enable_apmpi_coll_sync=no]
   )
   if test "x$enable_apmpi_coll_sync" = xyes ; then
      AC_DEFINE(__APMPI_COLL_SYNC, 1, Define if APMPI should enable sync time calculation for MPI collectives)
   fi

   #AUTOPERF XC module
   AC_ARG_ENABLE([apxc-mod],
      [AS_HELP_STRING([--enable-apxc-mod],
                      [Enables compilation and use of AUTOPERF XC module (requires MPI)])],
      [], [enable_apxc_mod=no]
   )
   if test "x$enable_apxc_mod" = xyes ; then
      if test "x$ENABLE_MPI" = xno ; then
         AC_MSG_ERROR([--enable-apxc-mod Autoperf XC module requires MPI support])
      fi
      abssrcdir=$(readlink -f ${srcdir})
      AC_CHECK_HEADER([papi.h],
                      [with_papi=-lpapi],
                      [AC_MSG_ERROR([Cannot find papi header required for Autoperf XC module])],
                      [])
      AC_CHECK_HEADER([${abssrcdir}/../modules/autoperf/apxc/darshan-apxc-log-format.h],
                      [],
                      [AC_MSG_ERROR([The Autoperf XC git submodule is not present])],
                      [-]) # this last part tells it to only check for presence
   fi

   # inform about HDF5 installs not found in default locations
   AC_ARG_WITH([hdf5],
      [AS_HELP_STRING([--with-hdf5@<:@=DIR@:>@],
                      [Installation directory for HDF5.])],
      [], [with_hdf5=no]
   )

   # HDF5 module (disabled by default)
   AC_ARG_ENABLE([hdf5-mod],
      [AS_HELP_STRING([--enable-hdf5-mod],
                      [Enables compilation and use of HDF5 module])],
      [], [enable_hdf5_mod=no]
   )
   if test "x$enable_hdf5_mod" = xyes ; then
      if test -d "$with_hdf5" ; then
         AC_PATH_PROG(H5DUMP_CHECK, h5dump, "no", "${with_hdf5}/bin")
      else
         AC_PATH_PROG(H5DUMP_CHECK, h5dump, "no")
         # autodetect HDF5 install prefix and set with_hdf5
         with_hdf5=$(dirname $(dirname $H5DUMP_CHECK))
      fi
      if test "x$H5DUMP_CHECK" = xno ; then
         AC_MSG_ERROR(m4_normalize([Darshan HDF5 module enabled but no valid HDF5 install found,
                       use --with-hdf5 to provide the HDF5 install prefix, if needed.]))
      fi

      CFLAGS="${CFLAGS} -I${with_hdf5}/include"
      DARSHAN_HDF5_LD_FLAGS="-L${with_hdf5}/lib [-Wl,-rpath=${with_hdf5}/lib -lhdf5]"

      # Checking hdf5.h is not effective, as compilation may fail for not able to
      # include mpi.h, if MPI is disabled but HDF5 is built with parallel support.
      AC_MSG_CHECKING([whether HDF5 version is at least 1.10.0])
      hdf5_version=`${with_hdf5}/bin/h5dump --version | cut -d' ' -f3`
      hdf5_version_major=`echo ${hdf5_version} | cut -d. -f1`
      hdf5_version_minor=`echo ${hdf5_version} | cut -d. -f2`
      if test $hdf5_version_major -gt 1 || (test $hdf5_version_major -eq 1 && test $hdf5_version_minor -ge 10) ; then
         AC_DEFINE([DARSHAN_HDF5_VERS_1_10_PLUS], 1,
                   [Define if HDF5 module built with version 1.10+])
         DARSHAN_HDF5_ADD_DFLUSH_LD_OPTS="--undefined=H5Dflush --wrap=H5Dflush"
         AC_MSG_RESULT(yes)
      else
         DARSHAN_HDF5_ADD_DFLUSH_LD_OPTS=""
         AC_MSG_RESULT(no)
      fi

      AC_MSG_CHECKING([whether HDF5 is built with parallel support])
      AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
         #include <H5pubconf.h>
         #ifndef H5_HAVE_PARALLEL
         #error HDF5 is built without parallel support
         #endif]])],
         [h5_have_parallel=yes],[h5_have_parallel=no]
      )
      AC_MSG_RESULT([$h5_have_parallel])
      if test "x$h5_have_parallel" = xyes ; then
         AC_DEFINE([DARSHAN_HDF5_PAR_BUILD], 1,
                   [Define if HDF5 built with parallel support])
      fi
      dnl check parallel settings against HDF5 configuration's parallel support
      if test "x$ENABLE_MPI" = xyes ; then
         if test "x$h5_have_parallel" != xyes ; then
            AC_MSG_ERROR(m4_normalize([MPI feature is enabled, but the specified HDF5 is not built with parallel support.
                         Please use an HDF5 built with parallel support.]))
         fi
      else
         if test "x$h5_have_parallel" = xyes ; then
            AC_MSG_ERROR(m4_normalize([MPI feature is disabled, but the specified HDF5 is built with parallel support.
                         Please use an HDF5 built without parallel support.]))
         fi
      fi
   elif test "x$enable_hdf5_mod" != xno ; then
      AC_MSG_ERROR(m4_normalize([--enable-hdf5-mod does not take any argument,
                   use --with-hdf5 to provide the HDF5 install prefix, if needed.]))
   fi

   # PNETCDF module
   AC_ARG_ENABLE([pnetcdf-mod],
      [AS_HELP_STRING([--disable-pnetcdf-mod],
                      [Disables compilation and use of PnetCDF module (requires MPI)])],
      [], [enable_pnetcdf_mod=$ENABLE_MPI]
   )
   if test "x$enable_pnetcdf_mod" = xyes ; then
      if test "x$ENABLE_MPI" = xno ; then
         AC_MSG_ERROR([--enable-pnetcdf-mod is used but MPI feature is disabled])
      fi
   fi

   # BG/Q module
   AC_ARG_ENABLE([bgq-mod],
      [AS_HELP_STRING([--disable-bgq-mod],
                      [Disables compilation and use of BG/Q module (for BG/Q systems)])],
      [], [enable_bgq_mod=check]
   )
   # if bgq module not disabled, check to make sure BG/Q environment available
   if test "x$enable_bgq_mod" != xno; then
      AC_MSG_CHECKING(for BG/Q environment)
      AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
         #ifndef __bgq__
         #error __bgq__ not set
         #endif]])],
         [AC_MSG_RESULT(yes)
          enable_bgq_mod=yes],
         [AC_MSG_RESULT(no)
          AS_IF([test "x$enable_bgq_mod" = xyes],
                [AC_MSG_ERROR(BG/Q module enabled in non-BG/Q environment)])
          enable_bgq_mod=no]
      )
   fi

   # Lustre module
   AC_ARG_ENABLE([lustre-mod],
      [AS_HELP_STRING([--disable-lustre-mod],
                      [Disables compilation and use of Lustre module])],
      [], [enable_lustre_mod=check]
   )
   # if lustre module not disabled, check for needed Lustre module header
   if test x$enable_lustre_mod != xno; then
      AC_CHECK_HEADER([lustre/lustreapi.h],
         [AC_CHECK_LIB(lustreapi, llapi_layout_get_by_xattr,
             [BUILD_LUSTRE_MODULE=1
	      DARSHAN_LUSTRE_LD_FLAGS="-llustreapi"],
	     [AS_IF([test "x$enable_lustre_mod" = xyes],
                 [AC_MSG_ERROR(Cannot find required llapi_layout_get_by_xattr function for the Lustre module)])
             ])
          AS_IF([test "x$enable_lustre_mod" = xyes],
                [AC_MSG_ERROR(Cannot find required headers for the Lustre module)])
          enable_lustre_mod=yes],
         [enable_lustre_mod=no])
   fi

   AC_ARG_ENABLE([mdhim-mod],
      [AS_HELP_STRING([--enable-mdhim-mod], [Enable mdhim module])],
      [], [enable_mdhim_mod=no]
   )
   if test "x$enable_mdhim_mod" = xyes ; then
      AC_CHECK_HEADERS([mdhim.h],
         [], [AC_MSG_ERROR([mdhim requested but headers cannot be found])])
   fi

   dnl sanity check some config parameters
   if test "x$GOT_LOG_PATH" != xyes ; then
      AC_MSG_ERROR(must provide --with-log-path=<path> _or_ --with-log-path-by-env=<variable list> argument to configure.)
   fi

   if test "x$GOT_JOBID" != xyes ; then
      AC_MSG_ERROR(must provide --with-jobid-env=<name> argument to configure.)
   fi

   __DARSHAN_LOG_HINTS_DEFAULT="romio_no_indep_rw=true;cb_nodes=4"
   if test "x$GOT_LOG_HINTS" != xyes ; then
      dnl use default hints
      AC_DEFINE_UNQUOTED([__DARSHAN_LOG_HINTS], "$__DARSHAN_LOG_HINTS_DEFAULT",
                         [Comma-separated list of MPI-IO hints for log file write])
      __DARSHAN_LOG_HINTS=$__DARSHAN_LOG_HINTS_DEFAULT
   fi

   # checks to see how we can print 64 bit values on this architecture
   gt_INTTYPES_PRI
   if test "x$PRI_MACROS_BROKEN" = x1; then
      AC_MSG_ERROR(PRI_xx macros are broken)
   else
      AC_CHECK_HEADERS([inttypes.h], [], [AC_MSG_ERROR(Couldn't find inttypes.h)])
   fi

   dnl Check byte ordering
   AC_C_BIGENDIAN

   dnl temporarily set large file flags just for this test; we don't want
   dnl it to propagate to the makefile because of zlib bugs
   old_cflags="$CFLAGS"
   CFLAGS="$CFLAGS -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE"
   AC_CHECK_TYPES([struct aiocb64],[],[],[[#include <aio.h>]])
   AC_CHECK_TYPES([off64_t])
   CFLAGS="$old_cflags"

   AC_CHECK_HEADERS(mntent.h sys/mount.h)

   #
   # Begin tests for MPI-enabled builds
   #

   if test "x$ENABLE_MPI" = xyes ; then

      # check for OpenMPI
      # NOTE: this check is needed due to a bug observed in some OpenMPI
      # versions triggered by Darshan's MPI-IO module usage of the
      # MPI_File_get_byte_offset() routine. For applications that create
      # zero-length datatypes, then read/write to them, it can trigger a
      # seg fault due to a divide by zero condition in OpenMPI. See the
      # following Darshan issue for more details:
      #   https://xgitlab.cels.anl.gov/darshan/darshan/-/issues/317
      AC_COMPILE_IFELSE([AC_LANG_PROGRAM(,[
         #include <mpi.h>
         #ifdef OPEN_MPI
         #error OPENMPI FOUND
         #endif
         ])],
         [],
      [AC_DEFINE(HAVE_OPEN_MPI, 1, Define if OpenMPI is being used)])

      # determine if the MPI library includes MPI-IO functions or not
      AC_MSG_CHECKING(for MPI-IO support in MPI)
      AC_CHECK_FUNC([MPI_File_open],[],
         [AC_SEARCH_LIBS([MPI_File_open], [mpi mpio],
          [], [AC_MSG_ERROR(Darshan requires a version of MPI with MPI-IO support)])]
      )

      # At some point MPI converted most API functions to be const correct.  We
      # need to test for this to determine how to define MPI wrappers in
      # Darshan.  First we try compiling without cost qualifiers.
      old_cflags="$CFLAGS"
      CFLAGS="$CFLAGS -Werror"
      AC_MSG_CHECKING(for MPI prototypes without const qualifier)
      AC_TRY_COMPILE(
          [
          #include <mpi.h>
          int MPI_File_open(MPI_Comm comm, char *filename, int amode,
              MPI_Info info, MPI_File *fh)
              {return 0;}
          ],
          [],
          [AC_MSG_RESULT(yes)
           AS_IF([test "x$BUILD_APMPI_MODULE" = "x1"],
                 AC_MSG_ERROR(APMPI module requires MPI version 3+))],
          [AC_MSG_RESULT(no)

          # unable to compile without const qualifiers.  Let's try again with
          # const qualifiers.
          AC_MSG_CHECKING(for MPI prototypes with const qualifier)
          AC_TRY_COMPILE(
              [
              #include <mpi.h>
              int MPI_File_open(MPI_Comm comm, const char *filename, int amode,
                  MPI_Info info, MPI_File *fh)
                  {return 0;}
              ],
              [],
              AC_MSG_RESULT(yes)
              AC_DEFINE(HAVE_MPI_CONST, 1, Define if MPI prototypes use const qualifier),
              ,
              AC_MSG_ERROR(Darshan is unable to find a compatible MPI_File_open prototype)
          )]
      )
      CFLAGS="$old_cflags"

      # determine if mpicc can create shared libraries that use MPI functions
      old_cflags="$CFLAGS"
      CFLAGS="$CFLAGS -shared -fpic -DPIC"
      old_libs="$LIBS"
      LIBS="$LIBS -ldl"
      AC_MSG_CHECKING(for shared libraries in MPI)
      AC_CHECK_FUNC([MPI_Comm_size], [],
         [AC_SEARCH_LIBS([MPI_Comm_size],[mpi mpich],[AC_MSG_RESULT(yes)],
             [AC_MSG_RESULT(no)
              AC_MSG_WARN(mpicc doesn't support shared objects; disabling LD_PRELOAD library)
              enable_ld_preload=no])]
      )
      CFLAGS="$old_cflags"
      LIBS="$old_libs"

      # if this version of MPI defines an MPIO_Request type, then we assume that
      # we should use it in the nonblocking MPI-IO functions.  This is necessary
      # for MPI 1.x implementations that did not use generalized requests yet in
      # MPI-IO.  Later MPI implementations might typedef MPIO_Request to MPI_Request
      # so there is no  harm in using the former if it is available there.
      AC_CHECK_TYPE([MPIO_Request],
                    [ac_mpi_req_type=MPIO_Request],
                    [ac_mpi_req_type=MPI_Request],
                    [[#include <mpi.h>]]
      )
      AC_DEFINE_UNQUOTED([__D_MPI_REQUEST], $ac_mpi_req_type,
                         [Generalized request type for MPI-IO])

      # attempt to detect library naming convention in MPI compiler script
      AC_MSG_CHECKING([for old (pre-3.1.1) style MPICH library naming convention])
      AS_IF([$CC -show foo.c -o foo >& /dev/null],
            [AS_IF([$CC -show foo.c -o foo |grep lmpich >& /dev/null],
                   [MPICH_LIB_OLD=1], [MPICH_LIB_OLD=0])
             AS_IF([test "x$MPICH_LIB_OLD" = x1],
                   [AC_MSG_RESULT(yes)], [AC_MSG_RESULT(no)])],
            [AC_MSG_RESULT(no)]
      )
   fi

   #
   # End of MPI-only checks
   #

   AC_MSG_CHECKING(for fscanf redirect)
   AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
      #define _GNU_SOURCE
      #include <stdio.h>
      int fscanf(FILE *stream, const char *format, ...) {return(0);}
      int __isoc99_fscanf(FILE *stream, const char *format, ...) {return(0);}
      ]])],
      [AC_MSG_RESULT(no)
       DARSHAN_STDIO_ADD_FSCANF_LD_OPTS="--wrap=__isoc99_fscanf"],
      [AC_MSG_RESULT(yes)
       AC_DEFINE([HAVE_FSCANF_REDIRECT], 1,
                 [Define if fscanf is redirected to another function])
       DARSHAN_STDIO_ADD_FSCANF_LD_OPTS=""]
   )

   # look for glibc-specific functions
   AC_CHECK_FUNCS([pwritev preadv pwritev2 preadv2])

   DARSHAN_VERSION="AC_PACKAGE_VERSION"
   AC_SUBST(LDFLAGS)
   AC_SUBST(__DARSHAN_LOG_PATH)
   AC_SUBST(__DARSHAN_LOG_ENV)
   AC_SUBST(DARSHAN_VERSION)
   AC_SUBST(MPICH_LIB_OLD)
   AC_SUBST(DARSHAN_STDIO_ADD_FSCANF_LD_OPTS)
   AC_SUBST(DARSHAN_HDF5_ADD_DFLUSH_LD_OPTS)
   AC_SUBST(DARSHAN_HDF5_LD_FLAGS)
   AC_SUBST(DARSHAN_LUSTRE_LD_FLAGS)
   AC_SUBST(with_papi)
else
   enable_mmap_logs=no
   enable_ld_preload=no
   enable_null_mod=no
   enable_posix_mod=no
   enable_stdio_mod=no
   enable_dxt_mod=no
   enable_mpiio_mod=no
   enable_apmpi_mod=no
   enable_apxc_mod=no
   enable_hdf5_mod=no
   enable_pnetcdf_mod=no
   enable_bgq_mod=no
   enable_lustre_mod=no
   enable_mdhim_mod=no
   with_log_path=
   with_jobid_env=
fi


AM_CONDITIONAL(ENABLE_MMAP_LOGS,    [test "x$enable_mmap_logs"   = xyes])
AM_CONDITIONAL(ENABLE_LDPRELOAD,    [test "x$enable_ld_preload"  = xyes])
AM_CONDITIONAL(ENABLE_STATIC,       [test "x$use_mpi"            = xyes])
AM_CONDITIONAL(BUILD_NULL_MODULE,   [test "x$enable_null_mod"    = xyes])
AM_CONDITIONAL(BUILD_POSIX_MODULE,  [test "x$enable_posix_mod"   = xyes])
AM_CONDITIONAL(BUILD_STDIO_MODULE,  [test "x$enable_stdio_mod"   = xyes])
AM_CONDITIONAL(BUILD_DXT_MODULE,    [test "x$enable_dxt_mod"     = xyes])
AM_CONDITIONAL(BUILD_MPIIO_MODULE,  [test "x$enable_mpiio_mod"   = xyes])
AM_CONDITIONAL(BUILD_HDF5_MODULE,   [test "x$enable_hdf5_mod"    = xyes])
AM_CONDITIONAL(BUILD_PNETCDF_MODULE,[test "x$enable_pnetcdf_mod" = xyes])
AM_CONDITIONAL(BUILD_BGQ_MODULE,    [test "x$enable_bgq_mod"     = xyes])
AM_CONDITIONAL(BUILD_LUSTRE_MODULE, [test "x$enable_lustre_mod"  = xyes])
AM_CONDITIONAL(BUILD_MDHIM_MODULE,  [test "x$enable_mdhim_mod"   = xyes])
AM_CONDITIONAL(BUILD_APMPI_MODULE,  [test "x$enable_apmpi_mod"   = xyes])
AM_CONDITIONAL(BUILD_APXC_MODULE,   [test "x$enable_apxc_mod"    = xyes])

AC_CONFIG_FILES(Makefile \
                darshan-config \
                darshan-mk-log-dirs.pl \
                darshan-gen-cc.pl \
                darshan-gen-cxx.pl \
                darshan-gen-fortran.pl \
                lib/Makefile \
                pkgconfig/Makefile \
                pkgconfig/darshan-runtime.pc \
                share/Makefile \
                share/craype-1.x/Makefile \
                share/craype-1.x/darshan-module \
                share/craype-2.x/Makefile \
                share/craype-2.x/darshan-module \
                share/mpi-profile/Makefile \
                share/mpi-profile/darshan-cc.conf \
                share/mpi-profile/darshan-cxx.conf \
                share/mpi-profile/darshan-f.conf \
                share/mpi-profile/darshan-cc-static.conf \
                share/mpi-profile/darshan-cxx-static.conf \
                share/mpi-profile/darshan-f-static.conf \
                share/ld-opts/Makefile \
                share/ld-opts/darshan-stdio-ld-opts \
                share/ld-opts/darshan-hdf5-ld-opts \
)

AC_OUTPUT

if test "x$enable_darshan_runtime" = xyes ; then
   # sanitize log path output
   if test "x$with_log_path_by_env" != xno ; then
      __log_path_by_env=$with_log_path_by_env
   else
      __log_path_by_env="N/A"
   fi
   if test "x$with_log_path" != xno ; then
      __log_path=$with_log_path
   else
      __log_path="N/A"
   fi
   # add extra details for HDF5 and APMPI if enabled
   if test "x$enable_hdf5_mod" = xyes ; then
      enable_hdf5_mod="yes (using HDF5 $hdf5_version)"
   fi
   if test "x$enable_apmpi_mod" = xyes ; then
      if test "x$enable_apmpi_coll_sync" = xyes ; then
         enable_apmpi_mod="yes (collective sync mode)"
      fi
   fi
   echo "------------------------------------------------------------------------------
   ${PACKAGE_NAME} Version ${PACKAGE_VERSION} configured with the following features:"
   if test "x$ENABLE_MPI" = xyes ; then
      echo "\
           MPI C compiler                - $CC"
   else
      echo "\
           C compiler                    - $CC"
   fi
   echo "\
           GCC-compatible compiler       - $with_gcc
           NULL          module support  - $enable_null_mod
           POSIX         module support  - $enable_posix_mod
           STDIO         module support  - $enable_stdio_mod
           DXT           module support  - $enable_dxt_mod
           MPI-IO        module support  - $enable_mpiio_mod
           AUTOPERF MPI  module support  - $enable_apmpi_mod
           AUTOPERF XC   module support  - $enable_apxc_mod
           HDF5          module support  - $enable_hdf5_mod
           PnetCDF       module support  - $enable_pnetcdf_mod
           BG/Q          module support  - $enable_bgq_mod
           Lustre        module support  - $enable_lustre_mod
           MDHIM         module support  - $enable_mdhim_mod
           Memory alignment in bytes     - $with_mem_align
           Log file env variables        - $__log_path_by_env
           Location of Darshan log files - $__log_path
           Job ID env variable           - $with_jobid_env
           MPI-IO hints                  - $__DARSHAN_LOG_HINTS"
fi
