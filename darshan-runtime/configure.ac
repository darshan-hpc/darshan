dnl **************************************************************
dnl DARSHAN AUTOCONF SCRIPT
dnl
dnl Process this file with autoconf to produce a configure script.
dnl You may need to use autoheader as well if changing any DEFINEs

dnl sanity checks, output header, location of scripts used here
AC_INIT([darshan-runtime], [3.3.1])
AC_CONFIG_SRCDIR([darshan.h])
AC_CONFIG_AUX_DIR(../maint/config)
AC_CONFIG_MACRO_DIR(../maint/config)
AC_CONFIG_HEADER(darshan-runtime-config.h)

AC_PROG_INSTALL

# We need to know the value of the $libdir and $bindir variables so that
# we can reference the correct path in the darshan compiler wrappers.
# Unfortunately, those two variables are not normally evaluated by autoconf.
# They are evaluated at build time using Makefile variable substitutions.
#
# The following logic was copied from mpich2 1.3.1 to resolve the $libdir
# variable at configure time.
#
# Temporarily replace the default NONE value for exec_prefix
# and prefix with the actual, default values.
savePrefix=$prefix
saveExecprefix=$exec_prefix
test "x$prefix" = xNONE && prefix=$ac_default_prefix
test "x$exec_prefix" = xNONE && exec_prefix=$prefix
eval darshan_lib_path=$libdir
eval darshan_share_path=$datarootdir
prefix=$savePrefix
exec_prefix=$saveExecprefix

dnl Check for MPI
AC_ARG_WITH(mpi,
            [AS_HELP_STRING([--without-mpi], [Build without support for MPI])],
            ,
            [with_mpi=check])

AX_PROG_CC_MPI([test x"$with_mpi" != xno],
               [use_mpi=yes],
               [use_mpi=no])

ENABLE_MPI=1
AS_IF([test "x$use_mpi" = "xno"],
      [ENABLE_MPI=
       AS_IF([test "x$with_mpi" = "xyes"],
             [AC_MSG_ERROR([CC doesn't appear to be a valid MPI compiler and MPI support was requested. See INSTALL document or try adding CC=mpicc to your configure command line.])],)],
      [ENABLE_MPI=1
       AC_DEFINE([HAVE_MPI], [], [Define if build is MPI-enabled])])

AS_IF([test "x$ENABLE_MPI" = "x"],
      DISABLE_STATIC_LIB=1,)  

dnl check for GCC compiler support
AC_ARG_WITH(gcc,
            [AS_HELP_STRING([--without-gcc], [Build without a GCC-compatible compiler])],
            ,
            [with_gcc=yes])

AC_COMPILE_IFELSE([
    AC_LANG_PROGRAM(,[
        #ifndef __GNUC__
        #error GCC ERROR
        #endif
    ])],
    [],
    [AS_IF([test "x$with_gcc" = "xyes"],
           [AC_MSG_ERROR([CC doesn't appear to be a GNU-compatible compiler, which is strongly recommended for building Darshan. To force use of a non-GCC compatible compiler, add --without-gcc to your configure command line.])],)])

dnl runtime libraries require zlib
CHECK_ZLIB

AC_ARG_ENABLE(ld-preload,
[  --disable-ld-preload    Disables support for LD_PRELOAD library],
[if test "x$enableval" = "xno" ; then
    if test "x$ENABLE_MPI" = "x" ; then
        AC_MSG_ERROR([Can not disable building of LD_PRELOAD library when building Darshan without MPI support.])
    fi
    DISABLE_LDPRELOAD="1"
fi]
,)

AC_ARG_ENABLE(cuserid,
[  --disable-cuserid       Disables attempted use of cuserid() at run time],
[if test "x$enableval" = "xno" ; then
    AC_DEFINE(__DARSHAN_DISABLE_CUSERID, 1, Define if cuserid() should be disabled)
fi]
,)

AC_ARG_ENABLE(group-readable-logs,
[  --enable-group-readable-logs
                          Set log files to be group readable],
[if test "x$enableval" = "xyes" ; then
    AC_DEFINE(__DARSHAN_GROUP_READABLE_LOGS, 1, Define if Darshan should set log files to be group readable)
fi]
,)


AC_ARG_ENABLE(mmap-logs,
[  --enable-mmap-logs      Enables ability to mmap I/O data to log file],
[if test "x$enableval" = "xyes" ; then
    AC_DEFINE(__DARSHAN_ENABLE_MMAP_LOGS, 1, Define if Darshan should mmap data structures to log file)
    ENABLE_MMAP_LOGS=1
    AC_CONFIG_FILES(share/darshan-mmap-epilog.sh)
fi]
,)

AC_ARG_WITH(mem-align,
[  --with-mem-align=<num>  Memory alignment in bytes @<:@default=8@:>@],
    [if test x$withval = xyes; then
        AC_MSG_ERROR(--with-mem-align must be given a number in bytes)
    else
        AC_DEFINE_UNQUOTED(__DARSHAN_MEM_ALIGNMENT, ${withval}, Memory alignment in bytes)
    fi],
    [AC_DEFINE([__DARSHAN_MEM_ALIGNMENT], [8], [Memory alignment in bytes])]
)

AC_ARG_WITH(log-path-by-env,
[  --with-log-path-by-env=<env var list>
                          Comma separated list of environment variables to check for
                          log path location before --with-log-path path],
    if test x$withval = xyes; then
        AC_MSG_ERROR(--with-log-path-by-env must be given at least one variable name)
    else
        AC_DEFINE_UNQUOTED(__DARSHAN_LOG_ENV, "${withval}", Comma separated list of env. variables to use for log path)
        __DARSHAN_LOG_ENV="${withval}"
        GOT_LOG_PATH=1
    fi
)

AC_ARG_WITH(log-hints,
[  --with-log-hints=<hint1=x;hint2=y>
                          Semicolon-separated list of MPI-IO hints for log file write],
    if test x$withval = xyes; then
        AC_MSG_ERROR(--with-log-hints must be given an argument)
    else
        AC_DEFINE_UNQUOTED(__DARSHAN_LOG_HINTS, "${withval}", Semicolon-separated list of MPI-IO hints for log file write)
        __DARSHAN_LOG_HINTS="${withval}"
        GOT_LOG_HINTS=1
    fi
)

AC_ARG_WITH(log-path,
[  --with-log-path=<path>  Location to store log files at run time],
    if test x$withval = xyes; then
        AC_MSG_ERROR(--with-log-path must be given a pathname)
    else
        AC_DEFINE_UNQUOTED(__DARSHAN_LOG_PATH, "${withval}", Location to store log files at run time)
        __DARSHAN_LOG_PATH="${withval}"
        GOT_LOG_PATH=1
    fi
)

AC_ARG_WITH(jobid-env,
[  --with-jobid-env=<name> Name of environment variable that stores the jobid
                          (specify "NONE" if no appropriate environment variable
                          is available: Darshan will use the pid of rank 0)],
    if test x$withval = xyes; then
        AC_MSG_ERROR(--with-jobid-env must be given a name)
    else
        AC_DEFINE_UNQUOTED(__DARSHAN_JOBID, "${withval}", Name of the environment variable that stores the jobid)
        __DARSHAN_JOBID="${withval}"
        GOT_JOBID=1
    fi
)

AC_ARG_WITH(mod-mem,
[  --with-mod-mem=<num>    Maximum runtime memory consumption per process
                          (in MiB) across all instrumentation modules],
    if test x$withval = xyes; then
        AC_MSG_ERROR(--with-mod-mem must be given a number)
    else
        AC_DEFINE_UNQUOTED(__DARSHAN_MOD_MEM_MAX, ${withval}, Maximum runtime memory consumption per process (in MiB) across all instrumentation modules)
    fi
)

#
# Check for specific module enable/disable options
#

# NULL module
BUILD_NULL_MODULE=1
AC_ARG_ENABLE(null-mod,
              AS_HELP_STRING([--disable-null-mod], [Disables compilation of NULL module]))
AS_IF([test "x$enable_null_mod" = "xno"],
      BUILD_NULL_MODULE=)

# POSIX module
BUILD_POSIX_MODULE=1
DARSHAN_POSIX_LD_OPTS="@${darshan_share_path}/ld-opts/darshan-posix-ld-opts"
AC_ARG_ENABLE(posix-mod,
              AS_HELP_STRING([--disable-posix-mod], [Disables compilation and use of POSIX module]))
AS_IF([test "x$enable_posix_mod" = "xno"],
      BUILD_POSIX_MODULE=
      DARSHAN_POSIX_LD_OPTS=)

# STDIO module
BUILD_STDIO_MODULE=1
DARSHAN_STDIO_LD_OPTS="@${darshan_share_path}/ld-opts/darshan-stdio-ld-opts"
AC_ARG_ENABLE(stdio-mod,
              AS_HELP_STRING([--disable-stdio-mod], [Disables compilation and use of STDIO module]))
AS_IF([test "x$enable_stdio_mod" = "xno"],
      BUILD_STDIO_MODULE=
      DARSHAN_STDIO_LD_OPTS=)

# DXT module
BUILD_DXT_MODULE=1
AC_ARG_ENABLE(dxt-mod,
              AS_HELP_STRING([--disable-dxt-mod], [Disables compilation and use of DXT module]))
AS_IF([test "x$enable_dxt_mod" = "xno"],
      BUILD_DXT_MODULE=)

# MPI-IO module
BUILD_MPIIO_MODULE=1
DARSHAN_MPIIO_LD_OPTS="@${darshan_share_path}/ld-opts/darshan-mpiio-ld-opts"
AC_ARG_ENABLE(mpiio-mod,
              AS_HELP_STRING([--disable-mpiio-mod], [Disables compilation and use of MPI-IO module (requires MPI)]))
AS_IF([test "x$enable_mpiio_mod" = "xno" || test "x$ENABLE_MPI" = "x"],
      BUILD_MPIIO_MODULE=
      DARSHAN_MPIIO_LD_OPTS=)

# AUTOPERF MPI module
BUILD_APMPI_MODULE=
DARSHAN_APMPI_LD_OPTS=
AC_ARG_ENABLE(apmpi-mod,
              AS_HELP_STRING([--enable-apmpi-mod], [Enables compilation and use of AUTOPERF MPI module (requires MPI)]))
AS_IF([test "x$enable_apmpi_mod" = "xyes"], [
    AS_IF([test "x$ENABLE_MPI" = "x"],
        [AC_MSG_ERROR(Autoperf MPI module requires MPI support)]
    )
    abssrcdir=$(readlink -f ${srcdir})
    AC_CHECK_HEADER([${abssrcdir}/../modules/autoperf/apmpi/darshan-apmpi-log-format.h],
                    [],
                    [AC_MSG_ERROR([The Autoperf MPI module is not present])],
                    [-]) # this last part tells it to only check for presence
    BUILD_APMPI_MODULE=1
    DARSHAN_APMPI_LD_OPTS="@${darshan_share_path}/ld-opts/autoperf-apmpi-ld-opts"
])

AC_ARG_ENABLE(apmpi-coll-sync,
[  --enable-apmpi-coll-sync
                          Enable sync time calculation for MPI collectives],
[if test "x$enableval" = "xyes" ; then
    AC_DEFINE(__APMPI_COLL_SYNC, 1, Define if APMPI should enable sync time calculation for MPI collectives)
fi]
,)

#AUTOPERF APXC module
BUILD_APXC_MODULE=
AC_ARG_ENABLE(apxc-mod,
              AS_HELP_STRING([--enable-apxc-mod], [Enables compilation and use of AUTOPERF APXC module (requires MPI)])
)
AS_IF([test "x$enable_apxc_mod" = "xyes"], [
    AS_IF([test "x$ENABLE_MPI" = "x"],
        [AC_MSG_ERROR(Autoperf XC module requires MPI support)]
    )
    abssrcdir=$(readlink -f ${srcdir})
    AC_CHECK_HEADER([papi.h],
                    [with_papi=-lpapi],
                    [AC_MSG_ERROR([Cannot find papi header required for Autoperf XC module])],
                    [])
    AC_CHECK_HEADER([${abssrcdir}/../modules/autoperf/apxc/darshan-apxc-log-format.h],
                    [],
                    [AC_MSG_ERROR([The Autoperf XC git submodule is not present])],
                    [-]) # this last part tells it to only check for presence
    BUILD_APXC_MODULE=1
])

# HDF5 module (disabled by default)
BUILD_HDF5_MODULE=
DARSHAN_HDF5_LD_OPTS=
AC_ARG_ENABLE(hdf5-mod,
              AS_HELP_STRING([--enable-hdf5-mod], [Enables compilation and use of HDF5 module]),
              [],
              [enable_hdf5_mod=no])
AS_IF([test "x$enable_hdf5_mod" != "xno"],[
    AS_IF([test -d "$enable_hdf5_mod"],
        [CFLAGS="${CFLAGS} -I${enable_hdf5_mod}/include"
        DARSHAN_HDF5_LD_FLAGS="-L${enable_hdf5_mod}/lib [-Wl,-rpath=${enable_hdf5_mod}/lib -lhdf5]"],
        AC_MSG_ERROR(enableval for hdf5-mod is required and must be a valid install prefix path for HDF5 library)
    )
    BUILD_HDF5_MODULE=1
    DARSHAN_HDF5_LD_OPTS="@${darshan_share_path}/ld-opts/darshan-hdf5-ld-opts"
    AC_TRY_COMPILE(
        [
        #include <hdf5.h>
        ],
        [
        #if (H5_VERS_MAJOR > 1) || ((H5_VERS_MAJOR == 1) && (H5_VERS_MINOR >= 10))
        #error H5_VERS 1.10+
        #endif
        ],
        DARSHAN_HDF5_ADD_DFLUSH_LD_OPTS="",
        AC_DEFINE(DARSHAN_HDF5_VERS_1_10_PLUS, 1, Define if HDF5 module built with version 1.10+)
        DARSHAN_HDF5_ADD_DFLUSH_LD_OPTS="--undefined=H5Dflush --wrap=H5Dflush"
    )
    AC_CHECK_PROG(H5PCC_CHECK, h5pcc, "yes", "no", "${enable_hdf5_mod}/bin")
    AS_IF([test x"$H5PCC_CHECK" = x"yes"], [AC_DEFINE(DARSHAN_HDF5_PAR_BUILD, 1, Define if HDF5 built with parallel support)])
])

# PNETCDF module
BUILD_PNETCDF_MODULE=1
DARSHAN_PNETCDF_LD_OPTS="@${darshan_share_path}/ld-opts/darshan-pnetcdf-ld-opts"
AC_ARG_ENABLE(pnetcdf-mod,
              AS_HELP_STRING([--disable-pnetcdf-mod], [Disables compilation and use of PnetCDF module (requires MPI)]))
AS_IF([test "x$enable_pnetcdf_mod" = "xno" || test "x$ENABLE_MPI" = "x"],
      BUILD_PNETCDF_MODULE=
      DARSHAN_PNETCDF_LD_OPTS=)

# BG/Q module
BUILD_BGQ_MODULE=
AC_ARG_ENABLE(bgq-mod,
              AS_HELP_STRING([--disable-bgq-mod], [Disables compilation and use of BG/Q module (for BG/Q systems)]),
              [],
              [enable_bgq_mod=check])
# if bgq module not disabled, check to make sure BG/Q environment available
if test x$enable_bgq_mod != xno; then
    AC_MSG_CHECKING(for BG/Q environment)
    AC_TRY_COMPILE([
        #ifndef __bgq__
        #error __bgq__ not set
        #endif
        ], [],
        AC_MSG_RESULT(yes)
        BUILD_BGQ_MODULE=1,
        AC_MSG_RESULT(no)
        AS_IF([test "x$enable_bgq_mod" = xyes],
            AC_MSG_ERROR(BG/Q module enabled in non-BG/Q environment),
            []))
fi

# Lustre module
BUILD_LUSTRE_MODULE=
AC_ARG_ENABLE(lustre-mod,
              AS_HELP_STRING([--disable-lustre-mod], [Disables compilation and use of Lustre module]),
              [],
              [enable_lustre_mod=check])
# if lustre module not disabled, check for needed Lustre module header
if test x$enable_lustre_mod != xno; then
    AC_CHECK_HEADER([lustre/lustreapi.h],
        [AC_CHECK_LIB(lustreapi, llapi_layout_get_by_xattr,
            [BUILD_LUSTRE_MODULE=1
	     DARSHAN_LUSTRE_LD_FLAGS="-llustreapi"],
	    AS_IF([test "x$enable_lustre_mod" = xyes],
                AC_MSG_ERROR(Cannot find required llapi_layout_get_by_xattr function for the Lustre module),
                []))],
        AS_IF([test "x$enable_lustre_mod" = xyes],
            AC_MSG_ERROR(Cannot find required headers for the Lustre module),
            []))
fi

BUILD_MDHIM_MODULE=
DARSHAN_MDHIM_LD_OPTS=
AC_ARG_ENABLE([mdhim],
	      AS_HELP_STRING([--enable-mdhim], [Enable mdhim module]),
	      [],[] )
AS_IF([test "x$enable_mdhim" = "xyes"],
      BUILD_MDHIM_MODULE=1
      DARSHAN_MDHIM_LD_OPTS="@${darshan_share_path}/ld-opts/darshan-mdhim-ld-opts"
      AC_CHECK_HEADERS([mdhim.h],
		       [],
		       AC_MSG_ERROR([mdhim requested but headers cannot be found]) ),
      []

      )

dnl sanity check some config parameters
if test x$GOT_LOG_PATH != x1; then
    AC_MSG_ERROR(must provide --with-log-path=<path> _or_ --with-log-path-by-env=<variable list> argument to configure.)
fi
if test x$GOT_JOBID != x1; then
    AC_MSG_ERROR(must provide --with-jobid-env=<name> argument to configure.)
fi
__DARSHAN_LOG_HINTS_DEFAULT="romio_no_indep_rw=true;cb_nodes=4"
if test x$GOT_LOG_HINTS != x1; then
dnl use default hints
    AC_DEFINE_UNQUOTED(__DARSHAN_LOG_HINTS, "$__DARSHAN_LOG_HINTS_DEFAULT", Comma-separated list of MPI-IO hints for log file write)
fi

# checks to see how we can print 64 bit values on this architecture
gt_INTTYPES_PRI
if test x$PRI_MACROS_BROKEN == xyes; then
    AC_MSG_ERROR(PRI_xx macros are broken)
else
    AC_CHECK_HEADERS(inttypes.h, , [AC_MSG_ERROR(Couldn't find inttypes.h)])
fi

dnl Check byte ordering
AC_C_BIGENDIAN

dnl temporarily set large file flags just for this test; we don't want
dnl it to propagate to the makefile because of zlib bugs
old_cflags="$CFLAGS"
CFLAGS="$CFLAGS -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE"
AC_MSG_CHECKING(for struct aiocb64)
AC_TRY_COMPILE(
    [
    #include <aio.h>
    ],
    [ struct aiocb64 aiocb; ],
    AC_MSG_RESULT(yes)
    AC_DEFINE(HAVE_AIOCB64, 1, Define if struct aiocb64 type is defined),
    AC_MSG_RESULT(no)
)
CFLAGS="$old_cflags"

dnl temporarily set large file flags just for this test; we don't want
dnl it to propagate to the makefile because of zlib bugs
old_cflags="$CFLAGS"
CFLAGS="$CFLAGS -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE"
AC_MSG_CHECKING(for off64_t)
AC_TRY_COMPILE(
    [
    #include <stdio.h>
    #include <unistd.h>
    #include <sys/types.h>
    #include <sys/stat.h>
    #include <fcntl.h>
    #include <stdarg.h>
    #include <string.h>
    #include <time.h>
    #include <stdlib.h>
    #include <errno.h>
    #include <sys/uio.h>
    #include <pthread.h>
    #include <sys/mman.h>
    #include <search.h>
    #include <assert.h>
    ],
    [ off64_t off; ],
    AC_MSG_RESULT(yes)
    AC_DEFINE(HAVE_OFF64_T, 1, Define if off64_t type is defined),
    AC_MSG_RESULT(no)
)
CFLAGS="$old_cflags"

AC_CHECK_HEADERS(mntent.h sys/mount.h)

#
# Begin tests for MPI-enabled builds
#

AS_IF([test "x$ENABLE_MPI" = "x1"], [

# determine if the MPI library includes MPI-IO functions or not
AC_MSG_CHECKING(for MPI-IO support in MPI)
AC_TRY_LINK([#include <mpi.h>], [
    MPI_Comm comm=0;
    char* filename=0;
    int amode=0;
    MPI_Info info=0;
    MPI_File fh;
            MPI_File_open(comm, filename, amode, info, &fh);],
    AC_MSG_RESULT(yes),
            AC_MSG_ERROR(Darshan requires a version of MPI with MPI-IO support))

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
AC_TRY_LINK([#include <mpi.h>], [
    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
],
    AC_MSG_RESULT(yes),
    AC_MSG_RESULT(no)
    AC_MSG_WARN(mpicc doesn't support shared objects; disabling LD_PRELOAD library)
    DISABLE_LDPRELOAD="1"
)
CFLAGS="$old_cflags"
LIBS="$old_libs"

# if this version of mpi defines an MPIO_Request type, then we assume that
# we should use it in the nonblocking MPI-IO functions.  This is necessary
# for MPI 1.x implementations that did not use generalized requests yet in
# MPI-IO.  Later MPI implementations might typedef MPIO_Request to MPI_Request
# so there is no  harm in using the former if it is available there.
AC_CHECK_TYPE([MPIO_Request],
		AC_DEFINE(__D_MPI_REQUEST, MPIO_Request, Generalized request type for MPI-IO)
	,
		AC_DEFINE(__D_MPI_REQUEST, MPI_Request, Generalized request type for MPI-IO)
	,[#include <mpi.h>])

# attempt to detect library naming convention in mpi compiler script
AS_IF([$CC -show foo.c -o foo >& /dev/null],
	AC_MSG_CHECKING(for old (pre-3.1.1) style MPICH library naming convention)
	AS_IF([$CC -show foo.c -o foo |grep lmpich >& /dev/null],
	    AC_MSG_RESULT(yes)
	    MPICH_LIB_OLD=1,
	    AC_MSG_RESULT(no)
	    MPICH_LIB_OLD=0),
	MPICH_LIB_OLD=0)

])
#
# End of MPI-only checks
#

AC_MSG_CHECKING(for fscanf redirect)
AC_TRY_COMPILE(
    [
    #define _GNU_SOURCE
    #include <stdio.h>
    int fscanf(FILE *stream, const char *format, ...) {return(0);}
    int __isoc99_fscanf(FILE *stream, const char *format, ...) {return(0);}
    ],
    [],
    AC_MSG_RESULT(no)
    DARSHAN_STDIO_ADD_FSCANF_LD_OPTS="--wrap=__isoc99_fscanf",
    AC_MSG_RESULT(yes)
    AC_DEFINE(HAVE_FSCANF_REDIRECT, 1, Define if fscanf is redirected to another function)
    DARSHAN_STDIO_ADD_FSCANF_LD_OPTS=""
)

# look for glibc-specific functions
AC_CHECK_FUNCS([pwritev],[],[])
AC_CHECK_FUNCS([preadv],[],[])
AC_CHECK_FUNCS([pwritev2],[],[])
AC_CHECK_FUNCS([preadv2],[],[])

# check for OpenMPI
# NOTE: this check is needed due to a bug observed in some OpenMPI
# versions triggered by Darshan's MPI-IO module usage of the
# MPI_File_get_byte_offset() routine. For applications that create
# zero-length datatypes, then read/write to them, it can trigger a
# seg fault due to a divide by zero condition in OpenMPI. See the
# following Darshan issue for more details:
#   https://xgitlab.cels.anl.gov/darshan/darshan/-/issues/317
AC_COMPILE_IFELSE([
    AC_LANG_PROGRAM(,[
        #include <mpi.h>
        #ifdef OPEN_MPI
        #error OPENMPI FOUND
        #endif
    ])],
    [],
    [AC_DEFINE(HAVE_OPEN_MPI, 1, Define if OpenMPI is being used)])

DARSHAN_VERSION="AC_PACKAGE_VERSION"
AC_SUBST(darshan_lib_path)
AC_SUBST(darshan_share_path)
AC_SUBST(LDFLAGS)
AC_SUBST(__DARSHAN_LOG_PATH)
AC_SUBST(__DARSHAN_LOG_ENV)
AC_SUBST(DISABLE_STATIC_LIB)
AC_SUBST(DISABLE_LDPRELOAD)
AC_SUBST(ENABLE_MMAP_LOGS)
AC_SUBST(DARSHAN_VERSION)
AC_SUBST(MPICH_LIB_OLD)
AC_SUBST(BUILD_NULL_MODULE)
AC_SUBST(BUILD_POSIX_MODULE)
AC_SUBST(DARSHAN_POSIX_LD_OPTS)
AC_SUBST(BUILD_STDIO_MODULE)
AC_SUBST(DARSHAN_STDIO_LD_OPTS)
AC_SUBST(DARSHAN_STDIO_ADD_FSCANF_LD_OPTS)
AC_SUBST(BUILD_DXT_MODULE)
AC_SUBST(BUILD_MPIIO_MODULE)
AC_SUBST(DARSHAN_MPIIO_LD_OPTS)
AC_SUBST(BUILD_PNETCDF_MODULE)
AC_SUBST(DARSHAN_PNETCDF_LD_OPTS)
AC_SUBST(BUILD_APXC_MODULE)
AC_SUBST(BUILD_APMPI_MODULE)
AC_SUBST(DARSHAN_APMPI_LD_OPTS)
AC_SUBST(BUILD_HDF5_MODULE)
AC_SUBST(DARSHAN_HDF5_LD_OPTS)
AC_SUBST(DARSHAN_HDF5_ADD_DFLUSH_LD_OPTS)
AC_SUBST(DARSHAN_HDF5_LD_FLAGS)
AC_SUBST(BUILD_BGQ_MODULE)
AC_SUBST(BUILD_LUSTRE_MODULE)
AC_SUBST(DARSHAN_LUSTRE_LD_FLAGS)
AC_SUBST(BUILD_MDHIM_MODULE)
AC_SUBST(DARSHAN_MDHIM_LD_OPTS)
AC_SUBST(with_papi)
AC_OUTPUT(Makefile
darshan-mk-log-dirs.pl
darshan-gen-cc.pl
darshan-gen-cxx.pl
darshan-gen-fortran.pl
darshan-config
share/craype-1.x/darshan-module
share/craype-2.x/darshan-module
lib/pkgconfig/darshan-runtime.pc
share/mpi-profile/darshan-cc.conf
share/mpi-profile/darshan-cxx.conf
share/mpi-profile/darshan-f.conf
share/mpi-profile/darshan-cc-static.conf
share/mpi-profile/darshan-cxx-static.conf
share/mpi-profile/darshan-f-static.conf
share/ld-opts/darshan-base-ld-opts
share/ld-opts/darshan-stdio-ld-opts
share/ld-opts/darshan-hdf5-ld-opts
)

msg="Building Darshan with CC=$CC and with the following features:"
AC_MSG_NOTICE($msg)
AS_IF([test "x$BUILD_NULL_MODULE" = "x"],   [AC_MSG_NOTICE(NULL module support:    no)], [AC_MSG_NOTICE(NULL module support:    yes)])
AS_IF([test "x$BUILD_POSIX_MODULE" = "x"],   [AC_MSG_NOTICE(POSIX module support:   no)], [AC_MSG_NOTICE(POSIX module support:   yes)])
AS_IF([test "x$BUILD_STDIO_MODULE" = "x"],   [AC_MSG_NOTICE(STDIO module support:   no)], [AC_MSG_NOTICE(STDIO module support:   yes)])
AS_IF([test "x$BUILD_DXT_MODULE" = "x"],    [AC_MSG_NOTICE(DXT module support:     no)], [AC_MSG_NOTICE(DXT module support:     yes)])
AS_IF([test "x$BUILD_MPIIO_MODULE" = "x"],   [AC_MSG_NOTICE(MPIIO module support:   no)], [AC_MSG_NOTICE(MPIIO module support:   yes)])
AS_IF([test "x$BUILD_HDF5_MODULE" = "x"], [AC_MSG_NOTICE(HDF5 module support:    no)], [AC_MSG_NOTICE(HDF5 module support:    yes)])
AS_IF([test "x$BUILD_PNETCDF_MODULE" = "x"], [AC_MSG_NOTICE(PnetCDF module support: no)], [AC_MSG_NOTICE(PnetCDF module support: yes)])
AS_IF([test "x$BUILD_BGQ_MODULE" = "x"],     [AC_MSG_NOTICE(BG/Q module support:    no)], [AC_MSG_NOTICE(BG/Q module support:    yes)])
AS_IF([test "x$BUILD_LUSTRE_MODULE" = "x"],  [AC_MSG_NOTICE(Lustre module support:  no)], [AC_MSG_NOTICE(Lustre module support:  yes)])
AS_IF([test "x$BUILD_MDHIM_MODULE" = "x"],   [AC_MSG_NOTICE(MDHIM module support:   no)], [AC_MSG_NOTICE(MDHIM module support:   yes)])
AS_IF([test "x$BUILD_APXC_MODULE" = "x"],   [AC_MSG_NOTICE(AUTOPERF APXC module support:   no)], [AC_MSG_NOTICE(AUTOPERF APXC module support:   yes)])
AS_IF([test "x$BUILD_APMPI_MODULE" = "x"],   [AC_MSG_NOTICE(AUTOPERF MPI module support:   no)], [AC_MSG_NOTICE(AUTOPERF MPI module support:   yes)])
