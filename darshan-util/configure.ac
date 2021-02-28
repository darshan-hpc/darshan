dnl **************************************************************
dnl DARSHAN AUTOCONF SCRIPT
dnl
dnl Process this file with autoconf to produce a configure script.
dnl You may need to use autoheader as well if changing any DEFINEs

dnl sanity checks, output header, location of scripts used here
AC_INIT([darshan-util], [3.3.1])
AC_CONFIG_SRCDIR([darshan-logutils.h])
AC_CONFIG_AUX_DIR(../maint/config)
AC_CONFIG_HEADER(darshan-util-config.h)

AC_PROG_INSTALL

CHECK_ZLIB
CHECK_BZLIB

AC_CONFIG_FILES([Makefile darshan-job-summary/bin/darshan-job-summary.pl maint/darshan-util.pc])

# We need to know the value of the $libdir variable so that
# we can reference the correct path in the pydarshan utilities.
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


# checks to see how we can print 64 bit values on this architecture
gt_INTTYPES_PRI
if test x$PRI_MACROS_BROKEN == xyes; then
    AC_MSG_ERROR(PRI_xx macros are broken)
else
    AC_CHECK_HEADERS(inttypes.h, , [AC_MSG_ERROR(Couldn't find inttypes.h)])
fi

AC_CHECK_PROG(HAVE_PDFLATEX, pdflatex, yes, no)

if test x$HAVE_PDFLATEX == xyes; then
    AC_MSG_CHECKING(for -halt-on-error argument to pdflatex)
    PDFLATEX_GREP=`pdflatex --help |grep halt-on-error`
    if test "x$PDFLATEX_GREP" != "x"; then
        AC_MSG_RESULT(yes)
        __DARSHAN_PDFLATEX_HALT_ON_ERROR="-halt-on-error"
    else
        __DARSHAN_PDFLATEX_HALT_ON_ERROR=""
        AC_MSG_RESULT(no)
    fi
fi

dnl Check byte ordering
AC_C_BIGENDIAN

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

DARSHAN_ENABLE_SHARED=0
AC_ARG_ENABLE(shared, 
[  --enable-shared   enables building of shared darshan-util library], 
[if test "x$enableval" = "xyes" ; then
    DARSHAN_ENABLE_SHARED=1
fi]
,)

AM_PATH_PYTHON

DARSHAN_ENABLE_PYDARSHAN=0
AC_ARG_ENABLE(pydarshan,
[  --enable-pydarshan   enables build/install of pydarshan module and tools],
[if test "x$enableval" = "xyes" ; then
    if test "x$DARSHAN_ENABLE_SHARED" != "x1" ; then
        AC_MSG_ERROR(--enable-pydarshan requires --enable-shared configure option)
    fi
    DARSHAN_ENABLE_PYDARSHAN=1
    DARSHAN_PYDARSHAN_PATH=${darshan_lib_path}/pydarshan/lib/python`${PYTHON} -c 'import sys; version=sys.version_info@<:@:2@:>@; print("{0}.{1}".format(*version))'`/site-packages
    AC_CONFIG_FILES([pydarshan-info.py pydarshan-info.sh])
fi]
,)

AC_ARG_ENABLE(
    [autoperf-apxc],
    [  --enable-autoperf-apxc    Enables compilation and use of the AutoPerf Cray XC module],
    [enable_autoperf_apxc=yes],
    []
)
AC_ARG_ENABLE(
    [autoperf-apmpi],
    [  --enable-autoperf-apmpi    Enables compilation and use of the AutoPerf MPI module],
    [enable_autoperf_apmpi=yes],
    []
)

if test x$enable_autoperf_apxc = xyes; then
    abssrcdir=$(readlink -f ${srcdir})
    AC_CHECK_HEADER([${abssrcdir}/../modules/autoperf/apxc/darshan-apxc-log-format.h],
                    DARSHAN_USE_APXC=1,
                    [AC_MSG_ERROR([The autoperf APXC module is not present])],
                    [-]) # this last part tells it to only check for presence
fi
if test x$enable_autoperf_apmpi = xyes; then
    abssrcdir=$(readlink -f ${srcdir})
    AC_CHECK_HEADER([${abssrcdir}/../modules/autoperf/apmpi/darshan-apmpi-log-format.h],
                    DARSHAN_USE_APMPI=1,
                    [AC_MSG_ERROR([The autoperf MPI module is not present])],
                    [-]) # this last part tells it to only check for presence
fi

AC_CHECK_FUNCS([strndup])

DARSHAN_UTIL_VERSION="AC_PACKAGE_VERSION"

AC_SUBST(__DARSHAN_ZLIB_LINK_FLAGS)
AC_SUBST(__DARSHAN_ZLIB_INCLUDE_FLAGS)
AC_SUBST(__DARSHAN_PDFLATEX_HALT_ON_ERROR)
AC_SUBST(DARSHAN_ENABLE_SHARED)
AC_SUBST(DARSHAN_ENABLE_PYDARSHAN)
AC_SUBST(PYTHON)
AC_SUBST(DARSHAN_PYDARSHAN_PATH)
AC_SUBST(DARSHAN_UTIL_VERSION)
AC_SUBST(DARSHAN_USE_APXC)
AC_SUBST(DARSHAN_USE_APMPI)

AC_OUTPUT

if test x$HAVE_PDFLATEX != xyes; then
    AC_MSG_WARN(Please install pdflatex if you wish to use the darshan-job-summary.pl utility)
fi
