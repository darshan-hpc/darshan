dnl **************************************************************
dnl DARSHAN AUTOCONF SCRIPT
dnl
dnl Process this file with autoconf to produce a configure script.
dnl You may need to use autoheader as well if changing any DEFINEs

AC_PREREQ([2.69])
AC_INIT([Darshan Utility],[3.3.1],[darshan-users@lists.mcs.anl.gov],
        [darshan-util],[https://www.mcs.anl.gov/research/projects/darshan])
AC_CONFIG_HEADERS(darshan-util-config.h)
AH_TOP([/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */
])
AC_CONFIG_SRCDIR([darshan-logutils.h])
AC_CONFIG_AUX_DIR(../maint/scripts)
AC_CONFIG_MACRO_DIRS(../maint/config)

AM_INIT_AUTOMAKE([1.13 foreign tar-pax])
AM_SILENT_RULES([yes])
AM_MAINTAINER_MODE([enable])

AC_ARG_ENABLE([darshan-util],
   [AS_HELP_STRING([--disable-darshan-util],
                   [Build without Darshan utility tools])],
   [enable_darshan_util=${enableval}], [enable_darshan_util=yes]
)

# LT_INIT cannot be invoked conditionally
LT_INIT

if test "x$enable_darshan_util" = xyes ; then

   # zlib is required
   CHECK_ZLIB

   # bz2 is optional
   CHECK_BZLIB

   # checks to see how we can print 64 bit values on this architecture
   gt_INTTYPES_PRI
   if test "x$PRI_MACROS_BROKEN" = x1 ; then
      AC_MSG_ERROR(PRI_xx macros are broken)
   else
      AC_CHECK_HEADERS([inttypes.h],[],[AC_MSG_ERROR(Couldn't find inttypes.h)])
   fi

   AC_CHECK_PROG([HAVE_PDFLATEX], [pdflatex], [yes], [no])

   if test "x$HAVE_PDFLATEX" = xyes; then
      AC_MSG_CHECKING(for -halt-on-error argument to pdflatex)
      PDFLATEX_GREP=`pdflatex --help |grep halt-on-error`
      if test "x$PDFLATEX_GREP" != x; then
         AC_MSG_RESULT(yes)
         __DARSHAN_PDFLATEX_HALT_ON_ERROR="-halt-on-error"
      else
         __DARSHAN_PDFLATEX_HALT_ON_ERROR=""
         AC_MSG_RESULT(no)
      fi
   else
      AC_MSG_WARN(Please install pdflatex if you wish to use the darshan-job-summary.pl utility)
   fi

   dnl Check byte ordering
   AC_C_BIGENDIAN

   dnl temporarily set large file flags just for this test; we don't want
   dnl it to propagate to the makefile because of zlib bugs
   old_cflags="$CFLAGS"
   CFLAGS="$CFLAGS -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE"
   AC_CHECK_TYPE([off64_t])
   CFLAGS="$old_cflags"

   AM_PATH_PYTHON

   AC_ARG_ENABLE([pydarshan],
      [AS_HELP_STRING([--enable-pydarshan],
                      [enables build/install of pydarshan module and tools])],
      [], [enable_pydarshan=no]
   )
   if test "x$enable_pydarshan" = xyes ; then
      if test "x$enable_shared" != xyes ; then
         AC_MSG_ERROR(--enable-pydarshan requires --enable-shared configure option)
      fi
      DARSHAN_PYDARSHAN_PATH=${darshan_lib_path}/pydarshan/lib/python`${PYTHON} -c 'import sys; version=sys.version_info@<:@:2@:>@; print("{0}.{1}".format(*version))'`/site-packages
      AC_CONFIG_FILES([pydarshan-info.py pydarshan-info.sh])
   fi

   AC_ARG_ENABLE([apxc_mod],
      [AS_HELP_STRING([--enable-apxc-mod],
                      [enables compilation and use of the AutoPerf Cray XC module])],
      [], [enable_apxc_mod=no]
   )
   if test "x$enable_apxc_mod" = xyes; then
      AC_CHECK_HEADER([${ac_abs_confdir}/../modules/autoperf/apxc/darshan-apxc-log-format.h],
                      DARSHAN_USE_APXC=1,
                      [AC_MSG_ERROR([The autoperf APXC module is not present])],
                      [-]) # this last part tells it to only check for presence
   fi
   AC_ARG_ENABLE([apmpi_mod],
      [AS_HELP_STRING([--enable-apmpi-mod],
                      [enables compilation and use of the AutoPerf MPI module])],
      [], [enable_apmpi_mod=no]
   )
   if test "x$enable_apmpi_mod" = xyes; then
      AC_CHECK_HEADER([${ac_abs_confdir}/../modules/autoperf/apmpi/darshan-apmpi-log-format.h],
                      DARSHAN_USE_APMPI=1,
                      [AC_MSG_ERROR([The autoperf MPI module is not present])],
                      [-]) # this last part tells it to only check for presence
   fi

   AC_CHECK_FUNCS([strndup])

   DARSHAN_UTIL_VERSION="AC_PACKAGE_VERSION"

   AC_SUBST(__DARSHAN_ZLIB_LINK_FLAGS)
   AC_SUBST(__DARSHAN_ZLIB_INCLUDE_FLAGS)
   AC_SUBST(__DARSHAN_PDFLATEX_HALT_ON_ERROR)
   AC_SUBST(PYTHON)
   AC_SUBST(DARSHAN_PYDARSHAN_PATH)
   AC_SUBST(DARSHAN_UTIL_VERSION)
else
   enable_pydarshan=no
fi

AM_CONDITIONAL(DARSHAN_ENABLE_PYDARSHAN, [test "x$enable_pydarshan" = xyes])
AM_CONDITIONAL(DARSHAN_USE_APXC, [test "x$enable_apxc_mod" = xyes])
AM_CONDITIONAL(DARSHAN_USE_APMPI, [test "x$enable_apmpi_mod" = xyes])

AC_CONFIG_FILES([Makefile \
                 darshan-job-summary/Makefile \
                 darshan-job-summary/bin/darshan-job-summary.pl \
                 maint/darshan-util.pc])

AC_OUTPUT

if test "x$enable_darshan_util" = xyes ; then
   echo "------------------------------------------------------------------------------
   ${PACKAGE_NAME} Version ${PACKAGE_VERSION} configured with the following features:
           C compiler                    - $CC
           Python module and tools       - $enable_pydarshan
           AUTOPERF MPI  module support  - $enable_apmpi_mod
           AUTOPERF APXC module support  - $enable_apxc_mod"
fi

