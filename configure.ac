dnl **************************************************************
dnl DARSHAN AUTOCONF SCRIPT
dnl
dnl Process this file with autoconf to produce a configure script.
dnl You may need to use autoheader as well if changing any DEFINEs

AC_PREREQ([2.69])
AC_INIT([Darshan],[3.2.1],[darshan-users@lists.mcs.anl.gov],
        [darshan],[https://www.mcs.anl.gov/research/projects/darshan])
AC_CONFIG_HEADERS(config.h)
AH_TOP([/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */
])
AC_CONFIG_SRCDIR([include/darshan-log-format.h])
AC_CONFIG_AUX_DIR(maint/scripts)
AC_CONFIG_MACRO_DIRS([maint/config])

AM_INIT_AUTOMAKE([1.13 foreign tar-pax])
AM_SILENT_RULES([yes])
AM_MAINTAINER_MODE([enable])

AC_ARG_ENABLE([darshan-runtime],
   [AS_HELP_STRING([--disable-darshan-runtime],
                   [Build without Darshan runtime libraries])],
   [enable_darshan_runtime=${enableval}], [enable_darshan_runtime=yes]
)
AM_CONDITIONAL(BUILD_DARSHAN_RUNTIME, [test "x$enable_darshan_runtime" != xno])

AC_ARG_ENABLE([darshan-util],
   [AS_HELP_STRING([--disable-darshan-util],
                   [Build without Darshan utility tools])],
   [enable_darshan_util=${enableval}], [enable_darshan_util=yes]
)
AM_CONDITIONAL(BUILD_DARSHAN_UTIL, [test "x$enable_darshan_util" != xno])

dnl must configure subpackages even if they are not built
AC_CONFIG_SUBDIRS([darshan-util])
AC_CONFIG_SUBDIRS([darshan-runtime])

AC_CONFIG_FILES(Makefile include/Makefile)

AC_OUTPUT

echo "------------------------------------------------------------------------------

   ${PACKAGE_NAME} Version ${PACKAGE_VERSION}

   Features:  Build Darshan runtime libraries - ${enable_darshan_runtime}
              Build Darshan utility tools     - ${enable_darshan_util}

   Next step: run 'make' to build the library and/or utility tools.
              run 'make install' to install Darshan in directory:
              ${prefix}

------------------------------------------------------------------------------"

