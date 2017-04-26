dnl @synopsis CHECK_BZLIB()
dnl
dnl This macro searches for an installed bzlib library. If nothing was
dnl specified when calling configure, it searches first in /usr/local
dnl and then in /usr. If the --with-bzlib=DIR is specified, it will try
dnl to find it in DIR/include/bzlib.h and DIR/lib/libbz2.a. If
dnl --without-bzlib is specified, the library is not searched at all.
dnl
dnl If either the header file (bzlib.h) or the library (libbz2) is not
dnl found, the configuration exits on error, asking for a valid bzlib
dnl installation directory or --without-bzlib.
dnl
dnl The macro defines the symbol HAVE_LIBBZ2 if the library is found. You
dnl should use autoheader to include a definition for this symbol in a
dnl config.h file. Sample usage in a C/C++ source is as follows:
dnl
dnl   #ifdef HAVE_LIBBZ2
dnl   #include <bzlib.h>
dnl   #endif /* HAVE_LIBBZ2 */
dnl
dnl @category InstalledPackages
dnl @author Loic Dachary <loic@senga.org>
dnl @version 2004-09-20
dnl @license GPLWithACException

AC_DEFUN([CHECK_BZLIB],
#
# Handle user hints
#
[AC_MSG_CHECKING(if bzlib is wanted)
AC_ARG_WITH(bzlib,
[  --with-bzlib=DIR root directory path of bzlib installation [defaults to
                    /usr/local or /usr if not found in /usr/local]
  --without-bzlib to disable bzlib usage completely],
[if test "$withval" != no ; then
  if test -d "$withval"
  then
    BZLIB_HOME="$withval"
  else
    BZLIB_HOME=/usr/local
    AC_MSG_WARN([Sorry, $withval does not exist, checking usual places])
  fi
else
  DISABLE_BZLIB=1
  AC_MSG_RESULT(no)
fi])

#
# Locate bzlib, if wanted
#
if test -z "${DISABLE_BZLIB}" 
then
        if test ! -f "${BZLIB_HOME}/include/bzlib.h"
        then
            BZLIB_HOME=/usr
        fi

        AC_MSG_RESULT(yes)
        BZLIB_OLD_LDFLAGS=$LDFLAGS
        BZLIB_OLD_CPPFLAGS=$CPPFLAGS
        LDFLAGS="$LDFLAGS -L${BZLIB_HOME}/lib"
        CPPFLAGS="$CPPFLAGS -I${BZLIB_HOME}/include"
        AC_LANG_SAVE
        AC_LANG_C
        AC_CHECK_LIB(bz2, BZ2_bzCompressInit, [bzlib_cv_libbz2=yes], [bzlib_cv_libbz2=no])
        AC_CHECK_HEADER(bzlib.h, [bzlib_cv_bzlib_h=yes], [bzlib_cv_bzlib_h=no])
        AC_LANG_RESTORE
        if test "$bzlib_cv_libbz2" = "yes" -a "$bzlib_cv_bzlib_h" = "yes"
        then
                #
                # If both library and header were found, use them
                #
                AC_CHECK_LIB(bz2, BZ2_bzCompressInit)
                AC_MSG_CHECKING(bzlib in ${BZLIB_HOME})
                AC_MSG_RESULT(ok)
                LIBBZ2=-lbz2
                AC_SUBST(LIBBZ2)
        else
                #
                # If either header or library was not found, revert and bomb
                #
                AC_MSG_CHECKING(bzlib in ${BZLIB_HOME})
                LDFLAGS="$BZLIB_OLD_LDFLAGS"
                CPPFLAGS="$BZLIB_OLD_CPPFLAGS"
                AC_MSG_RESULT(failed)
# Don't fail; this is optional in Darshan
#                AC_MSG_ERROR(either specify a valid bzlib installation with --with-bzlib=DIR or disable bzlib usage with --without-bzlib)
#                TODO: it would be nice if this showed up at the _end_ of
#                configure...
                AC_MSG_WARN(libbz2 not found; Darshan utilities will use gzip only.)
        fi
fi

])
