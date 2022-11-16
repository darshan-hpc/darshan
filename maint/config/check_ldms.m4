AC_DEFUN([CHECK_LDMS],
[
AC_ARG_WITH(ldms,
[  --with-ldms=DIR root directory path of ldms installation [defaults to
                    /usr/local or /usr if not found in /usr/local]
  --with-ldms to disable ldms usage completely],
[if test -d "$withval"; then
    LDMS_HOME="$withval"
    LDFLAGS="$LDFLAGS -L${LDMS_HOME}/lib -Wl,-rpath=${LDMS_HOME}/lib"
    CPPFLAGS="$CPPFLAGS -I${LDMS_HOME}/include"
    __DARSHAN_LDMS_LINK_FLAGS="-L${LDMS_HOME}/lib"
    __DARSHAN_LDMS_INCLUDE_FLAGS="-I${LDMS_HOME}/include"
  else
   AC_MSG_ERROR(ldms installation path is required)
  fi])
AC_CHECK_HEADERS([ldms/ldms.h ldms/ldmsd_stream.h ovis_json/ovis_json.h ovis_util/util.h],
	[ldms_found_stream_headers=yes; break;],
	[AC_MSG_ERROR(One or more LDMS headers not found. Please check installation path or LDMS version is > [4.3.4])])
AS_IF([test "x$ldms_found_stream_headers" = "xyes"],
	[AC_DEFINE([HAVE_LDMS],[1], [Define if standard LDMS library headers exist]) AM_CONDITIONAL([HAVE_LDMS],[test "x$ldms_found_stream_headers" = "xyes"])],
        [AC_MSG_ERROR([Unable to find the standard LDMS headers for Darshan.])])
])
