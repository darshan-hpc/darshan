AC_DEFUN([CHECK_LDMS],
[
AC_ARG_WITH([ldms],
[  --with-ldms=DIR         Root directory path of LDMS installation (defaults to
                          to /usr/local or /usr if not found in /usr/local)],
[if test -d "$withval"; then
        LDMS_HOME="$withval"
        LDFLAGS="$LDFLAGS -L${LDMS_HOME}/lib -Wl,-rpath=${LDMS_HOME}/lib"
        CPPFLAGS="$CPPFLAGS -I${LDMS_HOME}/include"
        __DARSHAN_LDMS_LINK_FLAGS="-L${LDMS_HOME}/lib"
        __DARSHAN_LDMS_INCLUDE_FLAGS="-I${LDMS_HOME}/include"
    else
        AC_MSG_ERROR([LDMS installation path is required])
fi])

AC_CHECK_HEADERS([ldms/ldms.h ldms/ldmsd_stream.h ovis_json/ovis_json.h ovis_util/util.h],
        [ldms_found_stream_headers=yes],
        [AC_MSG_ERROR([One or more LDMS headers not found. Please check installation path or LDMS version > 4.3.4])])

AS_IF([test "x$ldms_found_stream_headers" = "xyes"],
        [AC_DEFINE([HAVE_LDMS], [1], [Define if standard LDMS library headers exist])],
        [AC_MSG_ERROR([Unable to find the standard LDMS headers for Darshan.])])

AC_CHECK_LIB([ldms], [ldms_xprt_new_with_auth], [],
             [AC_MSG_ERROR([libldms.so is missing or incompatible])])

AC_MSG_CHECKING([whether ldms_xprt_new_with_auth has 3 or 4 arguments])
AC_COMPILE_IFELSE(
  [AC_LANG_SOURCE([[
#include <ldms/ldms.h>
ldms_t ldms_xprt_new_with_auth(const char *xprt_name, const char *auth_name, struct attr_value_list *auth_av_list);
]])],
  [AC_DEFINE([LDMS_XPRT_NEW_WITH_AUTH_3], [1], [Define if ldms_xprt_new_with_auth has 3 arguments])],
  [
    AC_COMPILE_IFELSE(
      [AC_LANG_SOURCE([[
#include <ldms/ldms.h>
ldms_t ldms_xprt_new_with_auth(const char *xprt_name, ldms_log_fn_t log_fn, const char *auth_name, struct attr_value_list *auth_av_list);
]])],
      [AC_DEFINE([LDMS_XPRT_NEW_WITH_AUTH_4], [1], [Define if ldms_xprt_new_with_auth has 4 arguments])],
      [AC_MSG_ERROR([Cannot determine the number of arguments for ldms_xprt_new_with_auth])]
    )
  ]
)

])
