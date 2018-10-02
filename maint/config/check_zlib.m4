AC_DEFUN([CHECK_ZLIB],
[

AC_ARG_WITH(zlib,
[  --with-zlib=DIR root directory path of zlib installation [defaults to
                    /usr/local or /usr if not found in /usr/local]
  --without-zlib to disable zlib usage completely],
[if test "$withval" != no ; then
  if test -d "$withval"
  then
    ZLIB_HOME="$withval"
    LDFLAGS="$LDFLAGS -L${ZLIB_HOME}/lib"
    CPPFLAGS="$CPPFLAGS -I${ZLIB_HOME}/include"
    __DARSHAN_ZLIB_LINK_FLAGS="-L${ZLIB_HOME}/lib"
    __DARSHAN_ZLIB_INCLUDE_FLAGS="-I${ZLIB_HOME}/include"
  else
    AC_MSG_WARN([Sorry, $withval does not exist, checking usual places])
  fi
else
  AC_MSG_ERROR(zlib is required)
fi])

AC_CHECK_HEADER(zlib.h, [],[AC_MSG_ERROR(zlib.h not found)])
AC_CHECK_LIB(z, inflateEnd, [],[AC_MSG_ERROR(libz not found)])

])
