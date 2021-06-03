#
# See COPYRIGHT notice in top-level directory.
#
# @configure_input@

ACLOCAL_AMFLAGS = -I maint/config

SUBDIRS =

if BUILD_DARSHAN_RUNTIME
   SUBDIRS += darshan-runtime
endif

if BUILD_DARSHAN_UTIL
   SUBDIRS += darshan-util
endif

EXTRA_DIST = doc \
             darshan-test \
             COPYRIGHT \
             ChangeLog \
             README.md
