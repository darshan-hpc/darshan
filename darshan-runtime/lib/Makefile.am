# See COPYRIGHT notice in top-level directory.
#
# @configure_input@

AM_CPPFLAGS = -I${top_srcdir}/../include

if ENABLE_LDPRELOAD
   lib_LTLIBRARIES = libdarshan.la
endif
if ENABLE_STATIC
   lib_LIBRARIES = libdarshan.a
endif

C_SRCS = darshan-core-init-finalize.c \
         darshan-core.c \
         darshan-common.c \
         lookup3.c \
         lookup8.c

if BUILD_NULL_MODULE
   C_SRCS += darshan-null.c
endif

if BUILD_POSIX_MODULE
   C_SRCS += darshan-posix.c
endif

if BUILD_STDIO_MODULE
   C_SRCS += darshan-stdio.c
endif

if BUILD_DXT_MODULE
   C_SRCS += darshan-dxt.c
endif

if BUILD_MPIIO_MODULE
   C_SRCS += darshan-mpiio.c
endif

if BUILD_PNETCDF_MODULE
   C_SRCS += darshan-pnetcdf.c
endif

if BUILD_HDF5_MODULE
   C_SRCS += darshan-hdf5.c
endif

if BUILD_BGQ_MODULE
   C_SRCS += darshan-bgq.c
   AM_CPPFLAGS += -DDARSHAN_BGQ
endif

if BUILD_LUSTRE_MODULE
   C_SRCS += darshan-lustre.c
   AM_CPPFLAGS += -DDARSHAN_LUSTRE
endif

if BUILD_MDHIM_MODULE
   C_SRCS += darshan-mdhim.c
   AM_CPPFLAGS += -DDARSHAN_MDHIM
endif

BUILT_SOURCES =
CLEANFILES =
include_HEADERS =
apxc_root = $(top_srcdir)/../modules/autoperf/apxc
if BUILD_APXC_MODULE
   include_HEADERS += $(apxc_root)/darshan-apxc-log-format.h \
                      $(apxc_root)/lib/darshan-apxc-utils.h
   BUILT_SOURCES += darshan-apxc.c
   CLEANFILES += darshan-apxc.c
   C_SRCS += darshan-apxc.c
   AM_CPPFLAGS += -DDARSHAN_USE_APXC \
                  -I$(apxc_root) -I$(apxc_root)/lib
endif
darshan-apxc.c:
	$(LN_S) $(apxc_root)/lib/darshan-apxc.c .

apmpi_root = $(top_srcdir)/../modules/autoperf/apmpi
if BUILD_APMPI_MODULE
   include_HEADERS += $(apmpi_root)/darshan-apmpi-log-format.h
   BUILT_SOURCES += darshan-apmpi.c
   CLEANFILES += darshan-apmpi.c
   C_SRCS += darshan-apmpi.c
   AM_CPPFLAGS += -DDARSHAN_USE_APMPI \
                  -I$(apmpi_root) -I$(apmpi_root)/lib
endif
darshan-apmpi.c:
	$(LN_S) $(apmpi_root)/lib/darshan-apmpi.c .

libdarshan_la_SOURCES  = $(C_SRCS)
libdarshan_la_LIBADD   = -lpthread -lrt -lz -ldl $(DARSHAN_LUSTRE_LD_FLAGS)
libdarshan_la_CPPFLAGS = $(AM_CPPFLAGS) -D_LARGEFILE64_SOURCE -DDARSHAN_PRELOAD

libdarshan_a_SOURCES   = $(C_SRCS)
libdarshan_a_CPPFLAGS  = $(AM_CPPFLAGS) -D_LARGEFILE64_SOURCE -DDARSHAN_WRAP_MMAP

H_SRCS = darshan-common.h \
         darshan.h \
         darshan-core.h \
         darshan-lustre.h \
         darshan-dxt.h \
         uthash.h \
         darshan-dynamic.h \
         utlist.h

EXTRA_DIST = $(H_SRCS) \
             darshan-null.c \
             darshan-posix.c \
             darshan-stdio.c \
             darshan-dxt.c \
             darshan-mpiio.c \
             darshan-pnetcdf.c \
             darshan-hdf5.c \
             darshan-bgq.c \
             darshan-lustre.c \
             darshan-mdhim.c

