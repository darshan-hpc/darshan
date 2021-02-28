# See COPYRIGHT notice in top-level directory.
#
# @configure_input@

CC = $(MPICC)

AM_CPPFLAGS = -I${top_srcdir}/../include

if ENABLE_LDPRELOAD
   AM_CPPFLAGS += -DDARSHAN_PRELOAD
endif

lib_LTLIBRARIES = libdarshan.la

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

libdarshan_la_SOURCES = $(C_SRCS)
libdarshan_la_LIBADD = -lpthread -lrt -lz -ldl $(DARSHAN_LUSTRE_LD_FLAGS)

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

