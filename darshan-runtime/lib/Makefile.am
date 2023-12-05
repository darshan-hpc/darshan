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

BUILT_SOURCES =

C_SRCS = darshan-core-init-finalize.c \
         darshan-core.c \
         darshan-common.c \
         darshan-config.c \
         darshan-ldms.c \
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
   BUILT_SOURCES += darshan-pnetcdf-api.c
endif

if BUILD_HDF5_MODULE
   C_SRCS += darshan-hdf5.c
   AM_CPPFLAGS += -DDARSHAN_HDF5
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

if BUILD_HEATMAP_MODULE
   C_SRCS += darshan-heatmap.c
   AM_CPPFLAGS += -DDARSHAN_HEATMAP
endif

if BUILD_DAOS_MODULE
   C_SRCS += darshan-dfs.c darshan-daos.c
   AM_CPPFLAGS += -DDARSHAN_DAOS
endif

.m4.c:
	$(M4) $(AM_M4FLAGS) $(M4FLAGS) $< >$@

CLEANFILES = darshan-pnetcdf-api.c

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

if HAVE_LDMS
libdarshan_la_LIBADD += -lldmsd_stream -lldms -lovis_json -lcoll
libdarshan_la_CPPFLAGS += -I$(LDMS_HOME)/include
libdarshan_la_LDFLAGS = -L$(LDMS_HOME)/lib -Wl,-rpath=$(LDMS_HOME)/lib
endif

H_SRCS = darshan-common.h \
         darshan.h \
         darshan-config.h \
         darshan-dxt.h \
         darshan-ldms.h \
         uthash.h \
         darshan-dynamic.h \
         utlist.h \
         darshan-heatmap.h

EXTRA_DIST = $(H_SRCS) \
             darshan-null.c \
             darshan-posix.c \
             darshan-stdio.c \
             darshan-dxt.c \
             darshan-ldms.c \
             darshan-mpiio.c \
             darshan-pnetcdf.c \
             darshan-pnetcdf-api.m4 \
             darshan-hdf5.c \
             darshan-bgq.c \
             darshan-lustre.c \
             darshan-mdhim.c \
	     darshan-heatmap.c \
	     darshan-dfs.c \
	     darshan-daos.c

