#TODO: each module provides own makefile with module-specific objects, build options, etc.

srcdir = @srcdir@
prefix = @prefix@
exec_prefix = @exec_prefix@
datarootdir = @datarootdir@
includedir = @includedir@
mandir = @mandir@
sbindir = @sbindir@
bindir = @bindir@
libdir = @libdir@
LDFLAGS = @LDFLAGS@
CC = @CC@
LD = @LD@

DISABLE_STATIC_LIB = @DISABLE_STATIC_LIB@
DISABLE_LDPRELOAD = @DISABLE_LDPRELOAD@
ENABLE_MMAP_LOGS = @ENABLE_MMAP_LOGS@
DARSHAN_LOG_FORMAT = $(srcdir)/../darshan-log-format.h
DARSHAN_VERSION = @DARSHAN_VERSION@

ifndef DISABLE_STATIC_LIB
all: lib/libdarshan.a
endif
ifndef DISABLE_LDPRELOAD
all: lib/libdarshan.so
endif

VPATH = $(srcdir)

CFLAGS = -DDARSHAN_CONFIG_H=\"darshan-runtime-config.h\" -I . -I ../ -I $(srcdir) -I$(srcdir)/../ @CFLAGS@ @CPPFLAGS@ -D_LARGEFILE64_SOURCE

CFLAGS_SHARED = -DDARSHAN_CONFIG_H=\"darshan-runtime-config.h\" -I . -I$(srcdir) -I$(srcdir)/../ @CFLAGS@ @CPPFLAGS@ -D_LARGEFILE64_SOURCE -shared -fpic -DPIC -DDARSHAN_PRELOAD

LIBS = -lpthread -lrt -lz -ldl @DARSHAN_LUSTRE_LD_FLAGS@

BUILD_NULL_MODULE = @BUILD_NULL_MODULE@
BUILD_POSIX_MODULE = @BUILD_POSIX_MODULE@
BUILD_STDIO_MODULE = @BUILD_STDIO_MODULE@
BUILD_DXT_MODULE = @BUILD_DXT_MODULE@
BUILD_MPIIO_MODULE = @BUILD_MPIIO_MODULE@
BUILD_PNETCDF_MODULE = @BUILD_PNETCDF_MODULE@
BUILD_HDF5_MODULE = @BUILD_HDF5_MODULE@
BUILD_BGQ_MODULE = @BUILD_BGQ_MODULE@
BUILD_LUSTRE_MODULE = @BUILD_LUSTRE_MODULE@
BUILD_MDHIM_MODULE = @BUILD_MDHIM_MODULE@
BUILD_APMPI_MODULE = @BUILD_APMPI_MODULE@
BUILD_APXC_MODULE = @BUILD_APXC_MODULE@

DARSHAN_STATIC_MOD_OBJS =
DARSHAN_DYNAMIC_MOD_OBJS =

ifdef BUILD_NULL_MODULE
DARSHAN_STATIC_MOD_OBJS += lib/darshan-null.o
DARSHAN_DYNAMIC_MOD_OBJS += lib/darshan-null.po
endif

ifdef BUILD_POSIX_MODULE
DARSHAN_STATIC_MOD_OBJS += lib/darshan-posix.o
DARSHAN_DYNAMIC_MOD_OBJS += lib/darshan-posix.po
endif

ifdef BUILD_STDIO_MODULE
DARSHAN_STATIC_MOD_OBJS += lib/darshan-stdio.o
DARSHAN_DYNAMIC_MOD_OBJS += lib/darshan-stdio.po
endif

ifdef BUILD_DXT_MODULE
DARSHAN_STATIC_MOD_OBJS += lib/darshan-dxt.o
DARSHAN_DYNAMIC_MOD_OBJS += lib/darshan-dxt.po
endif

ifdef BUILD_MPIIO_MODULE
DARSHAN_STATIC_MOD_OBJS += lib/darshan-mpiio.o
DARSHAN_DYNAMIC_MOD_OBJS += lib/darshan-mpiio.po
endif

ifdef BUILD_PNETCDF_MODULE
DARSHAN_STATIC_MOD_OBJS += lib/darshan-pnetcdf.o
DARSHAN_DYNAMIC_MOD_OBJS += lib/darshan-pnetcdf.po
endif

ifdef BUILD_HDF5_MODULE
DARSHAN_STATIC_MOD_OBJS += lib/darshan-hdf5.o
DARSHAN_DYNAMIC_MOD_OBJS += lib/darshan-hdf5.po
endif

ifdef BUILD_BGQ_MODULE
DARSHAN_STATIC_MOD_OBJS += lib/darshan-bgq.o
DARSHAN_DYNAMIC_MOD_OBJS += lib/darshan-bgq.po
CFLAGS += -DDARSHAN_BGQ
CFLAGS_SHARED += -DDARSHAN_BGQ
endif

ifdef BUILD_LUSTRE_MODULE
DARSHAN_STATIC_MOD_OBJS += lib/darshan-lustre.o
DARSHAN_DYNAMIC_MOD_OBJS += lib/darshan-lustre.po
CFLAGS += -DDARSHAN_LUSTRE
CFLAGS_SHARED += -DDARSHAN_LUSTRE
endif

ifdef BUILD_MDHIM_MODULE
DARSHAN_STATIC_MOD_OBJS += lib/darshan-mdhim.o
DARSHAN_DYNAMIC_MOD_OBJS += lib/darshan-mdhim.po
CFLAGS += -DDARSHAN_MDHIM
CFLAGS_SHARED += -DDARSHAN_MDHIM
endif

ifdef BUILD_APXC_MODULE
include $(srcdir)/../modules/autoperf/apxc/Makefile.darshan
endif
ifdef BUILD_APMPI_MODULE
include $(srcdir)/../modules/autoperf/apmpi/Makefile.darshan
endif

lib::
	@mkdir -p $@

lib/darshan-core-init-finalize.o: lib/darshan-core-init-finalize.c darshan.h darshan-core.h $(DARSHAN_LOG_FORMAT) | lib
	$(CC) $(CFLAGS) -c $< -o $@

lib/darshan-core-init-finalize.po: lib/darshan-core-init-finalize.c darshan.h darshan-dynamic.h darshan-core.h $(DARSHAN_LOG_FORMAT) | lib
	$(CC) $(CFLAGS_SHARED) -c $< -o $@

lib/darshan-core.o: lib/darshan-core.c darshan.h darshan-core.h $(DARSHAN_LOG_FORMAT) | lib
	$(CC) $(CFLAGS) -c $< -o $@

lib/darshan-core.po: lib/darshan-core.c darshan.h darshan-dynamic.h darshan-core.h $(DARSHAN_LOG_FORMAT) | lib
	$(CC) $(CFLAGS_SHARED) -c $< -o $@

lib/darshan-common.o: lib/darshan-common.c darshan.h darshan-common.h $(DARSHAN_LOG_FORMAT) | lib
	$(CC) $(CFLAGS) -c $< -o $@

lib/darshan-common.po: lib/darshan-common.c darshan.h darshan-common.h $(DARSHAN_LOG_FORMAT) | lib
	$(CC) $(CFLAGS_SHARED) -c $< -o $@

lib/darshan-null.o: lib/darshan-null.c darshan.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-null-log-format.h | lib
	$(CC) $(CFLAGS) -c $< -o $@

lib/darshan-null.po: lib/darshan-null.c darshan.h darshan-dynamic.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-null-log-format.h | lib
	$(CC) $(CFLAGS_SHARED) -c $< -o $@

lib/darshan-posix.o: lib/darshan-posix.c darshan.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-posix-log-format.h | lib
	$(CC) $(CFLAGS) -DDARSHAN_WRAP_MMAP -c $< -o $@

lib/darshan-posix.po: lib/darshan-posix.c darshan.h darshan-dynamic.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-posix-log-format.h | lib
	$(CC) $(CFLAGS_SHARED) -c $< -o $@

lib/darshan-mpiio.o: lib/darshan-mpiio.c darshan.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-mpiio-log-format.h | lib
	$(CC) $(CFLAGS) -c $< -o $@

lib/darshan-mpiio.po: lib/darshan-mpiio.c darshan.h darshan-dynamic.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-mpiio-log-format.h | lib
	$(CC) $(CFLAGS_SHARED) -c $< -o $@

lib/darshan-bgq.o: lib/darshan-bgq.c darshan.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-mpiio-log-format.h | lib
	$(CC) $(CFLAGS) -c $< -o $@

lib/darshan-bgq.po: lib/darshan-bgq.c darshan.h darshan-dynamic.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-mpiio-log-format.h | lib
	$(CC) $(CFLAGS_SHARED) -c $< -o $@

lib/darshan-hdf5.o: lib/darshan-hdf5.c darshan.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-hdf5-log-format.h | lib
	$(CC) $(CFLAGS) -c $< -o $@

lib/darshan-hdf5.po: lib/darshan-hdf5.c darshan.h darshan-dynamic.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-hdf5-log-format.h | lib
	$(CC) $(CFLAGS_SHARED) -c $< -o $@

lib/darshan-pnetcdf.o: lib/darshan-pnetcdf.c darshan.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-pnetcdf-log-format.h | lib
	$(CC) $(CFLAGS) -c $< -o $@

lib/darshan-pnetcdf.po: lib/darshan-pnetcdf.c darshan.h darshan-dynamic.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-pnetcdf-log-format.h | lib
	$(CC) $(CFLAGS_SHARED) -c $< -o $@

lib/darshan-lustre.o: lib/darshan-lustre.c darshan-lustre.h darshan.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-lustre-log-format.h | lib
	$(CC) $(CFLAGS) -c $< -o $@

lib/darshan-lustre.po: lib/darshan-lustre.c darshan-lustre.h darshan.h darshan-dynamic.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-lustre-log-format.h | lib
	$(CC) $(CFLAGS_SHARED) -c $< -o $@

lib/darshan-stdio.o: lib/darshan-stdio.c darshan.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-stdio-log-format.h | lib
	$(CC) $(CFLAGS) -c $< -o $@

lib/darshan-stdio.po: lib/darshan-stdio.c darshan.h darshan-dynamic.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-stdio-log-format.h | lib
	$(CC) $(CFLAGS_SHARED) -c $< -o $@

lib/darshan-dxt.o: lib/darshan-dxt.c darshan.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-dxt-log-format.h | lib
	$(CC) $(CFLAGS) -c $< -o $@

lib/darshan-dxt.po: lib/darshan-dxt.c darshan.h darshan-dynamic.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-dxt-log-format.h | lib
	$(CC) $(CFLAGS_SHARED) -c $< -o $@

lib/darshan-mdhim.o: lib/darshan-mdhim.c darshan.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-mdhim-log-format.h | lib
	$(CC) $(CFLAGS) -c $< -o $@

lib/darshan-mdhim.po: lib/darshan-mdhim.c darshan.h darshan-dynamic.h darshan-common.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-mdhim-log-format.h | lib
	$(CC) $(CFLAGS_SHARED) -c $< -o $@


lib/lookup3.o: lib/lookup3.c
	$(CC) $(CFLAGS) -c $< -o $@

lib/lookup3.po: lib/lookup3.c
	$(CC) $(CFLAGS_SHARED) -c $< -o $@

lib/lookup8.o: lib/lookup8.c
	$(CC) $(CFLAGS) -c $< -o $@

lib/lookup8.po: lib/lookup8.c
	$(CC) $(CFLAGS_SHARED) -c $< -o $@

lib/libdarshan.a: lib/darshan-core-init-finalize.o lib/darshan-core.o lib/darshan-common.o $(DARSHAN_STATIC_MOD_OBJS) lib/lookup3.o lib/lookup8.o
	ar rcs $@ $^

lib/libdarshan.so: lib/darshan-core-init-finalize.po lib/darshan-core.po lib/darshan-common.po $(DARSHAN_DYNAMIC_MOD_OBJS) lib/lookup3.po lib/lookup8.po
	$(CC) $(CFLAGS_SHARED) $(LDFLAGS) -o $@ $^ $(LIBS)


install:: all
	install -d $(DESTDIR)$(libdir)
ifndef DISABLE_STATIC_LIB
	install -m 755 lib/libdarshan.a $(DESTDIR)$(libdir)
endif
ifndef DISABLE_LDPRELOAD
	install -m 755 lib/libdarshan.so $(DESTDIR)$(libdir)
endif
	install -d $(DESTDIR)$(bindir)
	install -m 755 darshan-mk-log-dirs.pl $(DESTDIR)$(bindir)
	install -m 755 darshan-gen-cc.pl $(DESTDIR)$(bindir)
	install -m 755 darshan-gen-cxx.pl $(DESTDIR)$(bindir)
	install -m 755 darshan-gen-fortran.pl $(DESTDIR)$(bindir)
	install -m 755 darshan-config $(DESTDIR)$(bindir)
	install -d $(DESTDIR)$(datarootdir)/craype-1.x/modulefiles/darshan
	install -m 755 share/craype-1.x/darshan-module $(DESTDIR)$(datarootdir)/craype-1.x/modulefiles/darshan/$(DARSHAN_VERSION)
	install -d $(DESTDIR)$(datarootdir)/craype-2.x/modulefiles/darshan
	install -m 755 share/craype-2.x/darshan-module $(DESTDIR)$(datarootdir)/craype-2.x/modulefiles/darshan/$(DARSHAN_VERSION)
	install -d $(DESTDIR)$(datarootdir)/mpi-profile
	install -m 755 share/mpi-profile/darshan-cc.conf $(DESTDIR)$(datarootdir)/mpi-profile/darshan-cc.conf
	install -m 755 share/mpi-profile/darshan-cxx.conf $(DESTDIR)$(datarootdir)/mpi-profile/darshan-cxx.conf
	install -m 755 share/mpi-profile/darshan-f.conf $(DESTDIR)$(datarootdir)/mpi-profile/darshan-f.conf
	install -m 755 share/mpi-profile/darshan-cc-static.conf $(DESTDIR)$(datarootdir)/mpi-profile/darshan-cc-static.conf
	install -m 755 share/mpi-profile/darshan-cxx-static.conf $(DESTDIR)$(datarootdir)/mpi-profile/darshan-cxx-static.conf
	install -m 755 share/mpi-profile/darshan-f-static.conf $(DESTDIR)$(datarootdir)/mpi-profile/darshan-f-static.conf
	install -d $(DESTDIR)$(datarootdir)/ld-opts
	install -m 644 share/ld-opts/darshan-base-ld-opts $(DESTDIR)$(datarootdir)/ld-opts/darshan-base-ld-opts
ifdef BUILD_POSIX_MODULE
	install -m 644 $(srcdir)/share/ld-opts/darshan-posix-ld-opts $(DESTDIR)$(datarootdir)/ld-opts/darshan-posix-ld-opts
endif
ifdef BUILD_STDIO_MODULE
	install -m 644 share/ld-opts/darshan-stdio-ld-opts $(DESTDIR)$(datarootdir)/ld-opts/darshan-stdio-ld-opts
endif
ifdef BUILD_MPIIO_MODULE
	install -m 644 $(srcdir)/share/ld-opts/darshan-mpiio-ld-opts $(DESTDIR)$(datarootdir)/ld-opts/darshan-mpiio-ld-opts
endif
ifdef BUILD_PNETCDF_MODULE
	install -m 644 $(srcdir)/share/ld-opts/darshan-pnetcdf-ld-opts $(DESTDIR)$(datarootdir)/ld-opts/darshan-pnetcdf-ld-opts
endif
ifdef BUILD_HDF5_MODULE
	install -m 644 share/ld-opts/darshan-hdf5-ld-opts $(DESTDIR)$(datarootdir)/ld-opts/darshan-hdf5-ld-opts
endif
ifdef BUILD_MDHIM_MODULE
	install -m 644 $(srcdir)/share/ld-opts/darshan-mdhim-ld-opts $(DESTDIR)$(datarootdir)/ld-opts/darshan-mdhim-ld-opts
endif
ifdef BUILD_APMPI_MODULE
	install -m 644 $(srcdir)/../modules/autoperf/apmpi/share/ld-opts/autoperf-apmpi-ld-opts $(DESTDIR)$(datarootdir)/ld-opts/autoperf-apmpi-ld-opts
endif
ifdef ENABLE_MMAP_LOGS
	install -m 755 share/darshan-mmap-epilog.sh $(DESTDIR)$(datarootdir)/darshan-mmap-epilog.sh
endif
	install -d $(DESTDIR)$(libdir)/pkgconfig
	install -m 644 lib/pkgconfig/darshan-runtime.pc $(DESTDIR)$(libdir)/pkgconfig/darshan-runtime.pc

clean::
	rm -f *.o *.a lib/*.o lib/*.po lib/*.a lib/*.so

distclean:: clean
	rm -f darshan-runtime-config.h darshan-gen-cxx.pl darshan-gen-fortran.pl darshan-gen-cc.pl darshan-mk-log-dirs.pl darshan-config lib/pkgconfig/darshan-runtime.pc share/craype-1.x/darshan-module share/craype-2.x/darshan-module share/darshan-mmap-epilog.sh share/ld-opts/darshan-base-ld-opts share/mpi-profile/darshan-bg-cc.conf share/mpi-profile/darshan-bg-cxx.conf share/mpi-profile/darshan-bg-f.conf share/mpi-profile/darshan-cc.conf share/mpi-profile/darshan-cxx.conf share/mpi-profile/darshan-f.conf aclocal.m4 autom4te.cache/* config.status config.log Makefile
	rm -rf autom4te.cache
