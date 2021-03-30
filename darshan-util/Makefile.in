all: libdarshan-util.a darshan-null-logutils.o darshan-analyzer darshan-convert darshan-diff darshan-parser darshan-dxt-parser darshan-merge jenkins-hash-gen

srcdir = @srcdir@
prefix = @prefix@
exec_prefix = @exec_prefix@
datarootdir = @datarootdir@
includedir = @includedir@
mandir = @mandir@
sbindir = @sbindir@
bindir = @bindir@
libdir = @libdir@

DARSHAN_USE_APXC = @DARSHAN_USE_APXC@
DARSHAN_USE_APMPI = @DARSHAN_USE_APMPI@

DARSHAN_LOG_FORMAT = $(srcdir)/../darshan-log-format.h
DARSHAN_MOD_LOG_FORMATS = $(srcdir)/../darshan-posix-log-format.h \
			  $(srcdir)/../darshan-mpiio-log-format.h \
			  $(srcdir)/../darshan-hdf5-log-format.h \
			  $(srcdir)/../darshan-pnetcdf-log-format.h \
			  $(srcdir)/../darshan-bgq-log-format.h \
			  $(srcdir)/../darshan-lustre-log-format.h \
			  $(srcdir)/../darshan-stdio-log-format.h \
			  $(srcdir)/../darshan-dxt-log-format.h \
			  $(srcdir)/../darshan-mdhim-log-format.h

DARSHAN_MOD_LOGUTIL_HEADERS = darshan-posix-logutils.h \
			      darshan-mpiio-logutils.h \
			      darshan-hdf5-logutils.h \
			      darshan-pnetcdf-logutils.h \
			      darshan-bgq-logutils.h \
			      darshan-lustre-logutils.h \
			      darshan-stdio-logutils.h \
			      darshan-dxt-logutils.h \
			      darshan-mdhim-logutils.h

DARSHAN_STATIC_MOD_OBJS = darshan-posix-logutils.o \
			  darshan-mpiio-logutils.o \
			  darshan-hdf5-logutils.o \
			  darshan-pnetcdf-logutils.o \
			  darshan-bgq-logutils.o \
			  darshan-lustre-logutils.o \
			  darshan-stdio-logutils.o \
			  darshan-dxt-logutils.o \
			  darshan-mdhim-logutils.o

DARSHAN_DYNAMIC_MOD_OBJS = darshan-posix-logutils.po \
			   darshan-mpiio-logutils.po \
			   darshan-hdf5-logutils.po \
			   darshan-pnetcdf-logutils.po \
			   darshan-bgq-logutils.po \
			   darshan-lustre-logutils.po \
			   darshan-stdio-logutils.po \
			   darshan-dxt-logutils.po \
			   darshan-mdhim-logutils.po

DARSHAN_ENABLE_SHARED=@DARSHAN_ENABLE_SHARED@
DARSHAN_ENABLE_PYDARSHAN=@DARSHAN_ENABLE_PYDARSHAN@

ifeq ($(DARSHAN_ENABLE_PYDARSHAN),1)
PYTHON=@PYTHON@
DARSHAN_PYDARSHAN_PATH=@DARSHAN_PYDARSHAN_PATH@
endif

VPATH = $(srcdir)

ifeq ($(DARSHAN_ENABLE_SHARED),1)
all: libdarshan-util.so darshan-null-logutils.po
endif
cp_zlib_link_flags = @__DARSHAN_ZLIB_LINK_FLAGS@
cp_zlib_include_flags = @__DARSHAN_ZLIB_INCLUDE_FLAGS@

# deliberately avoid large file support for host side utilities to avoid
# potentially buggy libz 64 bit offset support
CFLAGS = -I . -I .. -I $(srcdir) -I $(srcdir)/../ -DDARSHAN_CONFIG_H=\"darshan-util-config.h\" @CFLAGS@ @CPPFLAGS@ 
CFLAGS_SHARED = $(CFLAGS) -shared -fpic -DPIC 
LDFLAGS=@LDFLAGS@

CC=@CC@
LD=@LD@
AR=@AR@

LIBS = -lz @LIBBZ2@

ifdef DARSHAN_USE_APXC
include $(srcdir)/../modules/autoperf/apxc/util/Makefile.darshan
endif
ifdef DARSHAN_USE_APMPI
include $(srcdir)/../modules/autoperf/apmpi/util/Makefile.darshan
endif

mktestdir::
	mkdir -p test

uthash-1.9.2:
	tar xjvf $(srcdir)/extern/uthash-1.9.2.tar.bz2

darshan-logutils.o: darshan-logutils.c darshan-logutils.h $(DARSHAN_LOG_FORMAT) | uthash-1.9.2
	$(CC) $(CFLAGS) -c  $< -o $@
darshan-logutils.po: darshan-logutils.c darshan-logutils.h $(DARSHAN_LOG_FORMAT) | uthash-1.9.2
	$(CC) $(CFLAGS_SHARED) -c  $< -o $@

darshan-null-logutils.o: darshan-null-logutils.c darshan-logutils.h darshan-null-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-null-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS) -c  $< -o $@
darshan-null-logutils.po: darshan-null-logutils.c darshan-logutils.h darshan-null-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-null-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS_SHARED) -c  $< -o $@

darshan-posix-logutils.o: darshan-posix-logutils.c darshan-logutils.h darshan-posix-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-posix-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS) -c  $< -o $@
darshan-posix-logutils.po: darshan-posix-logutils.c darshan-logutils.h darshan-posix-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-posix-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS_SHARED) -c  $< -o $@

darshan-mpiio-logutils.o: darshan-mpiio-logutils.c darshan-logutils.h darshan-mpiio-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-mpiio-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS) -c  $< -o $@
darshan-mpiio-logutils.po: darshan-mpiio-logutils.c darshan-logutils.h darshan-mpiio-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-mpiio-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS_SHARED) -c  $< -o $@

darshan-hdf5-logutils.o: darshan-hdf5-logutils.c darshan-logutils.h darshan-hdf5-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-hdf5-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS) -c  $< -o $@
darshan-hdf5-logutils.po: darshan-hdf5-logutils.c darshan-logutils.h darshan-hdf5-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-hdf5-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS_SHARED) -c  $< -o $@

darshan-pnetcdf-logutils.o: darshan-pnetcdf-logutils.c darshan-logutils.h darshan-pnetcdf-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-pnetcdf-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS) -c  $< -o $@
darshan-pnetcdf-logutils.po: darshan-pnetcdf-logutils.c darshan-logutils.h darshan-pnetcdf-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-pnetcdf-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS_SHARED) -c  $< -o $@

darshan-bgq-logutils.o: darshan-bgq-logutils.c darshan-logutils.h darshan-bgq-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-bgq-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS) -c  $< -o $@
darshan-bgq-logutils.po: darshan-bgq-logutils.c darshan-logutils.h darshan-bgq-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-bgq-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS_SHARED) -c  $< -o $@

darshan-lustre-logutils.o: darshan-lustre-logutils.c darshan-logutils.h darshan-lustre-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-lustre-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS) -c  $< -o $@
darshan-lustre-logutils.po: darshan-lustre-logutils.c darshan-logutils.h darshan-lustre-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-lustre-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS_SHARED) -c  $< -o $@

darshan-stdio-logutils.o: darshan-stdio-logutils.c darshan-logutils.h darshan-stdio-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-stdio-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS) -c  $< -o $@
darshan-stdio-logutils.po: darshan-stdio-logutils.c darshan-logutils.h darshan-stdio-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-stdio-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS_SHARED) -c  $< -o $@

darshan-dxt-logutils.o: darshan-dxt-logutils.c darshan-logutils.h darshan-dxt-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-dxt-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS) -c  $< -o $@
darshan-dxt-logutils.po: darshan-dxt-logutils.c darshan-logutils.h darshan-dxt-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-dxt-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS_SHARED) -c  $< -o $@

darshan-mdhim-logutils.o: darshan-mdhim-logutils.c darshan-logutils.h darshan-mdhim-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-mdhim-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS) -c  $< -o $@
darshan-mdhim-logutils.po: darshan-mdhim-logutils.c darshan-logutils.h darshan-mdhim-logutils.h $(DARSHAN_LOG_FORMAT) $(srcdir)/../darshan-mdhim-log-format.h | uthash-1.9.2
	$(CC) $(CFLAGS_SHARED) -c  $< -o $@


libdarshan-util.a: darshan-logutils.o $(DARSHAN_STATIC_MOD_OBJS)
	ar rcs libdarshan-util.a $^

libdarshan-util.so: darshan-logutils.po $(DARSHAN_DYNAMIC_MOD_OBJS) 
	$(CC) $(CFLAGS_SHARED) $(LDFLAGS) -o $@ $^ $(LIBS)
	
jenkins-hash-gen: jenkins-hash-gen.c lookup3.o
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o $@ $(LIBS)

lookup3.o: lookup3.c
	$(CC) $(CFLAGS) -c $< -o $@

darshan-analyzer: darshan-analyzer.c darshan-logutils.h $(DARSHAN_LOG_FORMAT) $(DARSHAN_MOD_LOGUTIL_HEADERS) $(DARSHAN_MOD_LOG_FORMATS) libdarshan-util.a | uthash-1.9.2
	$(CC) $(CFLAGS) $(LDFLAGS) $< libdarshan-util.a -o $@ $(LIBS)

darshan-convert: darshan-convert.c darshan-logutils.h $(DARSHAN_LOG_FORMAT) $(DARSHAN_MOD_LOGUTIL_HEADERS) $(DARSHAN_MOD_LOG_FORMATS) libdarshan-util.a lookup3.o | uthash-1.9.2
	$(CC) $(CFLAGS) $(LDFLAGS) $< lookup3.o libdarshan-util.a -o $@ $(LIBS)

darshan-diff: darshan-diff.c darshan-logutils.h $(DARSHAN_LOG_FORMAT) $(DARSHAN_MOD_LOGUTIL_HEADERS) $(DARSHAN_MOD_LOG_FORMATS) libdarshan-util.a | uthash-1.9.2
	$(CC) $(CFLAGS) $(LDFLAGS) $< libdarshan-util.a -o $@ $(LIBS) 

darshan-parser: darshan-parser.c darshan-logutils.h $(DARSHAN_LOG_FORMAT) $(DARSHAN_MOD_LOGUTIL_HEADERS) $(DARSHAN_MOD_LOG_FORMATS) libdarshan-util.a | uthash-1.9.2
	$(CC) $(CFLAGS) $(LDFLAGS) $< libdarshan-util.a -o $@ $(LIBS) 

darshan-dxt-parser: darshan-dxt-parser.c darshan-logutils.h $(DARSHAN_LOG_FORMAT) $(DARSHAN_MOD_LOGUTIL_HEADERS) $(DARSHAN_MOD_LOG_FORMATS) libdarshan-util.a | uthash-1.9.2
	$(CC) $(CFLAGS) $(LDFLAGS) $< libdarshan-util.a -o $@ $(LIBS) 

darshan-merge: darshan-merge.c darshan-logutils.h $(DARSHAN_LOG_FORMAT) $(DARSHAN_MOD_LOGUTIL_HEADERS) $(DARSHAN_MOD_LOG_FORMATS) libdarshan-util.a | uthash-1.9.2
	$(CC) $(CFLAGS) $(LDFLAGS) $< libdarshan-util.a -o $@ $(LIBS)

#test/gztest: test/gztest.c mktestdir
#	$(CC) $(CFLAGS)  $(LDFLAGS) -lz $< -o $@

#test/gz-bench: test/gz-bench.c mktestdir
#	$(CC) $(CFLAGS)  $(LDFLAGS) -lz $< -o $@

install:: all
	install -d $(DESTDIR)$(bindir)
	install -d $(DESTDIR)$(libdir)
	install -d $(DESTDIR)$(includedir)
	install -m 755 darshan-analyzer $(DESTDIR)$(bindir)
	install -m 755 darshan-convert $(DESTDIR)$(bindir)
	install -m 755 darshan-diff $(DESTDIR)$(bindir)
	install -m 755 darshan-parser $(DESTDIR)$(bindir)
	install -m 755 darshan-dxt-parser $(DESTDIR)$(bindir)
	install -m 755 darshan-merge $(DESTDIR)$(bindir)
	install -m 755 $(srcdir)/darshan-summary-per-file.sh $(DESTDIR)$(bindir)
	install -m 755 $(srcdir)/dxt_analyzer.py $(DESTDIR)$(bindir)
	install -m 755 libdarshan-util.a $(DESTDIR)$(libdir)
ifeq ($(DARSHAN_ENABLE_SHARED),1)
	install -m 755 libdarshan-util.so $(DESTDIR)$(libdir)
endif
	install -m 644 $(srcdir)/darshan-logutils.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/darshan-posix-logutils.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/darshan-mpiio-logutils.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/darshan-hdf5-logutils.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/darshan-pnetcdf-logutils.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/darshan-bgq-logutils.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/darshan-null-logutils.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/darshan-lustre-logutils.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/darshan-stdio-logutils.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/darshan-dxt-logutils.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/darshan-mdhim-logutils.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/../darshan-null-log-format.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/../darshan-posix-log-format.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/../darshan-mpiio-log-format.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/../darshan-hdf5-log-format.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/../darshan-pnetcdf-log-format.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/../darshan-bgq-log-format.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/../darshan-lustre-log-format.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/../darshan-stdio-log-format.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/../darshan-dxt-log-format.h $(DESTDIR)$(includedir)
	install -m 644 $(srcdir)/../darshan-mdhim-log-format.h $(DESTDIR)$(includedir)
	install -d $(DESTDIR)$(includedir)/uthash-1.9.2
	install -d $(DESTDIR)$(includedir)/uthash-1.9.2/src
	install -m 644 uthash-1.9.2/src/uthash.h $(DESTDIR)$(includedir)/uthash-1.9.2/src/
	install -m 644 $(DARSHAN_LOG_FORMAT) $(DESTDIR)$(includedir)
	install -m 755 darshan-job-summary/bin/darshan-job-summary.pl $(DESTDIR)$(bindir)
	install -d $(DESTDIR)$(libdir)/TeX
	install -m 644 $(srcdir)/darshan-job-summary/lib/TeX/Encode.pm $(DESTDIR)$(libdir)/TeX/
	install -d $(DESTDIR)$(libdir)/Number
	install -d $(DESTDIR)$(libdir)/Number/Bytes
	install -m 644 $(srcdir)/darshan-job-summary/lib/Number/Bytes/Human.pm $(DESTDIR)$(libdir)/Number/Bytes
	install -d $(DESTDIR)$(libdir)/Pod
	install -m 644 $(srcdir)/darshan-job-summary/lib/Pod/Constants.pm $(DESTDIR)$(libdir)/Pod/
	install -m 644 $(srcdir)/darshan-job-summary/lib/Pod/LaTeX.pm $(DESTDIR)$(libdir)/Pod/
	install -d $(DESTDIR)$(datarootdir)
	install -m 644 $(srcdir)/darshan-job-summary/share/* $(DESTDIR)$(datarootdir)
	install -d $(DESTDIR)$(libdir)/pkgconfig
	install -m 644 maint/darshan-util.pc $(DESTDIR)$(libdir)/pkgconfig/darshan-util.pc
ifeq ($(DARSHAN_ENABLE_PYDARSHAN),1)
	install -d $(DARSHAN_PYDARSHAN_PATH)
	cd $(srcdir)/pydarshan && \
        export PYTHONPATH=$(DARSHAN_PYDARSHAN_PATH):$(PYTHONPATH) && \
        $(PYTHON) setup.py install --prefix=$(DESTDIR)$(libdir)/pydarshan
	install -m 755 pydarshan-info.py $(DESTDIR)$(bindir)
	install -m 755 pydarshan-info.sh $(DESTDIR)$(bindir)
endif


clean::
	rm -f *.o *.po *.a *.so darshan-analyzer darshan-convert darshan-diff darshan-parser darshan-dxt-parser darshan-merge jenkins-hash-gen

distclean:: clean
	rm -f darshan-util-config.h aclocal.m4 autom4te.cache/* config.status config.log Makefile darshan-job-summary/bin/darshan-job-summary.pl maint/darshan-util.pc
	rm -rf uthash-1.9.2
	rm -rf autom4te.cache
