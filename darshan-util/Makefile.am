# See COPYRIGHT notice in top-level directory.
#
# @configure_input@

ACLOCAL_AMFLAGS = -I ../maint/config

AM_CPPFLAGS = -I${top_srcdir}/../include

SUBDIRS = darshan-job-summary

lib_LTLIBRARIES = libdarshan-util.la

libdarshan_util_la_SOURCES = darshan-null-logutils.c \
                             darshan-logutils.c \
                             darshan-posix-logutils.c \
                             darshan-mpiio-logutils.c \
                             darshan-hdf5-logutils.c \
                             darshan-pnetcdf-logutils.c \
                             darshan-bgq-logutils.c \
                             darshan-lustre-logutils.c \
                             darshan-stdio-logutils.c \
                             darshan-dxt-logutils.c \
                             darshan-mdhim-logutils.c

include_HEADERS = darshan-null-logutils.h \
                  darshan-logutils.h \
                  darshan-posix-logutils.h \
                  darshan-mpiio-logutils.h \
                  darshan-hdf5-logutils.h \
                  darshan-pnetcdf-logutils.h \
                  darshan-bgq-logutils.h \
                  darshan-lustre-logutils.h \
                  darshan-stdio-logutils.h \
                  darshan-dxt-logutils.h \
                  darshan-mdhim-logutils.h \
		  ../include/darshan-bgq-log-format.h \
                  ../include/darshan-dxt-log-format.h \
                  ../include/darshan-hdf5-log-format.h \
                  ../include/darshan-log-format.h \
                  ../include/darshan-lustre-log-format.h \
                  ../include/darshan-mdhim-log-format.h \
                  ../include/darshan-mpiio-log-format.h \
                  ../include/darshan-null-log-format.h \
                  ../include/darshan-pnetcdf-log-format.h \
                  ../include/darshan-posix-log-format.h \
                  ../include/darshan-stdio-log-format.h

bin_PROGRAMS = darshan-analyzer \
               darshan-convert \
               darshan-diff \
               darshan-parser \
               darshan-dxt-parser \
               darshan-merge

noinst_PROGRAMS = jenkins-hash-gen

# LIBS += @LIBBZ2@

jenkins_hash_gen_SOURCES = jenkins-hash-gen.c lookup3.c
jenkins_hash_gen_LDADD = libdarshan-util.la

darshan_analyzer_SOURCES = darshan-analyzer.c
darshan_analyzer_LDADD = libdarshan-util.la

darshan_convert_SOURCES = darshan-convert.c lookup3.c
darshan_convert_LDADD = libdarshan-util.la

darshan_diff_SOURCES = darshan-diff.c
darshan_diff_LDADD = libdarshan-util.la

darshan_parser_SOURCES = darshan-parser.c
darshan_parser_LDADD = libdarshan-util.la

darshan_dxt_parser_SOURCES = darshan-dxt-parser.c
darshan_dxt_parser_LDADD = libdarshan-util.la

darshan_merge_SOURCES = darshan-merge.c
darshan_merge_LDADD = libdarshan-util.la

BUILT_SOURCES = uthash-1.9.2

uthash-1.9.2:
	tar xjf $(srcdir)/extern/uthash-1.9.2.tar.bz2

nobase_include_HEADERS = uthash-1.9.2/src/uthash.h

CLEANFILES =
apxc_root = $(top_srcdir)/../modules/autoperf/apxc
if DARSHAN_USE_APXC
   include_HEADERS += $(apxc_root)/darshan-apxc-log-format.h \
                      $(apxc_root)/util/darshan-apxc-logutils.h
   BUILT_SOURCES += darshan-apxc-logutils.c
   CLEANFILES += darshan-apxc-logutils.c
   libdarshan_util_la_SOURCES += darshan-apxc-logutils.c
   AM_CPPFLAGS += -DDARSHAN_USE_APXC \
                  -I$(apxc_root) -I$(apxc_root)/util
endif
darshan-apxc-logutils.c:
	$(LN_S) $(apxc_root)/util/darshan-apxc-logutils.c .

apmpi_root = $(top_srcdir)/../modules/autoperf/apmpi
if DARSHAN_USE_APMPI
   include_HEADERS += $(apmpi_root)/darshan-apmpi-log-format.h \
                      $(apmpi_root)/util/darshan-apmpi-logutils.h
   BUILT_SOURCES += darshan-apmpi-logutils.c
   CLEANFILES += darshan-apmpi-logutils.c
   libdarshan_util_la_SOURCES += darshan-apmpi-logutils.c
   AM_CPPFLAGS += -DDARSHAN_USE_APMPI \
                  -I$(apmpi_root) -I$(apmpi_root)/util
endif
darshan-apmpi-logutils.c:
	$(LN_S) $(apmpi_root)/util/darshan-apmpi-logutils.c .

distclean-local: distclean-local-check
distclean-local-check:
	-rm -rf uthash-1.9.2

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = maint/darshan-util.pc

EXTRA_DIST = $(bin_SCRIPTS) $(include_HEADERS) \
             pydarshan \
             extern/uthash-1.9.2.tar.bz2

bin_SCRIPTS = darshan-summary-per-file.sh\
              dxt_analyzer.py

if DARSHAN_ENABLE_PYDARSHAN
bin_SCRIPTS += pydarshan-info.py \
               pydarshan-info.sh

install-exec-hook:
	cd $(top_srcdir)/pydarshan && \
        export PYTHONPATH=@DARSHAN_PYDARSHAN_PATH@:$(PYTHONPATH) && \
        @PYTHON@ setup.py install --prefix=$(DESTDIR)$(libdir)/pydarshan
endif

.PHONY: clean-local-check
