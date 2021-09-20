#
# See COPYRIGHT notice in top-level directory.
#
# @configure_input@

ld_optsdir = $(datadir)/ld-opts

dist_ld_opts_DATA =
nodist_ld_opts_DATA = darshan-ld-opts
BUILT_SOURCES =
CLEANFILES = darshan-ld-opts

if BUILD_POSIX_MODULE
   dist_ld_opts_DATA += darshan-posix-ld-opts
endif
if BUILD_STDIO_MODULE
   nodist_ld_opts_DATA += darshan-stdio-ld-opts
endif
if BUILD_MPIIO_MODULE
   dist_ld_opts_DATA += darshan-mpiio-ld-opts
endif
if BUILD_PNETCDF_MODULE
   dist_ld_opts_DATA += darshan-pnetcdf-ld-opts
endif
if BUILD_HDF5_MODULE
   nodist_ld_opts_DATA += darshan-hdf5-ld-opts
endif
if BUILD_MDHIM_MODULE
   dist_ld_opts_DATA += darshan-mdhim-ld-opts
endif
if BUILD_APMPI_MODULE
   nodist_ld_opts_DATA += autoperf-apmpi-ld-opts
   BUILT_SOURCES += autoperf-apmpi-ld-opts
   CLEANFILES += autoperf-apmpi-ld-opts
endif
apmpi_root = $(top_srcdir)/../modules/autoperf/apmpi
autoperf-apmpi-ld-opts:
	$(LN_S) $(apmpi_root)/share/ld-opts/autoperf-apmpi-ld-opts ./

darshan-ld-opts: darshan-base-ld-opts
	cat $< > $@
if BUILD_POSIX_MODULE
	echo '@$(datadir)/ld-opts/darshan-posix-ld-opts' >> $@
endif
if BUILD_STDIO_MODULE
	echo '@$(datadir)/ld-opts/darshan-stdio-ld-opts' >> $@
endif
if BUILD_MPIIO_MODULE
	echo '@$(datadir)/ld-opts/darshan-mpiio-ld-opts' >> $@
endif
if BUILD_PNETCDF_MODULE
	echo '@$(datadir)/ld-opts/darshan-pnetcdf-ld-opts' >> $@
endif
if BUILD_HDF5_MODULE
	echo '@$(datadir)/ld-opts/darshan-hdf5-ld-opts' >> $@
endif
if BUILD_MDHIM_MODULE
	echo '@$(datadir)/ld-opts/darshan-mdhim-ld-opts' >> $@
endif
if BUILD_APMPI_MODULE
	echo '@$(datadir)/ld-opts/autoperf-apmpi-ld-opts' >> $@
endif

EXTRA_DIST = darshan-base-ld-opts \
             darshan-hdf5-ld-opts.in \
             darshan-stdio-ld-opts.in

