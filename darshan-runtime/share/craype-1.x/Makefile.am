#
# See COPYRIGHT notice in top-level directory.
#
# @configure_input@

darshan_moduledir = $(datadir)/craype-1.x/modulefiles/darshan

$(DARSHAN_VERSION): darshan-module
	cp $< $@

nodist_darshan_module_DATA = $(DARSHAN_VERSION)

CLEANFILES = $(DARSHAN_VERSION)

EXTRA_DIST = darshan-module.in

