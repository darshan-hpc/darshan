#
# See COPYRIGHT notice in top-level directory.
#
# @configure_input@

apmpi_c_src = $(srcdir)/autoperf/apmpi/lib/darshan-apmpi.c
apmpi_c_saved = $(srcdir)/autoperf/apmpi/lib/saved_darshan-apmpi.c
apmpi_c_patch = $(srcdir)/apmpi.patch

# Rule to apply the patch
$(apmpi_c_saved): $(apmpi_c_src) $(apmpi_c_patch)
	cp -f $(apmpi_c_src) $(apmpi_c_saved)
	patch -f -p0 $(apmpi_c_src) < $(apmpi_c_patch)
	touch $@

BUILT_SOURCES = $(apmpi_c_saved)

EXTRA_DIST = apmpi.patch

clean: $(apmpi_c_saved)
	cp -f $(apmpi_c_saved) $(apmpi_c_src)
	rm -f $(apmpi_c_saved) $(apmpi_c_src).*

