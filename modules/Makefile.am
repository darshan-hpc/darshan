#
# See COPYRIGHT notice in top-level directory.
#
# @configure_input@

apmpi_c_src = $(srcdir)/autoperf/apmpi/lib/darshan-apmpi.c
apmpi_c_patched = $(srcdir)/patched_darshan-apmpi.c
apmpi_c_saved = $(srcdir)/saved_darshan-apmpi.c
apmpi_c_orig = $(srcdir)/orig_darshan-apmpi.c

# Rule to apply the patch
$(apmpi_c_saved): $(apmpi_c_src) $(apmpi_c_patched)
	cp -f $(apmpi_c_src) $(apmpi_c_saved)
	cp -f $(apmpi_c_patched) $(apmpi_c_src)
	touch $(apmpi_c_saved)

BUILT_SOURCES = $(apmpi_c_saved)

EXTRA_DIST = patched_darshan-apmpi.c orig_darshan-apmpi.c

clean:
	rm -f $(apmpi_c_saved)
	cp -f $(apmpi_c_orig) $(apmpi_c_src)

