#
# See COPYRIGHT notice in top-level directory.
#
# @configure_input@

SUBDIRS = ld-opts craype-1.x craype-2.x mpi-profile

darshan_mmap_epilog_shdir = $(datadir)

if ENABLE_MMAP_LOGS
   nodist_darshan_mmap_epilog_sh_DATA = darshan-mmap-epilog.sh
endif

EXTRA_DIST = darshan-mmap-epilog.sh.in

