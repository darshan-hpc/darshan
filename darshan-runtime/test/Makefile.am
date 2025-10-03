#
# See COPYRIGHT notice in top-level directory.
#
# @configure_input@

AM_CPPFLAGS  = -I$(top_builddir)

check_PROGRAMS =

if BUILD_MPIIO_MODULE
   check_PROGRAMS += tst_mpi_init \
                     tst_mpi_io

   tst_mpi_init_SOURCES = tst_mpi_init.c
   tst_mpi_io_SOURCES = tst_mpi_io.c

   TESTS = tst_runs.sh
else
   TESTS =
endif

AM_TESTS_ENVIRONMENT  = export TESTMPIRUN="$(TESTMPIRUN)";
AM_TESTS_ENVIRONMENT += export TST_DARSHAN_LOG_PATH="$(__DARSHAN_LOG_PATH)";
AM_TESTS_ENVIRONMENT += export DARSHAN_INSTALL_DIR="$(prefix)";
AM_TESTS_ENVIRONMENT += export USERNAME_ENV="$(USERNAME_ENV)";
AM_TESTS_ENVIRONMENT += export HAVE_MPI_LARGE_COUNT="$(HAVE_MPI_LARGE_COUNT)";
AM_TESTS_ENVIRONMENT += export HAVE_OPEN_MPI="$(HAVE_OPEN_MPI)";
AM_TESTS_ENVIRONMENT += export check_PROGRAMS="$(check_PROGRAMS)";

TEST_EXTENSIONS = .sh

EXTRA_DIST = tst_runs.sh

CLEANFILES = testfile.dat

