#!/bin/sh
#
#  (C) 2009 by Argonne National Laboratory.
#      See COPYRIGHT in top-level directory.
#

# This script greps through text to see if it contains a reference to any of
# the popular profiling libraries that use PMPI.  Darshan will use this to
# detect the use of such libraries at compile time and disable the Darshan
# PMPI libraries.

# known libraries: fpmpi, mpe, tau, mpiP, hpct
grep -E \(fpmpi\)\|\(mpe\)\|\(tau\)\|\(TAU\)\|\(mpiP\)\|\(hpm\)\|\(mpitrace\)
