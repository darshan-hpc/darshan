#!/bin/bash
#
# Copyright (C) 2026, Argonne National Laboratory
# See COPYRIGHT notice in top-level directory.
#

# Exit immediately if a command exits with a non-zero status.
set -e

for i in ${check_PROGRAMS} ; do

    cmd=`basename $i`

    if test "x$cmd" = "xdarshan-accumulator" ; then
        # tested through munit
        continue
    fi

    if test "x$cmd" = "xdarshan-get-name-records" ; then
        echo "./$i ${srcdir}/tests/unit-tests/test_log.darshan"
        ./$i ${srcdir}/tests/unit-tests/test_log.darshan
    fi

done # check_PROGRAMS

