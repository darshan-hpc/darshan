#!/bin/bash
#
# Base configure script which calls the system specific version.
#

status=0

if [[ $NODE_LABELS =~ "mcs" ]];
then
  source darshan-test/automated/mcs/configure.sh
  status=$?
else
  # unknown machine
  status=100
fi

exit $status
