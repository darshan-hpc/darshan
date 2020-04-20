#!/bin/bash
#
# Base configure script which calls the system specific version.
#

status=0

if [[ $NODE_LABELS =~ "CINOW" || $NODE_LABELS =~ "mcs" ]];
then
  source darshan-test/automated/generic/configure.sh
  status=$?
elif [[ $NODE_LABELS =~ "Theta" ]];
then
  source darshan-test/automated/theta/configure.sh
  status=$?
else
  # unknown machine
  status=100
fi

return $status
