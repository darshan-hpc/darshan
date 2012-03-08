#!/bin/bash
#
# Base build script which calls the system specific version.
#

status=0

if [[ $NODE_LABELS =~ "magellan" ]];
then
  source darshan-test/automated/magellan/build.sh
  status=$?
else
  # unknown machine
  status=100
fi

exit $status
