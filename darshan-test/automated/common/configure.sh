#!/bin/bash
#
# Base configure script which calls the system specific version.
#

if [[ `hostname` =~ "polaris" ]];
then
  source darshan-test/automated/polaris/configure.sh
else
  # try to use generic workstation config if nothing else matches
  source darshan-test/automated/generic/configure.sh
fi
