#!/bin/bash

# This script serves to build wheels as layed out by PyPA:
# https://github.com/pypa/manylinux
# It is a direct extract of the python-manylinux-demo repository:
# https://github.com/pypa/python-manylinux-demo
#
# A modernized full CI support build would update this procedure to use:
# https://github.com/pypa/cibuildwheel
#
#
# Allowable platform tags are defined in PEP 599
# This script currently only build for the x86_64 and i686 platforms
#
# Each platform will build for different Python verions.
# Refer to the build-wheels.sh script to review exactly which are build.


# Clean up any leftovers from a previous build
rm -rf wheelhouse
mkdir -p wheelhouse


# Define mount-point in container to find darshan source to that is needed to
# compile libdarshan-util.so.
MNT_PYDARSHAN=`pwd`
MNT_DARSHAN=`pwd`/../..


# x86_64 ######################################################################
PLAT=manylinux2014_x86_64
DOCKER_IMAGE=quay.io/pypa/manylinux2014_x86_64
PRE_CMD=

# On systems with SELinux it may be necessary to set the Z option for volumes.
docker pull $DOCKER_IMAGE
docker run --rm -e PLAT=$PLAT \
	-v $MNT_PYDARSHAN:/io:Z \
	-v $MNT_DARSHAN:/darshan:Z \
	$DOCKER_IMAGE $PRE_CMD /io/devel/build-wheels.sh


# i686 ########################################################################
#PLAT=manylinux2014_i686
#DOCKER_IMAGE=quay.io/pypa/manylinux2014_i686
#PRE_CMD=linux32
#
#docker pull $DOCKER_IMAGE
#docker run --rm -e PLAT=$PLAT \
#	-v $MNT_PYDARSHAN:/io:Z \
#	-v $MNT_DARSHAN:/darshan:Z \
#	$DOCKER_IMAGE $PRE_CMD /io/devel/build-wheels.sh
