#!/bin/bash

rm -rf wheelhouse
mkdir -p wheelhouse


MNT_PYDARSHAN=`pwd`
MNT_DARSHAN=`pwd`/../..



#DOCKER_IMAGE=quay.io/pypa/manylinux1_x86_64
DOCKER_IMAGE=quay.io/pypa/manylinux1_x86_64:2021-05-01-78330c5
PLAT=manylinux1_x86_64
PRE_CMD=

# On systems with SELinux it may be necessary to set the Z option for volumes.
docker pull $DOCKER_IMAGE
docker run --rm -e PLAT=$PLAT \
	-v $MNT_PYDARSHAN:/io:Z \
	-v $MNT_DARSHAN:/darshan:Z \
	$DOCKER_IMAGE $PRE_CMD /io/devel/build-wheels.sh



#DOCKER_IMAGE=quay.io/pypa/manylinux1_i686
DOCKER_IMAGE=quay.io/pypa/manylinux1_i686:2021-05-01-78330c5
PLAT=manylinux1_i686
PRE_CMD=linux32

docker pull $DOCKER_IMAGE
docker run --rm -e PLAT=$PLAT \
	-v $MNT_PYDARSHAN:/io:Z \
	-v $MNT_DARSHAN:/darshan:Z \
	$DOCKER_IMAGE $PRE_CMD /io/devel/build-wheels.sh



#DOCKER_IMAGE=quay.io/pypa/manylinux2010_x86_64
DOCKER_IMAGE=quay.io/pypa/manylinux2010_x86_64:2021-05-01-28d233a
PLAT=manylinux2010_x86_64
PRE_CMD=

docker pull $DOCKER_IMAGE
docker run --rm -e PLAT=$PLAT \
	-v $MNT_PYDARSHAN:/io:Z \
	-v $MNT_DARSHAN:/darshan:Z \
	$DOCKER_IMAGE $PRE_CMD /io/devel/build-wheels.sh

