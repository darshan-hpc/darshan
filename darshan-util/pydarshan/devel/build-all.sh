#!/bin/bash

rm -rf wheelhouse
mkdir -p wheelhouse


MNT_PYDARSHAN=`pwd`
MNT_DARSHAN=`pwd`/../..



DOCKER_IMAGE=quay.io/pypa/manylinux1_x86_64
PLAT=manylinux1_x86_64
PRE_CMD=

docker pull $DOCKER_IMAGE
docker run --rm -e PLAT=$PLAT \
	-v $MNT_PYDARSHAN:/io \
	-v $MNT_DARSHAN:/darshan \
	$DOCKER_IMAGE $PRE_CMD /io/devel/build-wheels.sh


exit


DOCKER_IMAGE=quay.io/pypa/manylinux1_i686
PLAT=manylinux1_i686
PRE_CMD=linux32

docker pull $DOCKER_IMAGE
docker run --rm -e PLAT=$PLAT -v $MNT_PYDARSHAN:/io -v $MNT_DARSHAN:/darshan $DOCKER_IMAGE $PRE_CMD /io/devel/build-wheels.sh



DOCKER_IMAGE=quay.io/pypa/manylinux2010_x86_64
PLAT=manylinux2010_x86_64
PRE_CMD=

docker pull $DOCKER_IMAGE
docker run --rm -e PLAT=$PLAT -v $MNT_PYDARSHAN:/io -v $MNT_DARSHAN:/darshan $DOCKER_IMAGE $PRE_CMD /io/devel/build-wheels.sh
