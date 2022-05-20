#!/bin/bash
set -e -u -x

mkdir -p /io/wheelhouse/${PLAT}

function repair_wheel {
    wheel="$1"
    if ! auditwheel show "$wheel"; then
        echo "Skipping non-platform wheel $wheel"
    else
        auditwheel repair "$wheel" --plat "$PLAT" -w /io/wheelhouse/${PLAT}/
    fi
}


# Install a system package required by darshan-utils
yum install -y zlib zlib-devel
yum install -y libjpeg libjpeg-devel

# Build and install darshan-util
cd /darshan/
./prepare.sh
./configure --disable-darshan-runtime --enable-apxc-mod --enable-apmpi-mod
make install
make distclean

cd /


# Uncomment any of the following lines to exclude python variant from build process
#rm -f /opt/python/cp36-cp36m
#rm -f /opt/python/cp37-cp37m
#rm -f /opt/python/cp38-cp38
#rm -f /opt/python/cp39-cp39
#rm -f /opt/python/cp310-cp310
#
#rm -f /opt/python/pp37-pypy37_pp73
#rm -f /opt/python/pp38-pypy38_pp73
#rm -f /opt/python/pp39-pypy39_pp73


ls /opt/python

# Force setup.py to build the C extension
export PYDARSHAN_BUILD_EXT=1

# Compile wheels
for PYBIN in /opt/python/*/bin; do
    # JL: we do not really need any dependencies to build the wheel,
    #     but requirements install needs to be renabled when testing automatically
    #"${PYBIN}/pip" install -r /io/requirements_wheels.txt
    "${PYBIN}/pip" wheel /io/ --no-deps -w /io/wheelhouse/${PLAT}
done

# Bundle external shared libraries into the wheels
for whl in /io/wheelhouse/${PLAT}/*.whl; do
    repair_wheel "$whl"
done

## Install packages and test
#for PYBIN in /opt/python/*/bin/; do
#    "${PYBIN}/pip" install darshan --no-index -f /io/wheelhouse/${PLAT}
#    (cd "$HOME"; "${PYBIN}/nosetests" darshan)
#done
