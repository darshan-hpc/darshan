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

# Build and install darshan-util
cd /darshan/darshan-util
./configure --enable-shared --enable-autoperf-apxc --enable-autoperf-apmpi
make install
make distclean

cd /


# Do not build for end-of-life python versions
rm -f /opt/python/cp27-cp27m
rm -f /opt/python/cp27-cp27mu
rm -f /opt/python/cp35-cp35m
ls /opt/python


# Compile wheels
for PYBIN in /opt/python/*/bin; do
    "${PYBIN}/pip" install -r /io/requirements_dev.txt
    "${PYBIN}/pip" wheel /io/ --build-option "--with-extension" --no-deps -w /io/wheelhouse/${PLAT}
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
