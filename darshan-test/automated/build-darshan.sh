#!/bin/bash
#
# Generic script to automate building of Darshan
#

# Exit immediately if a command exits with a non-zero status.
# set -e

basedir=$PWD
build_dir=$PWD/darshan_build
script_dir=$(dirname $(realpath $0))
darshan_root_dir=$(dirname $(dirname $script_dir))

if [ -z "${DARSHAN_INSTALL_PREFIX}" ]; then
    DARSHAN_INSTALL_PREFIX=$basedir/darshan_install
fi
if [ -z "${DARSHAN_LOG_PATH}" ]; then
    DARSHAN_LOG_PATH=$basedir/darshan_logs
fi

# update generated configuration files
cd $darshan_root_dir && ./prepare.sh && cd - > /dev/null

# create log file directory
mkdir -p $DARSHAN_LOG_PATH

# configure and build darshan-runtime (if not requested to skip)
if [ ! -v DARSHAN_RUNTIME_SKIP ]; then
    mkdir -p $build_dir/darshan-runtime && cd $build_dir/darshan-runtime
    if [ -z "${DARSHAN_RUNTIME_CONFIG_ARGS}" ]; then
        DARSHAN_RUNTIME_CONFIG_ARGS="--with-jobid-env=NONE --enable-apmpi-mod"
    fi
    $darshan_root_dir/darshan-runtime/configure $DARSHAN_RUNTIME_CONFIG_ARGS --with-log-path=$DARSHAN_LOG_PATH --prefix=$DARSHAN_INSTALL_PREFIX
    make -j8 install
fi

# configure and build darshan-util
mkdir -p $build_dir/darshan-util && cd $build_dir/darshan-util
if [ -z "${DARSHAN_UTIL_CONFIG_ARGS}" ]; then
    DARSHAN_UTIL_CONFIG_ARGS="--enable-apmpi-mod --enable-apxc-mod"
fi
$darshan_root_dir/darshan-util/configure $DARSHAN_UTIL_CONFIG_ARGS --prefix=$DARSHAN_INSTALL_PREFIX
make -j8 install

# run check
make check

# run "make check" for darshan-runtime must be done after darshan-util has been
# built, as it used darshan-parser.
if [ ! -v DARSHAN_RUNTIME_SKIP ]; then
    cd $build_dir/darshan-runtime

    # disable errexit, so we can dump make check log file
    # unset +e

    # run check
    make check

    check_status=$?

    # dump make check log file
    cat test/tst_runs.log

    if test "x$check_status" != x0 ; then
       exit $check_status
    fi
fi

