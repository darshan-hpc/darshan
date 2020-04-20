#!/bin/bash
#
# Run configure for runtime and utils

basedir=$PWD
status=0
fcount=0
runtime_result=""
util_result=""
thedate=$(date)

# unload any darshan module and use GNU compilers
module unload darshan
module switch PrgEnv-intel PrgEnv-gnu

cd build/darshan-runtime
../../darshan-runtime/configure --prefix=$basedir/install --with-mem-align=64 --with-jobid-env=COBALT_JOBID --with-log-path=$basedir/logs --disable-cuserid --host=x86_64 CC=cc
runtime_status=$?
if [ $runtime_status -ne 0 ]; then
  fcount=$((fcount+1));
  runtime_result="<error type='$runtime_status' message='configure failed' />"
fi

cd ../darshan-util
../../darshan-util/configure --prefix=$basedir/install
util_status=$?
if [ $util_status -ne 0 ]; then
  fcount=$((fcount+1));
  util_result="<error type='$util_status' message='configure failed' />"
fi

cd ../../;

module list

echo "
<testsuites>
  <testsuite name='configure' tests='2' failures='$fcount' time='$thedate'>
    <testcase name='darshan-runtime' time='$thedate'>
    $runtime_result
    </testcase>
    <testcase name='darshan-util' time='$thedate'>
    $util_result
    </testcase>
  </testsuite>
</testsuites>
" > configure-result.xml

return $fcount
