#!/bin/bash
#
# Build darshan runtime and util code
#
fcount=0
runtime_status=0
util_status=0
runtime_result=""
util_result=""
thedate=$(date)

cd build/darshan-runtime
make && make install
runtime_status=$?
if [ $runtime_status -ne 0 ]; then
  fcount=$((fcount+1));
  runtime_result="<error type='$runtime_status' message='build failed' />"
fi

cd ../../build/darshan-util
make && make install
util_status=$?
if [ $util_status -ne 0 ]; then
  fcount=$((fcount+1));
  util_result="<error type='$util_status' message='build failed' />"
fi

cd ../../;

echo "
<testsuites>
  <testsuite name='build' tests='2' failures='$fcount' time='$thedate'>
    <testcase name='darshan-runtime' time='$thedate'>
    $runtime_result
    </testcase>
    <testcase name='darshan-util' time='$thedate'>
    $util_result
    </testcase>
  </testsuite>
</testsuites>
" > build-result.xml

return $fcount
