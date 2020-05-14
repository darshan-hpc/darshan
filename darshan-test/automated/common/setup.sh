#!/bin/bash
#
# Create  build, install and log directories.
#

status=0
thedate=$(date)

mkdir -p install
status=$((status + $?))

mkdir -p logs
status=$((status + $?))

mkdir -p build/darshan-runtime
status=$((status + $?))

mkdir -p build/darshan-util
status=$((status + $?))

echo "
<testsuites>
  <testsuite name='setup' tests='1' errors='$status' time='$thedate'>
    <testcase name='setup' time='$thedate'>
    </testcase>
  </testsuite>
</testsuites>
" > setup-result.xml

return $status
