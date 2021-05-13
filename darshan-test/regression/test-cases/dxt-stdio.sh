#!/bin/bash

PROG=dxt-stdio

# set log file path; remove previous log if present
export DARSHAN_LOGFILE=$DARSHAN_TMP/${PROG}.darshan
rm -f ${DARSHAN_LOGFILE}

# compile
$DARSHAN_CC $DARSHAN_TESTDIR/test-cases/src/${PROG}.c -o $DARSHAN_TMP/${PROG}
if [ $? -ne 0 ]; then
    echo "Error: failed to compile ${PROG}" 1>&2
    exit 1
fi

# enable dxt tracing
export DXT_ENABLE_IO_TRACE=

# execute
$DARSHAN_RUNJOB $DARSHAN_TMP/${PROG} -f $DARSHAN_TMP/${PROG}.tmp.dat
if [ $? -ne 0 ]; then
    echo "Error: failed to execute ${PROG}" 1>&2
    exit 1
fi

# parse log
$DARSHAN_PATH/bin/darshan-dxt-parser $DARSHAN_LOGFILE > $DARSHAN_TMP/${PROG}.darshan.txt
if [ $? -ne 0 ]; then
    echo "Error: failed to parse ${DARSHAN_LOGFILE}" 1>&2
    exit 1
fi

# check results
grep -o 'assert: [^ ]* [^ ]* at [^ ]*' "$DARSHAN_TESTDIR/test-cases/src/${PROG}.c" | sed -e 's/sizeof(int)/4/g' | sort > "$DARSHAN_TMP/${PROG}.truth.txt"
awk '/X_STDIO/ {printf("assert: %s %d at %d\n", $3, $6, $5);}' "$DARSHAN_TMP/${PROG}.darshan.txt" | sort > "$DARSHAN_TMP/${PROG}.observed.txt"
unique_checksums=$(md5sum "$DARSHAN_TMP/${PROG}.truth.txt" "$DARSHAN_TMP/${PROG}.observed.txt" | cut -d' ' -f1 | uniq | wc -l)
if [ "$unique_checksums" -ne 1 ]; then
    echo "Error: dxt trace did not match expectations" 1>&2
    echo "Expected:" 1>&2
    cat "$DARSHAN_TMP/${PROG}.truth.txt" 1>&2
    echo "But got:" 1>&2
    cat "$DARSHAN_TMP/${PROG}.observed.txt" 1>&2
    exit 1
fi

exit 0
