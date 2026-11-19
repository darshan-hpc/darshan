#!/bin/bash
# Usage: run-gds-test.sh <libdarshan.so> <darshan-parser> [workdir]
# Builds a fake libcufile and a test program, runs the program under Darshan
# (non-MPI) and checks exact GDS counters.  Needs a build with --enable-gds-mod.
set -e
here=$(cd "$(dirname "$0")" && pwd)
libdarshan=$(readlink -f "$1"); parser=$(readlink -f "$2")
work=${3:-$(mktemp -d)}; mkdir -p "$work"; work=$(readlink -f "$work")
cc=${CC:-cc}
$cc -fPIC -shared -I"$here/../../darshan-runtime/lib" -o "$work/libfakecufile.so" "$here/fake-cufile.c"
$cc -I"$here/../../darshan-runtime/lib" -o "$work/gds-test" "$here/gds-test.c" -L"$work" -lfakecufile -Wl,-rpath,"$work"
mkdir -p "$work/logs"
rm -f "$work"/logs/*.darshan
(cd "$work" && \
 LD_PRELOAD="$libdarshan" DARSHAN_ENABLE_NONMPI=1 DARSHAN_LOGPATH="$work/logs" DARSHAN_LOGFILE="$work/logs/gds.darshan" \
 ./gds-test "$work/gds-test.dat")
log=$(ls "$work"/logs/*.darshan | head -1)
out="$work/parser.out"
"$parser" "$log" > "$out"

fail=0
check() { # counter expected
    got=$(awk -v c="$1" '$1=="GDS" && $4==c {print $5}' "$out" | head -1)
    if [ "$got" != "$2" ]; then echo "FAIL $1: got '$got' expected '$2'"; fail=1; fi
}
check GDS_HANDLE_REGISTERS 1
check GDS_HANDLE_DEREGISTERS 1
check GDS_WRITES 4
check GDS_READS 3
check GDS_READ_ERRORS 0
check GDS_BYTES_WRITTEN 16384
check GDS_BYTES_READ 12288
check GDS_BATCH_WRITES 1
check GDS_BATCH_READS 1
check GDS_BATCH_BYTES_WRITTEN 8192
check GDS_BATCH_BYTES_READ 512
check GDS_ASYNC_WRITES 1
check GDS_ASYNC_READS 1
check GDS_ASYNC_BYTES_REQ_WRITTEN 1000
check GDS_ASYNC_BYTES_REQ_READ 1000
check GDS_MAX_BYTE_WRITTEN 16383
check GDS_MAX_BYTE_READ 12287
check GDS_SEQ_WRITES 3
check GDS_CONSEC_WRITES 3
check GDS_SEQ_READS 2
check GDS_CONSEC_READS 2
check GDS_RW_SWITCHES 3
# 4096-byte syncs land in 1K_10K; batch/async sizes are included in histograms
check GDS_SIZE_WRITE_1K_10K 5
check GDS_SIZE_READ_100_1K 1
check GDS_ACCESS1_ACCESS 4096

# GDS record id must match the POSIX record id for the same file
gid=$(awk '$1=="GDS" && $4=="GDS_WRITES" {print $3}' "$out" | head -1)
pid=$(awk '$1=="POSIX" && $4=="POSIX_OPENS" {print $3}' "$out" | head -1)
if [ -z "$gid" ] || [ "$gid" != "$pid" ]; then echo "FAIL record id: GDS=$gid POSIX=$pid"; fail=1; fi

if [ $fail -eq 0 ]; then echo "GDS test PASSED"; else echo "parser output: $out"; exit 1; fi
