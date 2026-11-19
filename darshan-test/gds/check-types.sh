#!/bin/sh
# Dev-only: verify the private cuFile types in darshan-gds-types.h match the
# real cufile.h (needs libcufile-dev; uses a stub cuda.h).  Not part of the build.
set -e
here=$(cd "$(dirname "$0")" && pwd)
lib=$here/../../darshan-runtime/lib
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/stub"
cat > "$tmp/stub/cuda.h" <<'H'
typedef int CUresult;
typedef struct CUstream_st *CUstream;
H
# the private types, with real names under different typedef names
cat > "$tmp/t.c" <<C
#include <stdbool.h>
#include <stddef.h>
#include <cufile.h>
#define CUfileError_t     priv_CUfileError_t
#define CUfileDescr_t     priv_CUfileDescr_t
#define CUfileIOParams_t  priv_CUfileIOParams_t
#define CUfileHandle_t    priv_CUfileHandle_t
#define CUfileBatchHandle_t priv_CUfileBatchHandle_t
#define CUstream priv_CUstream
#undef HAVE_CUFILE_H
#include "$lib/darshan-gds-types.h"
#undef CUfileError_t
#undef CUfileDescr_t
#undef CUfileIOParams_t
#define CHK(T,P,f) _Static_assert(offsetof(T,f)==offsetof(P,f), #T "." #f)
_Static_assert(sizeof(CUfileError_t)==sizeof(priv_CUfileError_t),"error size");
CHK(CUfileError_t,priv_CUfileError_t,err);
CHK(CUfileError_t,priv_CUfileError_t,cu_err);
_Static_assert(sizeof(CUfileDescr_t)==sizeof(priv_CUfileDescr_t),"descr size");
CHK(CUfileDescr_t,priv_CUfileDescr_t,type);
CHK(CUfileDescr_t,priv_CUfileDescr_t,handle);
CHK(CUfileDescr_t,priv_CUfileDescr_t,fs_ops);
_Static_assert(sizeof(CUfileIOParams_t)==sizeof(priv_CUfileIOParams_t),"io size");
CHK(CUfileIOParams_t,priv_CUfileIOParams_t,mode);
CHK(CUfileIOParams_t,priv_CUfileIOParams_t,u);
CHK(CUfileIOParams_t,priv_CUfileIOParams_t,fh);
CHK(CUfileIOParams_t,priv_CUfileIOParams_t,opcode);
CHK(CUfileIOParams_t,priv_CUfileIOParams_t,cookie);
_Static_assert(offsetof(CUfileIOParams_t,u.batch.file_offset)==offsetof(priv_CUfileIOParams_t,u.batch.file_offset),"fo");
_Static_assert(offsetof(CUfileIOParams_t,u.batch.size)==offsetof(priv_CUfileIOParams_t,u.batch.size),"sz");
_Static_assert(CU_FILE_HANDLE_TYPE_OPAQUE_FD==DARSHAN_CU_FILE_HANDLE_TYPE_OPAQUE_FD,"fdtype");
_Static_assert(CUFILE_READ==DARSHAN_GDS_READ && CUFILE_WRITE==DARSHAN_GDS_WRITE,"opcodes");
_Static_assert(CU_FILE_SUCCESS==DARSHAN_CU_FILE_SUCCESS,"success");
int main(void){return 0;}
C
cc -I"$tmp/stub" -I/usr/include -c "$tmp/t.c" -o "$tmp/t.o"
echo "private cuFile types match cufile.h"
