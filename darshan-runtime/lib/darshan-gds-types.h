/*
 * Copyright (C) 2025 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

/* cuFile types used by the GDS module.  If configure found a cufile.h that
 * compiles, use it; otherwise declare the few types we need privately so the
 * module builds on machines without CUDA.  darshan-gds.c uses only the
 * DARSHAN_* constants and the type names common to both branches.
 */

#ifndef __DARSHAN_GDS_TYPES_H
#define __DARSHAN_GDS_TYPES_H

#include <sys/types.h>
#include <stddef.h>

#ifdef HAVE_CUFILE_H
#include <cufile.h>
#define DARSHAN_CU_FILE_HANDLE_TYPE_OPAQUE_FD CU_FILE_HANDLE_TYPE_OPAQUE_FD
#define DARSHAN_GDS_READ  CUFILE_READ
#define DARSHAN_GDS_WRITE CUFILE_WRITE
#define DARSHAN_CU_FILE_SUCCESS CU_FILE_SUCCESS
#else
/* Private declarations.  Layouts were checked against cufile.h from
 * libcufile-dev 1.9.1 (see darshan-test/gds/check-types.sh).  Struct tags
 * deliberately differ from the real ones so that including both headers by
 * mistake produces a typedef-redefinition error.
 */
typedef void *CUfileHandle_t;
typedef void *CUfileBatchHandle_t;
typedef struct CUstream_st *CUstream;                  /* opaque, as in cuda.h */
typedef struct darshan_cufile_error {
    int err;                                           /* enum CUfileOpError */
    int cu_err;                                        /* CUresult */
} CUfileError_t;
typedef struct darshan_cufile_descr {
    int type;                                          /* enum CUfileFileHandleType */
    union { int fd; void *handle; } handle;
    const void *fs_ops;
} CUfileDescr_t;
typedef struct darshan_cufile_io_params {
    int mode;                                          /* CUfileBatchMode_t, must be first */
    union {
        struct {
            void *devPtr_base;
            off_t file_offset;
            off_t devPtr_offset;
            size_t size;
        } batch;
    } u;
    CUfileHandle_t fh;
    int opcode;                                        /* CUFILE_READ=0, CUFILE_WRITE=1 */
    void *cookie;
} CUfileIOParams_t;
#define DARSHAN_CU_FILE_HANDLE_TYPE_OPAQUE_FD 1
#define DARSHAN_GDS_READ  0
#define DARSHAN_GDS_WRITE 1
#define DARSHAN_CU_FILE_SUCCESS 0
#endif

#endif /* __DARSHAN_GDS_TYPES_H */
