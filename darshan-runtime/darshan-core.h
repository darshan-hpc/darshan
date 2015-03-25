/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_CORE_H
#define __DARSHAN_CORE_H

#include <unistd.h>
#include <sys/types.h>
#include <stdint.h>

#include "darshan.h"

/* TODO: this goes where ? -- shared libs */
#define DARSHAN_MPI_CALL(func) func

#define DARSHAN_CORE_MAX_RECORDS 1024

/* TODO: revisit this default size if we change memory per module */
#define DARSHAN_CORE_COMP_BUF_SIZE (2 * 1024 * 1024)

#define DARSHAN_CORE_MOD_SET(flags, id) (flags | (1 << id))
#define DARSHAN_CORE_MOD_UNSET(flags, id) (flags & ~(1 << id))
#define DARSHAN_CORE_MOD_ISSET(flags, id) (flags & (1 << id))

/* in memory structure to keep up with job level data */
struct darshan_core_runtime
{
    struct darshan_header log_header;
    struct darshan_job log_job;
    char exe[DARSHAN_EXE_LEN+1];
    struct darshan_core_record_ref *rec_hash;
    int rec_count;
    struct darshan_core_module* mod_array[DARSHAN_MAX_MODS];
    char comp_buf[DARSHAN_CORE_COMP_BUF_SIZE];
    double wtime_offset;
    char *trailing_data;
};

struct darshan_core_module
{
    darshan_module_id id;
    struct darshan_module_funcs mod_funcs;
};

struct darshan_core_record_ref
{
    struct darshan_record rec;
    uint64_t mod_flags;
    uint64_t global_mod_flags;
    UT_hash_handle hlink;
};

uint32_t darshan_hashlittle(const void *key, size_t length, uint32_t initval);
uint64_t darshan_hash(const register unsigned char *k, register uint64_t length, register uint64_t level);

#endif /* __DARSHAN_CORE_H */
