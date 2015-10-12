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

#include "uthash.h"
#include "darshan-log-format.h"

/* Environment variable to override __DARSHAN_JOBID */
#define DARSHAN_JOBID_OVERRIDE "DARSHAN_JOBID"

/* Environment variable to override __DARSHAN_LOG_PATH */
#define DARSHAN_LOG_PATH_OVERRIDE "DARSHAN_LOGPATH"

/* Environment variable to override __DARSHAN_LOG_HINTS */
#define DARSHAN_LOG_HINTS_OVERRIDE "DARSHAN_LOGHINTS"

/* Environment variable to override __DARSHAN_MEM_ALIGNMENT */
#define DARSHAN_MEM_ALIGNMENT_OVERRIDE "DARSHAN_MEMALIGN"

/* maximum number of records that can be tracked on a single process */
#define DARSHAN_CORE_MAX_RECORDS 2048

/* TODO: revisit this default size if we change memory per module */
#define DARSHAN_CORE_COMP_BUF_SIZE (2 * 1024 * 1024)

/* this controls the maximum mmapped memory each module can use */
#define DARSHAN_MMAP_CHUNK_SIZE (4 * 1024)

#define DARSHAN_CORE_MOD_SET(flags, id) (flags | (1 << id))
#define DARSHAN_CORE_MOD_UNSET(flags, id) (flags & ~(1 << id))
#define DARSHAN_CORE_MOD_ISSET(flags, id) (flags & (1 << id))

/* in memory structure to keep up with job level data */
struct darshan_core_runtime
{
    /* XXX-MMAP */
    void *mmap_p;
    struct darshan_job *mmap_job_p;
    char *mmap_exe_mnt_p;
    void *mmap_mod_p;
    /* XXX-MMAP */

    struct darshan_core_record_ref *rec_hash;
    int rec_count;
    struct darshan_core_module* mod_array[DARSHAN_MAX_MODS];
    char comp_buf[DARSHAN_CORE_COMP_BUF_SIZE];
    double wtime_offset;
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

void darshan_core_initialize(int argc, char **argv);
void darshan_core_shutdown(void);

uint32_t darshan_hashlittle(const void *key, size_t length, uint32_t initval);
uint64_t darshan_hash(const register unsigned char *k, register uint64_t length, register uint64_t level);

#endif /* __DARSHAN_CORE_H */
