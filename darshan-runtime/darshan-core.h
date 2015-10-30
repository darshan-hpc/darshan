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

/* Environment variable to override memory per module */
#define DARSHAN_MOD_MEM_OVERRIDE "DARSHAN_MODMEM"

/* Maximum amount of memory per instrumentation module in MiB */
#ifdef __DARSHAN_MOD_MEM_MAX
#define DARSHAN_MOD_MEM_MAX (__DARSHAN_MOD_MEM_MAX * 1024 * 1024)
#else
#define DARSHAN_MOD_MEM_MAX (2 * 1024 * 1024) /* 2 MiB default */
#endif

/* Default runtime compression buffer size */
#define DARSHAN_COMP_BUF_SIZE DARSHAN_MOD_MEM_MAX

/* in memory structure to keep up with job level data */
struct darshan_core_runtime
{
    struct darshan_header log_header;
    struct darshan_job log_job;
    char exe[DARSHAN_EXE_LEN+1];
    struct darshan_core_record_ref *rec_hash;
    int rec_count;
    struct darshan_core_module* mod_array[DARSHAN_MAX_MODS];
    char comp_buf[DARSHAN_COMP_BUF_SIZE];
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

void darshan_core_initialize(int argc, char **argv);
void darshan_core_shutdown(void);

uint32_t darshan_hashlittle(const void *key, size_t length, uint32_t initval);
uint64_t darshan_hash(const register unsigned char *k, register uint64_t length, register uint64_t level);

#endif /* __DARSHAN_CORE_H */
