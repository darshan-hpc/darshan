/*
 *  (C) 2014 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_CORE_H
#define __DARSHAN_CORE_H

#include <unistd.h>
#include <sys/types.h>
#include <stdint.h>

#include "darshan.h"

/* TODO: enforce this when handing out ids */
#define DARSHAN_CORE_MAX_RECORDS 1024

struct darshan_core_module
{
    darshan_module_id id;
    char name[DARSHAN_MOD_NAME_LEN+1];
    struct darshan_module_funcs mod_funcs;
};

/* in memory structure to keep up with job level data */
/* TODO: trailing data ? */
struct darshan_core_runtime
{
    struct darshan_job log_job;
    char exe[CP_EXE_LEN+1];
    double wtime_offset;
    struct darshan_core_record_ref *rec_hash;
    struct darshan_core_module* mod_array[DARSHAN_MAX_MODS];
};

struct darshan_core_record_ref
{
    char* name;
    darshan_record_id id;
    UT_hash_handle hlink;
};

#endif /* __DARSHAN_CORE_H */
