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

struct darshan_core_module
{
    darshan_module_id id;
    char name[DARSHAN_MOD_NAME_LEN+1];
    struct darshan_module_funcs mod_funcs;
};

/* in memory structure to keep up with job level data */
/* TODO: trailing data ? */
struct darshan_core_job_runtime
{
    struct darshan_job log_job;
    struct darshan_core_module* mod_array[DARSHAN_MAX_MODS];
    char exe[CP_EXE_LEN+1];
    double wtime_offset;
};

#endif /* __DARSHAN_CORE_H */
