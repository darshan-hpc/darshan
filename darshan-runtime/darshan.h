/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_H
#define __DARSHAN_H

#include <unistd.h>
#include <sys/types.h>
#include <stdint.h>
#include <mpi.h>

#include "darshan-log-format.h"

/* Environment variable to override CP_JOBID */
#define CP_JOBID_OVERRIDE "DARSHAN_JOBID"

/* Environment variable to override __CP_LOG_PATH */
#define CP_LOG_PATH_OVERRIDE "DARSHAN_LOGPATH"

/* Environment variable to override __CP_LOG_PATH */
#define CP_LOG_HINTS_OVERRIDE "DARSHAN_LOGHINTS"

/* Environment variable to override __CP_MEM_ALIGNMENT */
#define CP_MEM_ALIGNMENT_OVERRIDE "DARSHAN_MEMALIGN"

/* TODO these go where ? */
#define DARSHAN_MPI_CALL(func) func

typedef uint64_t darshan_file_id;

struct darshan_module_funcs
{
    void (*prepare_for_shutdown)(void);
    void (*get_output_data)(void **, int);
};

/*********************************************
* darshan-core functions for darshan modules *
*********************************************/

void darshan_core_register_module(
    char *name,
    struct darshan_module_funcs *funcs,
    int *runtime_mem_limit);

void darshan_core_lookup_id(
    void *name,
    int len,
    int printable_flag,
    darshan_file_id *id);

double darshan_core_wtime(void);

/***********************************************
* darshan-common functions for darshan modules *
***********************************************/

char* darshan_clean_file_path(const char* path);

#endif /* __DARSHAN_H */
