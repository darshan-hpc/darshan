/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
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

/* macros for declaring wrapper functions and calling MPI routines
 * consistently regardless of whether static or dynamic linking is used
 */
#ifdef DARSHAN_PRELOAD
#define __USE_GNU
#include <dlfcn.h>
#include <stdlib.h>

#define DARSHAN_FORWARD_DECL(name,ret,args) \
  ret (*__real_ ## name)args = NULL;

#define DARSHAN_DECL(__name) __name

#define DARSHAN_MPI_CALL(func) __real_ ## func

#define MAP_OR_FAIL(func) \
    if (!(__real_ ## func)) \
    { \
        __real_ ## func = dlsym(RTLD_NEXT, #func); \
        if(!(__real_ ## func)) { \
           fprintf(stderr, "Darshan failed to map symbol: %s\n", #func); \
           exit(1); \
       } \
    }

#else

#define DARSHAN_FORWARD_DECL(name,ret,args) \
  extern ret __real_ ## name args;

#define DARSHAN_DECL(__name) __wrap_ ## __name

#define DARSHAN_MPI_CALL(func) func

#define MAP_OR_FAIL(func)

#endif

/* macros for manipulating module's counter variables */
/* NOTE: */

/* module developers provide the following functions to darshan-core */
struct darshan_module_funcs
{
    /* disable futher instrumentation within a module */
    void (*disable_instrumentation)(void);
    /* perform any necessary steps prior to reducing */
    void (*prepare_for_reduction)(
        darshan_record_id *shared_recs, /* input list of shared records */
        int *shared_rec_count, /* in/out shared record count */
        void **send_buf, /* send buffer for shared file reduction */
        void **recv_buf, /* recv buffer for shared file reduction (root only) */
        int *rec_size /* size of records being stored for this module */
    );
    /* reduce records which are shared globally across this module */
    void (*record_reduction_op)(
        void* infile_v,
        void* inoutfile_v,
        int *len,
        MPI_Datatype *datatype
    );
    /* retrieve module data to write to log file */
    void (*get_output_data)(
        void** buf, /* output parameter to save module buffer address */
        int* size /* output parameter to save module buffer size */
    );
    /* shutdown module data structures */
    void (*shutdown)(void);
};

/* paths that darshan will not trace */
extern char* darshan_path_exclusions[]; /* defined in lib/darshan-core.c */

/*****************************************************
* darshan-core functions exported to darshan modules *
*****************************************************/

void darshan_core_register_module(
    darshan_module_id mod_id,
    struct darshan_module_funcs *funcs,
    int *runtime_mem_limit);

void darshan_core_unregister_module(
    darshan_module_id mod_id);

void darshan_core_register_record(
    void *name,
    int len,
    int printable_flag,
    darshan_module_id mod_id,
    darshan_record_id *rec_id);

void darshan_core_unregister_record(
    darshan_record_id rec_id,
    darshan_module_id mod_id);

double darshan_core_wtime(void);

/***********************************************
* darshan-common functions for darshan modules *
***********************************************/

char* darshan_clean_file_path(const char* path);

#endif /* __DARSHAN_H */
