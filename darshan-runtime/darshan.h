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
#include "darshan-common.h"

/* macros for declaring wrapper functions and calling MPI routines
 * consistently regardless of whether static or dynamic linking is used
 */
#ifdef DARSHAN_PRELOAD

#include <dlfcn.h>
#include <stdlib.h>

#define DARSHAN_FORWARD_DECL(__func,__ret,__args) \
  __ret (*__real_ ## __func)__args = NULL

#define DARSHAN_DECL(__func) __func

#define DARSHAN_MPI_CALL(__func) __real_ ## __func

#define MAP_OR_FAIL(__func) \
    if (!(__real_ ## __func)) \
    { \
        __real_ ## __func = dlsym(RTLD_NEXT, #__func); \
        if(!(__real_ ## __func)) { \
           fprintf(stderr, "Darshan failed to map symbol: %s\n", #__func); \
           exit(1); \
       } \
    }

#else

#define DARSHAN_FORWARD_DECL(__name,__ret,__args) \
  extern __ret __real_ ## __name __args;

#define DARSHAN_DECL(__name) __wrap_ ## __name

#define DARSHAN_MPI_CALL(__func) __func

#define MAP_OR_FAIL(__func)

#endif

/* module developers provide the following functions to darshan-core */
struct darshan_module_funcs
{
    /* perform any necessary pre-shutdown steps */
    void (*begin_shutdown)(void);
    /* retrieve module data to write to log file */
    void (*get_output_data)(
        void** buf, /* output parameter to save module buffer address */
        int* size /* output parameter to save module buffer size */
    );
    /* shutdown module data structures */
    void (*shutdown)(void);
    /* (OPTIONAL) perform any necessary steps prior to performing a reduction
     * of shared Darshan I/O records. To bypass shared file reduction mechanism,
     * set this pointer to NULL.
     */
    void (*setup_reduction)(
        darshan_record_id *shared_recs, /* input list of shared records */
        int *shared_rec_count, /* in/out shared record count */
        void **send_buf, /* send buffer for shared file reduction */
        void **recv_buf, /* recv buffer for shared file reduction (root only) */
        int *rec_size /* size of records being stored for this module */
    );
    /* (OPTIONAL) perform the actual shared file reduction operation. This 
     * operation follows the prototype of MPI_Op_create, which allows the
     * specification of user-defined combination functions which may be used
     * directly by MPI. To bypass shared file reduction mechanism, set this
     * pointer to NULL. 
     */
    void (*record_reduction_op)(
        void* infile_v,
        void* inoutfile_v,
        int *len,
        MPI_Datatype *datatype
    );
};

/* paths that darshan will not trace */
extern char* darshan_path_exclusions[]; /* defined in lib/darshan-core.c */

/*****************************************************
* darshan-core functions exported to darshan modules *
*****************************************************/

/* darshan_core_register_module()
 *
 * Register module identifier 'mod_id' with the darshan-core runtime
 * environment, allowing the module to store I/O characterization data.
 * 'funcs' is a pointer to a structure containing each of the function
 * pointers required by darshan-core to shut down the module. The function
 * returns the following integers passed in as pointers: 'my_rank' is the
 * MPI rank of the calling process, 'mod_mem_limit' is the maximum amount
 * of memory the module may use, and 'sys_mem_alignment' is the configured
 * memory alignment value Darshan was configured with.
 */
void darshan_core_register_module(
    darshan_module_id mod_id,
    struct darshan_module_funcs *funcs,
    int *my_rank,
    int *mod_mem_limit,
    int *sys_mem_alignment);

/* darshan_core_unregister_module()
 * 
 * Unregisters module identifier 'mod_id' with the darshan-core runtime,
 * removing the given module from the resulting I/O characterization log.
 */
void darshan_core_unregister_module(
    darshan_module_id mod_id);

/* darshan_core_register_record()
 *
 * Register the Darshan record given by 'name' with the darshan-core
 * runtime, allowing it to be properly tracked and (potentially)
 * correlated with records from other modules. 'len' is the size of
 * the name pointer (string length for string names), 'printable_flag'
 * indicates whether the name is a string, and 'mod_id' is the identifier
 * of the calling module. 'rec_id' is an output pointer storing the
 * correspoing Darshan record identifier and 'file_alignment' is an
 * output pointer storing the file system alignment value for the given
 * record.
 */
void darshan_core_register_record(
    void *name,
    int len,
    int printable_flag,
    darshan_module_id mod_id,
    darshan_record_id *rec_id,
    int *file_alignment);

/* darshan_core_unregister_record()
 *
 * Unregister record identifier 'rec_id' in the darshan-core runtime.
 * This unregister is only in the context of module identifier 'mod_id',
 * meaning that if the file record has other module's associated with
 * it, then the record won't be completely removed.
 */
void darshan_core_unregister_record(
    darshan_record_id rec_id,
    darshan_module_id mod_id);

/* darshan_core_wtime()
 *
 * Returns the elapsed time relative to (roughly) the start of
 * the application.
 */
double darshan_core_wtime(void);

#endif /* __DARSHAN_H */
