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

/* default number of records to attempt to store for each module */
#define DARSHAN_DEF_MOD_REC_COUNT 1024

/* module developers must define a 'darshan_module_shutdown' function
 * for allowing darshan-core to call into a module and retrieve final
 * output data to be saved in the log.
 *
 * NOTE: module developers can use this function to run collective
 * MPI operations at shutdown time. Typically this functionality
 * has been used to reduce records shared globablly (given in the
 * 'shared_recs' array) into a single data record.
 */
typedef void (*darshan_module_shutdown)(
    MPI_Comm mod_comm,  /* MPI communicator to run collectives with */
    darshan_record_id *shared_recs, /* list of shared data record ids */
    int shared_rec_count, /* count of shared data records */
    void **mod_buf, /* output parameter to save module buffer address */
    int *mod_buf_sz /* output parameter to save module buffer size */
);

/*****************************************************
* darshan-core functions exported to darshan modules *
*****************************************************/

/* darshan_core_register_module()
 *
 * Register module identifier 'mod_id' with the darshan-core runtime
 * environment, allowing the module to store I/O characterization data.
 * 'funcs' is a pointer to a structure containing each of the function
 * pointers required by darshan-core to shut down the module.
 * 'inout_mod_buf_size' is an input/output argument, with it being
 * set to the requested amount of module memory on input, and set to
 * the amount allocated by darshan-core on output. If given, 'rank' is
 * a pointer to an integer which will contain the calling process's
 * MPI rank on return. If given, 'sys_mem_alignment' is a pointer to
 * an integer which will contain the memory alignment value Darshan
 * was configured with on return.
 */
void darshan_core_register_module(
    darshan_module_id mod_id,
    darshan_module_shutdown mod_shutdown_func,
    int *inout_mod_buf_size,
    int *rank,
    int *sys_mem_alignment);

/* darshan_core_unregister_module()
 * 
 * Unregisters module identifier 'mod_id' with the darshan-core runtime,
 * removing the given module from the resulting I/O characterization log.
 */
void darshan_core_unregister_module(
    darshan_module_id mod_id);

/* darshan_core_gen_record_id()
 *
 * Returns the Darshan record ID correpsonding to input string 'name'.
 */
darshan_record_id darshan_core_gen_record_id(
    const char *name);

/* darshan_core_register_record()
 *
 * Register a record with the darshan-core runtime, allowing it to be
 * properly tracked and (potentially) correlated with records from other
 * modules. 'rec_id' is the Darshan record id as given by the
 * `darshan_core_gen_record_id` function. 'name' is the the name of the
 * Darshan record (e.g., the full file path), which for now is just a
 * string. 'mod_id' is the identifier of the calling module. 'rec_len'
 * is the size of the record being registered with Darshan. If given,
 * 'file_alignment' is a pointer to an integer which on return will
 * contain the corresponding file system alignment of the file system
 * path 'name' resides on. Returns a pointer to the address the record
 * should be written to on success, NULL on failure.
 */
void *darshan_core_register_record(
    darshan_record_id rec_id,
    const char *name,
    darshan_module_id mod_id,
    int rec_len,
    int *file_alignment);

/* darshan_core_wtime()
 *
 * Returns the elapsed time relative to (roughly) the start of
 * the application.
 */
double darshan_core_wtime(void);

/* darshan_core_excluded_path()
 *
 * Returns true (1) if the given file path 'path' is in Darshan's
 * list of excluded file paths, false (0) otherwise.
 */
int darshan_core_excluded_path(
    const char * path);

#endif /* __DARSHAN_H */
