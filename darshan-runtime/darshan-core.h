/*
 *  (C) 2014 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_CORE_H
#define __DARSHAN_CORE_H

#include <unistd.h>
#include <sys/types.h>
#include <stdint.h>

#define DARSHAN_MPI_CALL(func) func

typedef uint64_t darshan_file_id;

struct darshan_module_funcs
{
    void (*prepare_for_shutdown)(void);
    void (*get_output_data)(void **, int);
};

struct darshan_module
{
    char *name;
    struct darshan_module_funcs mod_funcs;
};

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

#endif /* __DARSHAN_CORE_H */
