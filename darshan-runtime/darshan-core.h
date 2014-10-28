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

/* calculation of compression buffer size (defaults to 50% of the maximum
 * memory that Darshan is allowed to consume on a process) 
 */
//#define CP_COMP_BUF_SIZE ((CP_MAX_FILES * sizeof(struct darshan_file))/2)
#define CP_COMP_BUF_SIZE 0

/* max length of module name string (not counting \0) */
#define DARSHAN_MOD_NAME_LEN 31

/* flags to indicate properties of file records */
#define CP_FLAG_CONDENSED 1<<0
#define CP_FLAG_NOTIMING 1<<1

struct darshan_core_module
{
    char name[DARSHAN_MOD_NAME_LEN+1];
    struct darshan_module_funcs mod_funcs;
    struct darshan_core_module *next_mod;
};

/* in memory structure to keep up with job level data */
struct darshan_core_job_runtime
{
    struct darshan_job log_job;
    char exe[CP_EXE_LEN+1];
    struct darshan_core_module *mod_list_head;
    char comp_buf[CP_COMP_BUF_SIZE];
    int flags;
    int file_count;
    double wtime_offset;
    char* trailing_data;
};

#endif /* __DARSHAN_CORE_H */
