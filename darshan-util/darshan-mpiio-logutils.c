/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#define _GNU_SOURCE
#include "darshan-util-config.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include "darshan-mpiio-logutils.h"

/* counter name strings for the MPI-IO module */
#define X(a) #a,
char *mpiio_counter_names[] = {
    MPIIO_COUNTERS
};

char *mpiio_f_counter_names[] = {
    MPIIO_F_COUNTERS
};
#undef X

static int darshan_log_get_mpiio_file(darshan_fd fd, void** mpiio_buf_p,
    int* bytes_left, void** file_rec, darshan_record_id* rec_id);
static void darshan_log_print_mpiio_file(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type);

struct darshan_mod_logutil_funcs mpiio_logutils =
{
    .log_get_record = &darshan_log_get_mpiio_file,
    .log_print_record = &darshan_log_print_mpiio_file,
};

static int darshan_log_get_mpiio_file(darshan_fd fd, void** mpiio_buf_p,
    int* bytes_left, void** file_rec, darshan_record_id* rec_id)
{
    int i;
    struct darshan_mpiio_file *file = (struct darshan_mpiio_file *)
        (*mpiio_buf_p);

    if(*bytes_left < sizeof(struct darshan_mpiio_file))
        return(-1);

    if(fd->swap_flag)
    {
        /* swap bytes if necessary */
        DARSHAN_BSWAP64(&file->f_id);
        DARSHAN_BSWAP64(&file->rank);
        for(i=0; i<MPIIO_NUM_INDICES; i++)
            DARSHAN_BSWAP64(&file->counters[i]);
        for(i=0; i<MPIIO_F_NUM_INDICES; i++)
            DARSHAN_BSWAP64(&file->fcounters[i]);
    }

    /* update/set output variables */
    *file_rec = (void *)file;
    *rec_id = file->f_id;
    *mpiio_buf_p = (file + 1); /* increment input buf by size of file record */
    *bytes_left -= sizeof(struct darshan_mpiio_file);

    return(0);
}

static void darshan_log_print_mpiio_file(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type)
{
    int i;
    struct darshan_mpiio_file *mpiio_file_rec =
        (struct darshan_mpiio_file *)file_rec;

    for(i=0; i<MPIIO_NUM_INDICES; i++)
    {
        DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_MPIIO_MOD],
            mpiio_file_rec->rank, mpiio_file_rec->f_id, mpiio_counter_names[i],
            mpiio_file_rec->counters[i], file_name, mnt_pt, fs_type);
    }

    for(i=0; i<MPIIO_F_NUM_INDICES; i++)
    {
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_MPIIO_MOD],
            mpiio_file_rec->rank, mpiio_file_rec->f_id, mpiio_f_counter_names[i],
            mpiio_file_rec->fcounters[i], file_name, mnt_pt, fs_type);
    }

    return;
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
