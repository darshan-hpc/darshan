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

#include "darshan-bgq-logutils.h"

/* counter name strings for the POSIX module */
#define X(a, b) #a,
char *bgq_counter_names[] = {
    BGQ_COUNTERS
};

char *bgq_f_counter_names[] = {
    BGQ_F_COUNTERS
};
#undef X

static int darshan_log_get_bgq_file(void** psx_buf_p, int* bytes_left,
    void** file_rec, darshan_record_id* rec_id, int byte_swap_flag);
static void darshan_log_print_bgq_file(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type);

struct darshan_mod_logutil_funcs bgq_logutils =
{
    .log_get_record = &darshan_log_get_bgq_file,
    .log_print_record = &darshan_log_print_bgq_file,
};

static int darshan_log_get_bgq_file(void** psx_buf_p, int* bytes_left,
    void** file_rec, darshan_record_id* rec_id, int byte_swap_flag)
{
    int i;
    struct darshan_bgq_record *file = (struct darshan_bgq_record *)
        (*psx_buf_p);

    if(*bytes_left < sizeof(struct darshan_bgq_record))
        return(-1);

    if(byte_swap_flag)
    {
        /* swap bytes if necessary */
        DARSHAN_BSWAP64(&file->f_id);
        DARSHAN_BSWAP64(&file->rank);
        for(i=0; i<POSIX_NUM_INDICES; i++)
            DARSHAN_BSWAP64(&file->counters[i]);
        for(i=0; i<POSIX_F_NUM_INDICES; i++)
            DARSHAN_BSWAP64(&file->fcounters[i]);
    }

    /* update/set output variables */
    *file_rec = (void *)file;
    *rec_id = file->f_id;
    *psx_buf_p = (file + 1); /* increment input buf by size of file record */
    *bytes_left -= sizeof(struct darshan_bgq_record);

    return(0);
}

static void darshan_log_print_bgq_file(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type)
{
    int i;
    struct darshan_bgq_record *bgq_file_rec =
        (struct darshan_bgq_record *)file_rec;

    for(i=0; i<POSIX_NUM_INDICES; i++)
    {
        DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
            bgq_file_rec->rank, bgq_file_rec->f_id, bgq_counter_names[i],
            bgq_file_rec->counters[i], file_name, mnt_pt, fs_type);
    }

    for(i=0; i<POSIX_F_NUM_INDICES; i++)
    {
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
            bgq_file_rec->rank, bgq_file_rec->f_id, bgq_f_counter_names[i],
            bgq_file_rec->fcounters[i], file_name, mnt_pt, fs_type);
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
