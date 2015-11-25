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

#include "darshan-logutils.h"

/* integer counter name strings for the NULL module */
#define X(a) #a,
char *null_counter_names[] = {
    NULL_COUNTERS
};

/* floating point counter name strings for the NULL module */
char *null_f_counter_names[] = {
    NULL_F_COUNTERS
};
#undef X

/* prototypes for each of the NULL module's logutil functions */
static int darshan_log_get_null_record(darshan_fd fd, void* null_buf,
    darshan_record_id* rec_id);
static int darshan_log_put_null_record(darshan_fd fd, void* null_buf, int ver);
static void darshan_log_print_null_record(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type, int ver);

/* structure storing each function needed for implementing the darshan
 * logutil interface. these functions are used for reading, writing, and
 * printing module data in a consistent manner.
 */
struct darshan_mod_logutil_funcs null_logutils =
{
    .log_get_record = &darshan_log_get_null_record,
    .log_put_record = &darshan_log_put_null_record,
    .log_print_record = &darshan_log_print_null_record,
};

/* retrieve a NULL record from log file descriptor 'fd', storing the
 * buffer in 'null_buf' and the corresponding Darshan record id in
 * 'rec_id'. Return 1 on successful record read, .
 */
static int darshan_log_get_null_record(darshan_fd fd, void* null_buf, 
    darshan_record_id* rec_id)
{
    struct darshan_null_record *rec;
    int i;
    int ret;

    /* read a NULL module record from the darshan log file */
    ret = darshan_log_getmod(fd, DARSHAN_NULL_MOD, null_buf,
        sizeof(struct darshan_null_record));
    if(ret < 0)
        return(-1);
    else if(ret < sizeof(struct darshan_null_record))
        return(0);
    else
    {
        /* if the read was successful, do any necessary byte-swapping */
        rec = (struct darshan_null_record *)null_buf;
        if(fd->swap_flag)
        {
            /* swap bytes if necessary */
            DARSHAN_BSWAP64(&rec->f_id);
            DARSHAN_BSWAP64(&rec->rank);
            for(i=0; i<NULL_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&rec->counters[i]);
            for(i=0; i<NULL_F_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&rec->fcounters[i]);
        }

        /* set the output record id */
        *rec_id = rec->f_id;
        return(1);
    }
}

/* write the NULL record stored in 'null_buf' to log file descriptor 'fd'.
 * Return 0 on success, -1 on failure
 */
static int darshan_log_put_null_record(darshan_fd fd, void* null_buf, int ver)
{
    struct darshan_null_record *rec = (struct darshan_null_record *)null_buf;
    int ret;

    /* append NULL record to darshan log file */
    ret = darshan_log_putmod(fd, DARSHAN_NULL_MOD, rec,
        sizeof(struct darshan_null_record), ver);
    if(ret < 0)
        return(-1);

    return(0);
}

/* print all I/O data record statistics for the given NULL record */
static void darshan_log_print_null_record(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type, int ver)
{
    int i;
    struct darshan_null_record *null_rec =
        (struct darshan_null_record *)file_rec;

    /* print each of the integer and floating point counters for the NULL module */
    for(i=0; i<NULL_NUM_INDICES; i++)
    {
        /* macro defined in darshan-logutils.h */
        DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_NULL_MOD],
            null_rec->rank, null_rec->f_id, null_counter_names[i],
            null_rec->counters[i], file_name, mnt_pt, fs_type);
    }

    for(i=0; i<NULL_F_NUM_INDICES; i++)
    {
        /* macro defined in darshan-logutils.h */
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_NULL_MOD],
            null_rec->rank, null_rec->f_id, null_f_counter_names[i],
            null_rec->fcounters[i], file_name, mnt_pt, fs_type);
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
