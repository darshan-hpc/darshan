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

static int darshan_log_get_mpiio_file(darshan_fd fd, void* mpiio_buf);
static int darshan_log_put_mpiio_file(darshan_fd fd, void* mpiio_buf);
static void darshan_log_print_mpiio_file(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type);
static void darshan_log_agg_mpiio_files(void *rec, void *agg_rec, int init_flag);

struct darshan_mod_logutil_funcs mpiio_logutils =
{
    .log_get_record = &darshan_log_get_mpiio_file,
    .log_put_record = &darshan_log_put_mpiio_file,
    .log_print_record = &darshan_log_print_mpiio_file,
    .log_agg_records = &darshan_log_agg_mpiio_files
};

static int darshan_log_get_mpiio_file(darshan_fd fd, void* mpiio_buf)
{
    struct darshan_mpiio_file *file;
    int i;
    int ret;

    ret = darshan_log_getmod(fd, DARSHAN_MPIIO_MOD, mpiio_buf,
        sizeof(struct darshan_mpiio_file));
    if(ret < 0)
        return(-1);
    else if(ret < sizeof(struct darshan_mpiio_file))
        return(0);
    else
    {
        file = (struct darshan_mpiio_file *)mpiio_buf;
        if(fd->swap_flag)
        {
            /* swap bytes if necessary */
            DARSHAN_BSWAP64(&(file->base_rec.id));
            DARSHAN_BSWAP64(&(file->base_rec.rank));
            for(i=0; i<MPIIO_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->counters[i]);
            for(i=0; i<MPIIO_F_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->fcounters[i]);
        }

        return(1);
    }
}

static int darshan_log_put_mpiio_file(darshan_fd fd, void* mpiio_buf)
{
    struct darshan_mpiio_file *file = (struct darshan_mpiio_file *)mpiio_buf;
    int ret;

    ret = darshan_log_putmod(fd, DARSHAN_MPIIO_MOD, file,
        sizeof(struct darshan_mpiio_file));
    if(ret < 0)
        return(-1);

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
            mpiio_file_rec->base_rec.rank, mpiio_file_rec->base_rec.id,
            mpiio_counter_names[i], mpiio_file_rec->counters[i],
            file_name, mnt_pt, fs_type);
    }

    for(i=0; i<MPIIO_F_NUM_INDICES; i++)
    {
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_MPIIO_MOD],
            mpiio_file_rec->base_rec.rank, mpiio_file_rec->base_rec.id,
            mpiio_f_counter_names[i], mpiio_file_rec->fcounters[i],
            file_name, mnt_pt, fs_type);
    }

    return;
}

static void darshan_log_agg_mpiio_files(void *rec, void *agg_rec, int init_flag)
{

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
