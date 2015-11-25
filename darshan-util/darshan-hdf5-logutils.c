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

/* counter name strings for the HDF5 module */
#define X(a) #a,
char *hdf5_counter_names[] = {
    HDF5_COUNTERS
};

char *hdf5_f_counter_names[] = {
    HDF5_F_COUNTERS
};
#undef X

static int darshan_log_get_hdf5_file(darshan_fd fd, void* hdf5_buf,
    darshan_record_id* rec_id);
static int darshan_log_put_hdf5_file(darshan_fd fd, void* hdf5_buf, int ver);
static void darshan_log_print_hdf5_file(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type, int ver);

struct darshan_mod_logutil_funcs hdf5_logutils =
{
    .log_get_record = &darshan_log_get_hdf5_file,
    .log_put_record = &darshan_log_put_hdf5_file,
    .log_print_record = &darshan_log_print_hdf5_file,
};

static int darshan_log_get_hdf5_file(darshan_fd fd, void* hdf5_buf,
    darshan_record_id* rec_id)
{
    struct darshan_hdf5_file *file;
    int i;
    int ret;

    ret = darshan_log_getmod(fd, DARSHAN_HDF5_MOD, hdf5_buf,
        sizeof(struct darshan_hdf5_file));
    if(ret < 0)
        return(-1);
    else if(ret < sizeof(struct darshan_hdf5_file))
        return(0);
    else
    {
        file = (struct darshan_hdf5_file *)hdf5_buf;
        if(fd->swap_flag)
        {
            /* swap bytes if necessary */
            DARSHAN_BSWAP64(&file->f_id);
            DARSHAN_BSWAP64(&file->rank);
            for(i=0; i<HDF5_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->counters[i]);
            for(i=0; i<HDF5_F_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->fcounters[i]);
        }

        *rec_id = file->f_id;
        return(1);
    }
}

static int darshan_log_put_hdf5_file(darshan_fd fd, void* hdf5_buf, int ver)
{
    struct darshan_hdf5_file *file = (struct darshan_hdf5_file *)hdf5_buf;
    int ret;

    ret = darshan_log_putmod(fd, DARSHAN_HDF5_MOD, file,
        sizeof(struct darshan_hdf5_file), ver);
    if(ret < 0)
        return(-1);

    return(0);
}

static void darshan_log_print_hdf5_file(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type, int ver)
{
    int i;
    struct darshan_hdf5_file *hdf5_file_rec =
        (struct darshan_hdf5_file *)file_rec;

    for(i=0; i<HDF5_NUM_INDICES; i++)
    {
        DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_HDF5_MOD],
            hdf5_file_rec->rank, hdf5_file_rec->f_id, hdf5_counter_names[i],
            hdf5_file_rec->counters[i], file_name, mnt_pt, fs_type);
    }

    for(i=0; i<HDF5_F_NUM_INDICES; i++)
    {
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_HDF5_MOD],
            hdf5_file_rec->rank, hdf5_file_rec->f_id, hdf5_f_counter_names[i],
            hdf5_file_rec->fcounters[i], file_name, mnt_pt, fs_type);
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
