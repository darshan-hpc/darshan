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

static int darshan_log_get_hdf5_file(darshan_fd fd, void** hdf5_buf_p);
static int darshan_log_put_hdf5_file(darshan_fd fd, void* hdf5_buf, int ver);
static void darshan_log_print_hdf5_file(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type, int ver);
static void darshan_log_print_hdf5_description(void);
static void darshan_log_print_hdf5_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2);
static void darshan_log_agg_hdf5_files(void *rec, void *agg_rec, int init_flag);

struct darshan_mod_logutil_funcs hdf5_logutils =
{
    .log_get_record = &darshan_log_get_hdf5_file,
    .log_put_record = &darshan_log_put_hdf5_file,
    .log_print_record = &darshan_log_print_hdf5_file,
    .log_print_description = &darshan_log_print_hdf5_description,
    .log_print_diff = &darshan_log_print_hdf5_file_diff,
    .log_agg_records = &darshan_log_agg_hdf5_files
};

static int darshan_log_get_hdf5_file(darshan_fd fd, void** hdf5_buf_p)
{
    struct darshan_hdf5_file *file = *((struct darshan_hdf5_file **)hdf5_buf_p);
    int i;
    int ret;

    if(fd->mod_map[DARSHAN_HDF5_MOD].len == 0)
        return(0);

    if(*hdf5_buf_p == NULL)
    {
        file = malloc(sizeof(*file));
        if(!file)
            return(-1);
    }

    ret = darshan_log_get_mod(fd, DARSHAN_HDF5_MOD, file,
        sizeof(struct darshan_hdf5_file));

    if(*hdf5_buf_p == NULL)
    {
        if(ret == sizeof(struct darshan_hdf5_file))
            *hdf5_buf_p = file;
        else
            free(file);
    }

    if(ret < 0)
        return(-1);
    else if(ret < sizeof(struct darshan_hdf5_file))
        return(0);
    else
    {
        /* if the read was successful, do any necessary byte-swapping */
        if(fd->swap_flag)
        {
            DARSHAN_BSWAP64(&(file->base_rec.id));
            DARSHAN_BSWAP64(&(file->base_rec.rank));
            for(i=0; i<HDF5_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->counters[i]);
            for(i=0; i<HDF5_F_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->fcounters[i]);
        }

        return(1);
    }
}

static int darshan_log_put_hdf5_file(darshan_fd fd, void* hdf5_buf, int ver)
{
    struct darshan_hdf5_file *file = (struct darshan_hdf5_file *)hdf5_buf;
    int ret;

    ret = darshan_log_put_mod(fd, DARSHAN_HDF5_MOD, file,
        sizeof(struct darshan_hdf5_file), DARSHAN_HDF5_VER);
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
            hdf5_file_rec->base_rec.rank, hdf5_file_rec->base_rec.id,
            hdf5_counter_names[i], hdf5_file_rec->counters[i],
            file_name, mnt_pt, fs_type);
    }

    for(i=0; i<HDF5_F_NUM_INDICES; i++)
    {
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_HDF5_MOD],
            hdf5_file_rec->base_rec.rank, hdf5_file_rec->base_rec.id,
            hdf5_f_counter_names[i], hdf5_file_rec->fcounters[i],
            file_name, mnt_pt, fs_type);
    }

    return;
}

static void darshan_log_print_hdf5_description()
{
    printf("\n# description of HDF5 counters:\n");
    printf("#   HDF5_OPENS: HDF5 file open operation counts.\n");
    printf("#   HDF5_F_OPEN_TIMESTAMP: timestamp of first HDF5 file open.\n");
    printf("#   HDF5_F_CLOSE_TIMESTAMP: timestamp of last HDF5 file close.\n");

    return;
}

static void darshan_log_print_hdf5_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2)
{
    struct darshan_hdf5_file *file1 = (struct darshan_hdf5_file *)file_rec1;
    struct darshan_hdf5_file *file2 = (struct darshan_hdf5_file *)file_rec2;
    int i;

    /* NOTE: we assume that both input records are the same module format version */

    for(i=0; i<HDF5_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_HDF5_MOD],
                file1->base_rec.rank, file1->base_rec.id, hdf5_counter_names[i],
                file1->counters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_HDF5_MOD],
                file2->base_rec.rank, file2->base_rec.id, hdf5_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
        else if(file1->counters[i] != file2->counters[i])
        {
            printf("- ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_HDF5_MOD],
                file1->base_rec.rank, file1->base_rec.id, hdf5_counter_names[i],
                file1->counters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_HDF5_MOD],
                file2->base_rec.rank, file2->base_rec.id, hdf5_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
    }

    for(i=0; i<HDF5_F_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_HDF5_MOD],
                file1->base_rec.rank, file1->base_rec.id, hdf5_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_HDF5_MOD],
                file2->base_rec.rank, file2->base_rec.id, hdf5_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
        else if(file1->fcounters[i] != file2->fcounters[i])
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_HDF5_MOD],
                file1->base_rec.rank, file1->base_rec.id, hdf5_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_HDF5_MOD],
                file2->base_rec.rank, file2->base_rec.id, hdf5_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
    }

    return;
}

static void darshan_log_agg_hdf5_files(void *rec, void *agg_rec, int init_flag)
{
    struct darshan_hdf5_file *hdf5_rec = (struct darshan_hdf5_file *)rec;
    struct darshan_hdf5_file *agg_hdf5_rec = (struct darshan_hdf5_file *)agg_rec;
    int i;

    for(i = 0; i < HDF5_NUM_INDICES; i++)
    {
        switch(i)
        {
            case HDF5_OPENS:
                /* sum */
                agg_hdf5_rec->counters[i] += hdf5_rec->counters[i];
                break;
            default:
                agg_hdf5_rec->counters[i] = -1;
                break;
        }
    }

    for(i = 0; i < HDF5_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case HDF5_F_OPEN_TIMESTAMP:
                /* minimum non-zero */
                if((hdf5_rec->fcounters[i] > 0)  &&
                    ((agg_hdf5_rec->fcounters[i] == 0) ||
                    (hdf5_rec->fcounters[i] < agg_hdf5_rec->fcounters[i])))
                {
                    agg_hdf5_rec->fcounters[i] = hdf5_rec->fcounters[i];
                }
                break;
            case HDF5_F_CLOSE_TIMESTAMP:
                /* maximum */
                if(hdf5_rec->fcounters[i] > agg_hdf5_rec->fcounters[i])
                {
                    agg_hdf5_rec->fcounters[i] = hdf5_rec->fcounters[i];
                }
                break;
            default:
                agg_hdf5_rec->fcounters[i] = -1;
                break;
        }
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
