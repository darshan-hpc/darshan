/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifdef HAVE_CONFIG_H
# include "darshan-util-config.h"
#endif

#define _GNU_SOURCE
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
char *h5f_counter_names[] = {
    H5F_COUNTERS
};
char *h5f_f_counter_names[] = {
    H5F_F_COUNTERS
};

char *h5d_counter_names[] = {
    H5D_COUNTERS
};
char *h5d_f_counter_names[] = {
    H5D_F_COUNTERS
};
#undef X

#define DARSHAN_H5F_FILE_SIZE_1 40
#define DARSHAN_H5F_FILE_SIZE_2 56

#define DARSHAN_H5D_DATASET_SIZE_1 904

static int darshan_log_get_hdf5_file(darshan_fd fd, void** hdf5_buf_p);
static int darshan_log_put_hdf5_file(darshan_fd fd, void* hdf5_buf);
static void darshan_log_print_hdf5_file(void *ds_rec,
    char *ds_name, char *mnt_pt, char *fs_type);
static void darshan_log_print_hdf5_file_description(int ver);
static void darshan_log_print_hdf5_file_diff(void *ds_rec1, char *ds_name1,
    void *ds_rec2, char *ds_name2);
static void darshan_log_agg_hdf5_files(void *rec, void *agg_rec, int init_flag);

static int darshan_log_get_hdf5_dataset(darshan_fd fd, void** hdf5_buf_p);
static int darshan_log_put_hdf5_dataset(darshan_fd fd, void* hdf5_buf);
static void darshan_log_print_hdf5_dataset(void *ds_rec,
    char *ds_name, char *mnt_pt, char *fs_type);
static void darshan_log_print_hdf5_dataset_description(int ver);
static void darshan_log_print_hdf5_dataset_diff(void *ds_rec1, char *ds_name1,
    void *ds_rec2, char *ds_name2);
static void darshan_log_agg_hdf5_datasets(void *rec, void *agg_rec, int init_flag);

struct darshan_mod_logutil_funcs hdf5_file_logutils =
{
    .log_get_record = &darshan_log_get_hdf5_file,
    .log_put_record = &darshan_log_put_hdf5_file,
    .log_print_record = &darshan_log_print_hdf5_file,
    .log_print_description = &darshan_log_print_hdf5_file_description,
    .log_print_diff = &darshan_log_print_hdf5_file_diff,
    .log_agg_records = &darshan_log_agg_hdf5_files
};

struct darshan_mod_logutil_funcs hdf5_dataset_logutils =
{
    .log_get_record = &darshan_log_get_hdf5_dataset,
    .log_put_record = &darshan_log_put_hdf5_dataset,
    .log_print_record = &darshan_log_print_hdf5_dataset,
    .log_print_description = &darshan_log_print_hdf5_dataset_description,
    .log_print_diff = &darshan_log_print_hdf5_dataset_diff,
    .log_agg_records = &darshan_log_agg_hdf5_datasets
};

static int darshan_log_get_hdf5_file(darshan_fd fd, void** hdf5_buf_p)
{
    struct darshan_hdf5_file *file = *((struct darshan_hdf5_file **)hdf5_buf_p);
    int rec_len;
    int i;
    int ret;

    if(fd->mod_map[DARSHAN_H5F_MOD].len == 0)
        return(0);

    if(fd->mod_ver[DARSHAN_H5F_MOD] == 0 ||
        fd->mod_ver[DARSHAN_H5F_MOD] > DARSHAN_H5F_VER)
    {
        fprintf(stderr, "Error: Invalid H5F module version number (got %d)\n",
            fd->mod_ver[DARSHAN_H5F_MOD]);
        return(-1);
    }

    if(*hdf5_buf_p == NULL)
    {
        file = malloc(sizeof(*file));
        if(!file)
            return(-1);
    }

    if(fd->mod_ver[DARSHAN_H5F_MOD] == DARSHAN_H5F_VER)
    {
        /* log format is in current version, so we don't need to do any
         * translation of counters while reading
         */
        rec_len = sizeof(struct darshan_hdf5_file);
        ret = darshan_log_get_mod(fd, DARSHAN_H5F_MOD, file, rec_len);
    }
    else
    {
        char scratch[1024] = {0};
        char *src_p, *dest_p;
        int len;

        if(fd->mod_ver[DARSHAN_H5F_MOD] == 1)
        {
            rec_len = DARSHAN_H5F_FILE_SIZE_1;
            ret = darshan_log_get_mod(fd, DARSHAN_H5F_MOD, scratch, rec_len);
            if(ret != rec_len)
                goto exit;

            /* upconvert version 1 to version 2 in-place */
            dest_p = scratch + (sizeof(struct darshan_base_record) +
                (1 * sizeof(int64_t)) + (3 * sizeof(double)));
            src_p = dest_p - (2 * sizeof(double));
            len = sizeof(double);
            memmove(dest_p, src_p, len);
            /* set F_CLOSE_START and F_OPEN_END to -1 */
            *((double *)src_p) = -1;
            *((double *)(src_p + sizeof(double))) = -1;
        }
        if(fd->mod_ver[DARSHAN_H5F_MOD] <= 2)
        {
            if(fd->mod_ver[DARSHAN_H5F_MOD] == 2)
            {
                rec_len = DARSHAN_H5F_FILE_SIZE_2;
                ret = darshan_log_get_mod(fd, DARSHAN_H5F_MOD, scratch, rec_len);
                if(ret != rec_len)
                    goto exit;
            }

            /* upconvert version 2 to version 3 in-place */
            dest_p = scratch + (sizeof(struct darshan_base_record) +
                (3 * sizeof(int64_t)));
            src_p = dest_p - (2 * sizeof(int64_t));
            len = 4 * sizeof(double);
            memmove(dest_p, src_p, len);
            /* set H5F_FLUSHES, H5F_USE_MPIIO to -1 */
            *((int64_t *)src_p) = -1;
            *((int64_t *)(src_p + sizeof(int64_t))) = -1;
            /* set H5F_F_META_TIME to -1 */
            *((double *)(dest_p + (4 * sizeof(double)))) = -1;
        }

        memcpy(file, scratch, sizeof(struct darshan_hdf5_file));
    }

exit:
    if(*hdf5_buf_p == NULL)
    {
        if(ret == rec_len)
            *hdf5_buf_p = file;
        else
            free(file);
    }

    if(ret < 0)
        return(-1);
    else if(ret < rec_len)
        return(0);
    else
    {
        /* if the read was successful, do any necessary byte-swapping */
        if(fd->swap_flag)
        {
            DARSHAN_BSWAP64(&(file->base_rec.id));
            DARSHAN_BSWAP64(&(file->base_rec.rank));
            for(i=0; i<H5F_NUM_INDICES; i++)
            {
                /* skip counters we explicitly set to -1 since they don't
                 * need to be byte swapped
                 */
                if((fd->mod_ver[DARSHAN_H5F_MOD] < 3) &&
                    ((i == H5F_FLUSHES) || (i == H5F_USE_MPIIO)))
                    continue;
                DARSHAN_BSWAP64(&file->counters[i]);
            }
            for(i=0; i<H5F_F_NUM_INDICES; i++)
            {
                /* skip counters we explicitly set to -1 since they don't
                 * need to be byte swapped
                 */
                if((fd->mod_ver[DARSHAN_H5F_MOD] == 1) &&
                    ((i == H5F_F_CLOSE_START_TIMESTAMP) ||
                     (i == H5F_F_OPEN_END_TIMESTAMP)))
                    continue;
                if((fd->mod_ver[DARSHAN_H5F_MOD] < 3) &&
                    (i == H5F_F_META_TIME))
                    continue;
                DARSHAN_BSWAP64(&file->fcounters[i]);
            }
        }

        return(1);
    }
}

static int darshan_log_get_hdf5_dataset(darshan_fd fd, void** hdf5_buf_p)
{
    struct darshan_hdf5_dataset *ds = *((struct darshan_hdf5_dataset **)hdf5_buf_p);
    int rec_len;
    int i;
    int ret;

    if(fd->mod_map[DARSHAN_H5D_MOD].len == 0)
        return(0);

    if(fd->mod_ver[DARSHAN_H5D_MOD] == 0 ||
        fd->mod_ver[DARSHAN_H5D_MOD] > DARSHAN_H5D_VER)
    {
        fprintf(stderr, "Error: Invalid H5D module version number (got %d)\n",
            fd->mod_ver[DARSHAN_H5D_MOD]);
        ret = -1;
        goto exit;
    }

    if(*hdf5_buf_p == NULL)
    {
        ds = malloc(sizeof(*ds));
        if(!ds)
        {
            ret = -1;
            goto exit;
        }
    }

    if(fd->mod_ver[DARSHAN_H5D_MOD] == DARSHAN_H5D_VER)
    {
        /* log format is in current version, so we don't need to do any
         * translation of counters while reading
         */
        rec_len = sizeof(struct darshan_hdf5_dataset);
        ret = darshan_log_get_mod(fd, DARSHAN_H5D_MOD, ds, rec_len);
    }
    else
    {
        char scratch[1024] = {0};
        char *src_p, *dest_p;
        int len;

        if(fd->mod_ver[DARSHAN_H5D_MOD] == 1)
        {
            rec_len = DARSHAN_H5D_DATASET_SIZE_1;
            ret = darshan_log_get_mod(fd, DARSHAN_H5D_MOD, scratch, rec_len);
            if(ret != rec_len)
                goto exit;

            /* upconvert version 1 to version 2 in-place */
            dest_p = scratch + sizeof(struct darshan_base_record) + sizeof(uint64_t);
            src_p = dest_p - sizeof(uint64_t);
            len = DARSHAN_H5D_DATASET_SIZE_1 - sizeof(struct darshan_base_record);
            memmove(dest_p, src_p, len);
            /* set FILE_REC_ID to 0 */
            *((uint64_t *)src_p) = 0;
        }

        memcpy(ds, scratch, sizeof(struct darshan_hdf5_dataset));
    }

exit:
    if(*hdf5_buf_p == NULL)
    {
        if(ret == rec_len)
            *hdf5_buf_p = ds;
        else
            free(ds);
    }

    if(ret < 0)
        return(-1);
    else if(ret < rec_len)
        return(0);
    else
    {
        /* if the read was successful, do any necessary byte-swapping */
        if(fd->swap_flag)
        {
            DARSHAN_BSWAP64(&(ds->base_rec.id));
            DARSHAN_BSWAP64(&(ds->base_rec.rank));
            /* skip counters we explicitly set to 0 since they don't
             * need to be byte swapped
             */
            if(fd->mod_ver[DARSHAN_H5F_MOD] >= 2)
                DARSHAN_BSWAP64(&ds->file_rec_id);
            for(i=0; i<H5D_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&ds->counters[i]);
            for(i=0; i<H5D_F_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&ds->fcounters[i]);
        }

        return(1);
    }
}

static int darshan_log_put_hdf5_file(darshan_fd fd, void* hdf5_buf)
{
    struct darshan_hdf5_file *file = (struct darshan_hdf5_file *)hdf5_buf;
    int ret;

    ret = darshan_log_put_mod(fd, DARSHAN_H5F_MOD, file,
        sizeof(struct darshan_hdf5_file), DARSHAN_H5F_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

static int darshan_log_put_hdf5_dataset(darshan_fd fd, void* hdf5_buf)
{
    struct darshan_hdf5_dataset *ds = (struct darshan_hdf5_dataset *)hdf5_buf;
    int ret;

    ret = darshan_log_put_mod(fd, DARSHAN_H5D_MOD, ds,
        sizeof(struct darshan_hdf5_dataset), DARSHAN_H5D_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

static void darshan_log_print_hdf5_file(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type)
{
    int i;
    struct darshan_hdf5_file *hdf5_file_rec =
        (struct darshan_hdf5_file *)file_rec;

    for(i=0; i<H5F_NUM_INDICES; i++)
    {
        DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_H5F_MOD],
            hdf5_file_rec->base_rec.rank, hdf5_file_rec->base_rec.id,
            h5f_counter_names[i], hdf5_file_rec->counters[i],
            file_name, mnt_pt, fs_type);
    }

    for(i=0; i<H5F_F_NUM_INDICES; i++)
    {
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_H5F_MOD],
            hdf5_file_rec->base_rec.rank, hdf5_file_rec->base_rec.id,
            h5f_f_counter_names[i], hdf5_file_rec->fcounters[i],
            file_name, mnt_pt, fs_type);
    }

    return;
}

static void darshan_log_print_hdf5_dataset(void *ds_rec, char *ds_name,
    char *mnt_pt, char *fs_type)
{
    int i;
    struct darshan_hdf5_dataset *hdf5_ds_rec =
        (struct darshan_hdf5_dataset *)ds_rec;

    for(i=0; i<H5D_NUM_INDICES; i++)
    {
        DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_H5D_MOD],
            hdf5_ds_rec->base_rec.rank, hdf5_ds_rec->base_rec.id,
            h5d_counter_names[i], hdf5_ds_rec->counters[i],
            ds_name, mnt_pt, fs_type);
    }

    for(i=0; i<H5D_F_NUM_INDICES; i++)
    {
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_H5D_MOD],
            hdf5_ds_rec->base_rec.rank, hdf5_ds_rec->base_rec.id,
            h5d_f_counter_names[i], hdf5_ds_rec->fcounters[i],
            ds_name, mnt_pt, fs_type);
    }

    DARSHAN_U_COUNTER_PRINT(darshan_module_names[DARSHAN_H5D_MOD],
        hdf5_ds_rec->base_rec.rank, hdf5_ds_rec->base_rec.id,
        "H5D_FILE_REC_ID", hdf5_ds_rec->file_rec_id,
        ds_name, mnt_pt, fs_type);

    return;
}

static void darshan_log_print_hdf5_file_description(int ver)
{
    printf("\n# description of HDF5 counters:\n");
    printf("#   H5F_OPENS: HDF5 file open/create operation counts.\n");
    printf("#   H5F_FLUSHES: HDF5 file flush operation counts.\n");
    printf("#   H5F_USE_MPIIO: flag indicating whether MPI-IO was used to access this file.\n");
    printf("#   H5F_F_*_START_TIMESTAMP: timestamp of first HDF5 file open/close.\n");
    printf("#   H5F_F_*_END_TIMESTAMP: timestamp of last HDF5 file open/close.\n");
    printf("#   H5F_F_META_TIME: cumulative time spent in HDF5 metadata operations.\n");

    if(ver == 1)
    {
        printf("\n# WARNING: H5F module log format version 1 does not support the following counters:\n");
        printf("# - H5F_F_CLOSE_START_TIMESTAMP\n");
        printf("# - H5F_F_OPEN_END_TIMESTAMP\n");
    }
    if(ver <= 2)
    {
        printf("\n# WARNING: H5F module log format version <=2 does not support the following counters:\n");
        printf("# \t- H5F_FLUSHES\n");
        printf("# \t- H5F_USE_MPIIO\n");
        printf("# \t- H5F_F_META_TIME\n");
    }

    return;
}

static void darshan_log_print_hdf5_dataset_description(int ver)
{
    printf("\n# description of HDF5 counters:\n");
    printf("#   H5D_OPENS: HDF5 dataset open/create operation counts.\n");
    printf("#   H5D_READS: HDF5 dataset read operation counts.\n");
    printf("#   H5D_WRITES: HDF5 dataset write operation counts.\n");
    printf("#   H5D_FLUSHES: HDF5 dataset flush operation counts.\n");
    printf("#   H5D_BYTES_*: total bytes read and written at HDF5 dataset layer.\n");
    printf("#   H5D_RW_SWITCHES: number of times access alternated between read and write.\n");
    printf("#   H5D_*_SELECTS: number of different access selections (regular/irregular hyperslab or points).\n");
    printf("#   H5D_MAX_*_TIME_SIZE: size of the slowest read and write operations.\n");
    printf("#   H5D_SIZE_*_AGG_*: histogram of H5D total access sizes for read and write operations.\n");
    printf("#   H5D_ACCESS*_*: the four most common total accesses, in terms of size and length/stride (in last 5 dimensions).\n");
    printf("#   H5D_ACCESS*_COUNT: count of the four most common total access sizes.\n");
    printf("#   H5D_DATASPACE_NDIMS: number of dimensions in dataset's dataspace.\n");
    printf("#   H5D_DATASPACE_NPOINTS: number of points in dataset's dataspace.\n");
    printf("#   H5D_DATATYPE_SIZE: size of each dataset element.\n");
    printf("#   H5D_CHUNK_SIZE_D*: chunk size in last 5 dimensions of dataset.\n");
    printf("#   H5D_USE_MPIIO_COLLECTIVE: flag indicating whether MPI-IO collectives were used to access this dataset.\n");
    printf("#   H5D_USE_DEPRECATED: flag indicating whether deprecated H5D calls were used.\n");
    printf("#   H5D_*_RANK: rank of the processes that were the fastest and slowest at I/O (for shared datasets).\n");
    printf("#   H5D_*_RANK_BYTES: total bytes transferred at H5D layer by the fastest and slowest ranks (for shared datasets).\n");
    printf("#   H5D_F_*_START_TIMESTAMP: timestamp of first HDF5 dataset open/read/write/close.\n");
    printf("#   H5D_F_*_END_TIMESTAMP: timestamp of last HDF5 datset open/read/write/close.\n");
    printf("#   H5D_F_READ/WRITE/META_TIME: cumulative time spent in H5D read, write, or metadata operations.\n");
    printf("#   H5D_F_MAX_*_TIME: duration of the slowest H5D read and write operations.\n");
    printf("#   H5D_F_*_RANK_TIME: fastest and slowest I/O time for a single rank (for shared datasets).\n");
    printf("#   H5D_F_VARIANCE_RANK_*: variance of total I/O time and bytes moved for all ranks (for shared datasets).\n");
    printf("#   H5D_FILE_REC_ID: Darshan file record ID of the file the dataset belongs to.\n");

    if(ver == 1)
    {
        printf("\n# WARNING: H5D module log format version 1 does not support the following counters:\n");
        printf("# - H5D_FILE_REC_ID\n");
    }

    return;
}

static void darshan_log_print_hdf5_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2)
{
    struct darshan_hdf5_file *file1 = (struct darshan_hdf5_file *)file_rec1;
    struct darshan_hdf5_file *file2 = (struct darshan_hdf5_file *)file_rec2;
    int i;

    /* NOTE: we assume that both input records are the same module format version */

    for(i=0; i<H5F_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_H5F_MOD],
                file1->base_rec.rank, file1->base_rec.id, h5f_counter_names[i],
                file1->counters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_H5F_MOD],
                file2->base_rec.rank, file2->base_rec.id, h5f_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
        else if(file1->counters[i] != file2->counters[i])
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_H5F_MOD],
                file1->base_rec.rank, file1->base_rec.id, h5f_counter_names[i],
                file1->counters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_H5F_MOD],
                file2->base_rec.rank, file2->base_rec.id, h5f_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
    }

    for(i=0; i<H5F_F_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_H5F_MOD],
                file1->base_rec.rank, file1->base_rec.id, h5f_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_H5F_MOD],
                file2->base_rec.rank, file2->base_rec.id, h5f_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
        else if(file1->fcounters[i] != file2->fcounters[i])
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_H5F_MOD],
                file1->base_rec.rank, file1->base_rec.id, h5f_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_H5F_MOD],
                file2->base_rec.rank, file2->base_rec.id, h5f_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
    }

    return;
}

static void darshan_log_print_hdf5_dataset_diff(void *ds_rec1, char *ds_name1,
    void *ds_rec2, char *ds_name2)
{
    struct darshan_hdf5_dataset *ds1 = (struct darshan_hdf5_dataset *)ds_rec1;
    struct darshan_hdf5_dataset *ds2 = (struct darshan_hdf5_dataset *)ds_rec2;
    int i;

    /* NOTE: we assume that both input records are the same module format version */

    for(i=0; i<H5D_NUM_INDICES; i++)
    {
        if(!ds2)
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_H5D_MOD],
                ds1->base_rec.rank, ds1->base_rec.id, h5d_counter_names[i],
                ds1->counters[i], ds_name1, "", "");

        }
        else if(!ds1)
        {
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_H5D_MOD],
                ds2->base_rec.rank, ds2->base_rec.id, h5d_counter_names[i],
                ds2->counters[i], ds_name2, "", "");
        }
        else if(ds1->counters[i] != ds2->counters[i])
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_H5D_MOD],
                ds1->base_rec.rank, ds1->base_rec.id, h5d_counter_names[i],
                ds1->counters[i], ds_name1, "", "");
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_H5D_MOD],
                ds2->base_rec.rank, ds2->base_rec.id, h5d_counter_names[i],
                ds2->counters[i], ds_name2, "", "");
        }
    }

    for(i=0; i<H5D_F_NUM_INDICES; i++)
    {
        if(!ds2)
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_H5D_MOD],
                ds1->base_rec.rank, ds1->base_rec.id, h5d_f_counter_names[i],
                ds1->fcounters[i], ds_name1, "", "");

        }
        else if(!ds1)
        {
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_H5D_MOD],
                ds2->base_rec.rank, ds2->base_rec.id, h5d_f_counter_names[i],
                ds2->fcounters[i], ds_name2, "", "");
        }
        else if(ds1->fcounters[i] != ds2->fcounters[i])
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_H5D_MOD],
                ds1->base_rec.rank, ds1->base_rec.id, h5d_f_counter_names[i],
                ds1->fcounters[i], ds_name1, "", "");
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_H5D_MOD],
                ds2->base_rec.rank, ds2->base_rec.id, h5d_f_counter_names[i],
                ds2->fcounters[i], ds_name2, "", "");
        }
    }

    return;
}

static void darshan_log_agg_hdf5_files(void *rec, void *agg_rec, int init_flag)
{
    struct darshan_hdf5_file *hdf5_rec = (struct darshan_hdf5_file *)rec;
    struct darshan_hdf5_file *agg_hdf5_rec = (struct darshan_hdf5_file *)agg_rec;
    int i;

    for(i = 0; i < H5F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case H5F_OPENS:
            case H5F_FLUSHES:
                /* sum */
                agg_hdf5_rec->counters[i] += hdf5_rec->counters[i];
                break;
            case H5F_USE_MPIIO:
                if(hdf5_rec->counters[i] > 0)
                    agg_hdf5_rec->counters[i] = 1;
                break;
            default:
                agg_hdf5_rec->counters[i] = -1;
                break;
        }
    }

    for(i = 0; i < H5F_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case H5F_F_OPEN_START_TIMESTAMP:
            case H5F_F_CLOSE_START_TIMESTAMP:
                /* minimum non-zero */
                if((hdf5_rec->fcounters[i] > 0)  &&
                    ((agg_hdf5_rec->fcounters[i] == 0) ||
                    (hdf5_rec->fcounters[i] < agg_hdf5_rec->fcounters[i])))
                {
                    agg_hdf5_rec->fcounters[i] = hdf5_rec->fcounters[i];
                }
                break;
            case H5F_F_OPEN_END_TIMESTAMP:
            case H5F_F_CLOSE_END_TIMESTAMP:
                /* maximum */
                if(hdf5_rec->fcounters[i] > agg_hdf5_rec->fcounters[i])
                {
                    agg_hdf5_rec->fcounters[i] = hdf5_rec->fcounters[i];
                }
                break;
            case H5F_F_META_TIME:
                /* sum */
                agg_hdf5_rec->counters[i] += hdf5_rec->counters[i];
                break;
            default:
                agg_hdf5_rec->fcounters[i] = -1;
                break;
        }
    }

    return;
}

/* simple helper struct for determining time & byte variances */
struct var_t
{
    double n;
    double M;
    double S;
};

static void darshan_log_agg_hdf5_datasets(void *rec, void *agg_rec, int init_flag)
{
    struct darshan_hdf5_dataset *hdf5_rec = (struct darshan_hdf5_dataset *)rec;
    struct darshan_hdf5_dataset *agg_hdf5_rec = (struct darshan_hdf5_dataset *)agg_rec;
    int i, j, j2, k, k2;
    int total_count;
    int64_t tmp_val[4 * (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1)];
    int64_t tmp_cnt[4];
    int tmp_ndx;
    double old_M;
    double hdf5_time = hdf5_rec->fcounters[H5D_F_READ_TIME] +
        hdf5_rec->fcounters[H5D_F_WRITE_TIME] +
        hdf5_rec->fcounters[H5D_F_META_TIME];
    double hdf5_bytes = (double)hdf5_rec->counters[H5D_BYTES_READ] +
        hdf5_rec->counters[H5D_BYTES_WRITTEN];
    struct var_t *var_time_p = (struct var_t *)
        ((char *)rec + sizeof(struct darshan_hdf5_dataset));
    struct var_t *var_bytes_p = (struct var_t *)
        ((char *)var_time_p + sizeof(struct var_t));

    agg_hdf5_rec->file_rec_id = hdf5_rec->file_rec_id;
    for(i = 0; i < H5D_NUM_INDICES; i++)
    {
        switch(i)
        {
            case H5D_OPENS:
            case H5D_READS:
            case H5D_WRITES:
            case H5D_FLUSHES:
            case H5D_BYTES_READ:
            case H5D_BYTES_WRITTEN:
            case H5D_RW_SWITCHES:
            case H5D_REGULAR_HYPERSLAB_SELECTS:
            case H5D_IRREGULAR_HYPERSLAB_SELECTS:
            case H5D_POINT_SELECTS:
            case H5D_SIZE_READ_AGG_0_100:
            case H5D_SIZE_READ_AGG_100_1K:
            case H5D_SIZE_READ_AGG_1K_10K:
            case H5D_SIZE_READ_AGG_10K_100K:
            case H5D_SIZE_READ_AGG_100K_1M:
            case H5D_SIZE_READ_AGG_1M_4M:
            case H5D_SIZE_READ_AGG_4M_10M:
            case H5D_SIZE_READ_AGG_10M_100M:
            case H5D_SIZE_READ_AGG_100M_1G:
            case H5D_SIZE_READ_AGG_1G_PLUS:
            case H5D_SIZE_WRITE_AGG_0_100:
            case H5D_SIZE_WRITE_AGG_100_1K:
            case H5D_SIZE_WRITE_AGG_1K_10K:
            case H5D_SIZE_WRITE_AGG_10K_100K:
            case H5D_SIZE_WRITE_AGG_100K_1M:
            case H5D_SIZE_WRITE_AGG_1M_4M:
            case H5D_SIZE_WRITE_AGG_4M_10M:
            case H5D_SIZE_WRITE_AGG_10M_100M:
            case H5D_SIZE_WRITE_AGG_100M_1G:
            case H5D_SIZE_WRITE_AGG_1G_PLUS:
                /* sum */
                agg_hdf5_rec->counters[i] += hdf5_rec->counters[i];
                break;
            case H5D_MAX_READ_TIME_SIZE:
            case H5D_MAX_WRITE_TIME_SIZE:
            case H5D_FASTEST_RANK:
            case H5D_FASTEST_RANK_BYTES:
            case H5D_SLOWEST_RANK:
            case H5D_SLOWEST_RANK_BYTES:
                /* these are set with the FP counters */
                break;
            case H5D_ACCESS1_ACCESS:
                /* increment common value counters */
                if(hdf5_rec->counters[i] == 0) break;

                /* first, collapse duplicates */
                for(j = i, j2 = H5D_ACCESS1_COUNT; j <= H5D_ACCESS4_ACCESS;
                    j += (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1), j2++)
                {
                    for(k = i, k2 = H5D_ACCESS1_COUNT; k <= H5D_ACCESS4_ACCESS;
                        k += (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1), k2++)
                    {
                        if(!memcmp(&hdf5_rec->counters[j], &agg_hdf5_rec->counters[k],
                            sizeof(int64_t) * (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1)))
                        {
                            memset(&hdf5_rec->counters[j], 0, sizeof(int64_t) *
                                (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1));
                            agg_hdf5_rec->counters[k2] += hdf5_rec->counters[j2];
                            hdf5_rec->counters[j2] = 0;
                        }
                    }
                }

                /* second, add new counters */
                for(j = i, j2 = H5D_ACCESS1_COUNT; j <= H5D_ACCESS4_ACCESS;
                    j += (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1), j2++)
                {
                    tmp_ndx = 0;
                    memset(tmp_val, 0, 4 * (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1) * sizeof(int64_t));
                    memset(tmp_cnt, 0, 4 * sizeof(int64_t));

                    if(hdf5_rec->counters[j] == 0) break;
                    for(k = i, k2 = H5D_ACCESS1_COUNT; k <= H5D_ACCESS4_ACCESS;
                        k += (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1), k2++)
                    {
                        if(!memcmp(&hdf5_rec->counters[j], &agg_hdf5_rec->counters[k],
                            sizeof(int64_t) * (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1)))
                        {
                            total_count = agg_hdf5_rec->counters[k2] +
                                hdf5_rec->counters[j2];
                            break;
                        }
                    }
                    if(k > H5D_ACCESS4_ACCESS) total_count = hdf5_rec->counters[j2];

                    for(k = i, k2 = H5D_ACCESS1_COUNT; k <= H5D_ACCESS4_ACCESS;
                        k += (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1), k2++)
                    {
                        if((agg_hdf5_rec->counters[k2] > total_count) ||
                           ((agg_hdf5_rec->counters[k2] == total_count) &&
                            (agg_hdf5_rec->counters[k] > hdf5_rec->counters[j])))
                        {
                            memcpy(&tmp_val[tmp_ndx * (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1)],
                                &agg_hdf5_rec->counters[k],
                                sizeof(int64_t) * (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1));
                            tmp_cnt[tmp_ndx] = agg_hdf5_rec->counters[k2];
                            tmp_ndx++;
                        }
                        else break;
                    }
                    if(tmp_ndx == 4) break;

                    memcpy(&tmp_val[tmp_ndx * (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1)],
                        &hdf5_rec->counters[j],
                        sizeof(int64_t) * (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1));
                    tmp_cnt[tmp_ndx] = hdf5_rec->counters[j2];
                    tmp_ndx++;

                    while(tmp_ndx != 4)
                    {
                        if(memcmp(&agg_hdf5_rec->counters[k], &hdf5_rec->counters[j],
                            sizeof(int64_t) * (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1)))
                        {
                            memcpy(&tmp_val[tmp_ndx * (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1)],
                                &agg_hdf5_rec->counters[k],
                                sizeof(int64_t) * (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1));
                            tmp_cnt[tmp_ndx] = agg_hdf5_rec->counters[k2];
                            tmp_ndx++;
                        }
                        k += (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1);
                        k2++;
                    }
                    memcpy(&(agg_hdf5_rec->counters[H5D_ACCESS1_ACCESS]), tmp_val,
                        4 * (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1) * sizeof(int64_t));
                    memcpy(&(agg_hdf5_rec->counters[H5D_ACCESS1_COUNT]), tmp_cnt,
                        4 * sizeof(int64_t));
                }
                break;
            case H5D_ACCESS1_LENGTH_D1:
            case H5D_ACCESS1_LENGTH_D2:
            case H5D_ACCESS1_LENGTH_D3:
            case H5D_ACCESS1_LENGTH_D4:
            case H5D_ACCESS1_LENGTH_D5:
            case H5D_ACCESS1_STRIDE_D1:
            case H5D_ACCESS1_STRIDE_D2:
            case H5D_ACCESS1_STRIDE_D3:
            case H5D_ACCESS1_STRIDE_D4:
            case H5D_ACCESS1_STRIDE_D5:
            case H5D_ACCESS2_ACCESS:
            case H5D_ACCESS2_LENGTH_D1:
            case H5D_ACCESS2_LENGTH_D2:
            case H5D_ACCESS2_LENGTH_D3:
            case H5D_ACCESS2_LENGTH_D4:
            case H5D_ACCESS2_LENGTH_D5:
            case H5D_ACCESS2_STRIDE_D1:
            case H5D_ACCESS2_STRIDE_D2:
            case H5D_ACCESS2_STRIDE_D3:
            case H5D_ACCESS2_STRIDE_D4:
            case H5D_ACCESS2_STRIDE_D5:
            case H5D_ACCESS3_ACCESS:
            case H5D_ACCESS3_LENGTH_D1:
            case H5D_ACCESS3_LENGTH_D2:
            case H5D_ACCESS3_LENGTH_D3:
            case H5D_ACCESS3_LENGTH_D4:
            case H5D_ACCESS3_LENGTH_D5:
            case H5D_ACCESS3_STRIDE_D1:
            case H5D_ACCESS3_STRIDE_D2:
            case H5D_ACCESS3_STRIDE_D3:
            case H5D_ACCESS3_STRIDE_D4:
            case H5D_ACCESS3_STRIDE_D5:
            case H5D_ACCESS4_ACCESS:
            case H5D_ACCESS4_LENGTH_D1:
            case H5D_ACCESS4_LENGTH_D2:
            case H5D_ACCESS4_LENGTH_D3:
            case H5D_ACCESS4_LENGTH_D4:
            case H5D_ACCESS4_LENGTH_D5:
            case H5D_ACCESS4_STRIDE_D1:
            case H5D_ACCESS4_STRIDE_D2:
            case H5D_ACCESS4_STRIDE_D3:
            case H5D_ACCESS4_STRIDE_D4:
            case H5D_ACCESS4_STRIDE_D5:
            case H5D_ACCESS1_COUNT:
            case H5D_ACCESS2_COUNT:
            case H5D_ACCESS3_COUNT:
            case H5D_ACCESS4_COUNT:
                /* these are set all at once with common counters above */
                break;
            case H5D_DATASPACE_NDIMS:
            case H5D_DATASPACE_NPOINTS:
            case H5D_DATATYPE_SIZE:
            case H5D_CHUNK_SIZE_D1:
            case H5D_CHUNK_SIZE_D2:
            case H5D_CHUNK_SIZE_D3:
            case H5D_CHUNK_SIZE_D4:
            case H5D_CHUNK_SIZE_D5:
                /* just set to the input value */
                agg_hdf5_rec->counters[i] = hdf5_rec->counters[i];
                break;
            case H5D_USE_MPIIO_COLLECTIVE:
            case H5D_USE_DEPRECATED:
                if(hdf5_rec->counters[i] > 0)
                    agg_hdf5_rec->counters[i] = 1;
                break;
            default:
                agg_hdf5_rec->counters[i] = -1;
                break;
        }
    }

    for(i = 0; i < H5D_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case H5D_F_READ_TIME:
            case H5D_F_WRITE_TIME:
            case H5D_F_META_TIME:
                /* sum */
                agg_hdf5_rec->fcounters[i] += hdf5_rec->fcounters[i];
                break;
            case H5D_F_OPEN_START_TIMESTAMP:
            case H5D_F_READ_START_TIMESTAMP:
            case H5D_F_WRITE_START_TIMESTAMP:
            case H5D_F_CLOSE_START_TIMESTAMP:
                /* minimum non-zero */
                if((hdf5_rec->fcounters[i] > 0)  &&
                    ((agg_hdf5_rec->fcounters[i] == 0) ||
                    (hdf5_rec->fcounters[i] < agg_hdf5_rec->fcounters[i])))
                {
                    agg_hdf5_rec->fcounters[i] = hdf5_rec->fcounters[i];
                }
                break;
            case H5D_F_OPEN_END_TIMESTAMP:
            case H5D_F_READ_END_TIMESTAMP:
            case H5D_F_WRITE_END_TIMESTAMP:
            case H5D_F_CLOSE_END_TIMESTAMP:
                /* maximum */
                if(hdf5_rec->fcounters[i] > agg_hdf5_rec->fcounters[i])
                {
                    agg_hdf5_rec->fcounters[i] = hdf5_rec->fcounters[i];
                }
                break;
            case H5D_F_MAX_READ_TIME:
                if(hdf5_rec->fcounters[i] > agg_hdf5_rec->fcounters[i])
                {
                    agg_hdf5_rec->fcounters[i] = hdf5_rec->fcounters[i];
                    agg_hdf5_rec->counters[H5D_MAX_READ_TIME_SIZE] =
                        hdf5_rec->counters[H5D_MAX_READ_TIME_SIZE];
                }
                break;
            case H5D_F_MAX_WRITE_TIME:
                if(hdf5_rec->fcounters[i] > agg_hdf5_rec->fcounters[i])
                {
                    agg_hdf5_rec->fcounters[i] = hdf5_rec->fcounters[i];
                    agg_hdf5_rec->counters[H5D_MAX_WRITE_TIME_SIZE] =
                        hdf5_rec->counters[H5D_MAX_WRITE_TIME_SIZE];
                }
                break;
            case H5D_F_FASTEST_RANK_TIME:
                if(init_flag)
                {
                    /* set fastest rank counters according to root rank. these counters
                     * will be determined as the aggregation progresses.
                     */
                    agg_hdf5_rec->counters[H5D_FASTEST_RANK] = hdf5_rec->base_rec.rank;
                    agg_hdf5_rec->counters[H5D_FASTEST_RANK_BYTES] = hdf5_bytes;
                    agg_hdf5_rec->fcounters[H5D_F_FASTEST_RANK_TIME] = hdf5_time;
                }

                if(hdf5_time < agg_hdf5_rec->fcounters[H5D_F_FASTEST_RANK_TIME])
                {
                    agg_hdf5_rec->counters[H5D_FASTEST_RANK] = hdf5_rec->base_rec.rank;
                    agg_hdf5_rec->counters[H5D_FASTEST_RANK_BYTES] = hdf5_bytes;
                    agg_hdf5_rec->fcounters[H5D_F_FASTEST_RANK_TIME] = hdf5_time;
                }
                break;
            case H5D_F_SLOWEST_RANK_TIME:
                if(init_flag)
                {   
                    /* set slowest rank counters according to root rank. these counters
                     * will be determined as the aggregation progresses.
                     */
                    agg_hdf5_rec->counters[H5D_SLOWEST_RANK] = hdf5_rec->base_rec.rank;
                    agg_hdf5_rec->counters[H5D_SLOWEST_RANK_BYTES] = hdf5_bytes;
                    agg_hdf5_rec->fcounters[H5D_F_SLOWEST_RANK_TIME] = hdf5_time;
                }   
                    
                if(hdf5_time > agg_hdf5_rec->fcounters[H5D_F_SLOWEST_RANK_TIME])
                {
                    agg_hdf5_rec->counters[H5D_SLOWEST_RANK] = hdf5_rec->base_rec.rank;
                    agg_hdf5_rec->counters[H5D_SLOWEST_RANK_BYTES] = hdf5_bytes;
                    agg_hdf5_rec->fcounters[H5D_F_SLOWEST_RANK_TIME] = hdf5_time;
                }
                break;
            case H5D_F_VARIANCE_RANK_TIME:
                if(init_flag)
                {   
                    var_time_p->n = 1;
                    var_time_p->M = hdf5_time;
                    var_time_p->S = 0;
                }
                else
                {
                    old_M = var_time_p->M;

                    var_time_p->n++;
                    var_time_p->M += (hdf5_time - var_time_p->M) / var_time_p->n;
                    var_time_p->S += (hdf5_time - var_time_p->M) * (hdf5_time - old_M);

                    agg_hdf5_rec->fcounters[H5D_F_VARIANCE_RANK_TIME] =
                        var_time_p->S / var_time_p->n;
                }
                break;
            case H5D_F_VARIANCE_RANK_BYTES:
                if(init_flag)
                {
                    var_bytes_p->n = 1;
                    var_bytes_p->M = hdf5_bytes;
                    var_bytes_p->S = 0;
                }
                else
                {
                    old_M = var_bytes_p->M;

                    var_bytes_p->n++;
                    var_bytes_p->M += (hdf5_bytes - var_bytes_p->M) / var_bytes_p->n;
                    var_bytes_p->S += (hdf5_bytes - var_bytes_p->M) * (hdf5_bytes - old_M);

                    agg_hdf5_rec->fcounters[H5D_F_VARIANCE_RANK_BYTES] =
                        var_bytes_p->S / var_bytes_p->n;
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
