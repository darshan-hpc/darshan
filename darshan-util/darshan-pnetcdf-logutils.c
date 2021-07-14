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

/* counter name strings for the PNETCDF module */
#define X(a) #a,
char *pnetcdf_counter_names[] = {
    PNETCDF_COUNTERS
};

char *pnetcdf_f_counter_names[] = {
    PNETCDF_F_COUNTERS
};
#undef X

#define DARSHAN_PNETCDF_FILE_SIZE_1 48

static int darshan_log_get_pnetcdf_file(darshan_fd fd, void** pnetcdf_buf_p);
static int darshan_log_put_pnetcdf_file(darshan_fd fd, void* pnetcdf_buf);
static void darshan_log_print_pnetcdf_file(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type);
static void darshan_log_print_pnetcdf_description(int ver);
static void darshan_log_print_pnetcdf_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2);
static void darshan_log_agg_pnetcdf_files(void *rec, void *agg_rec, int init_flag);

struct darshan_mod_logutil_funcs pnetcdf_logutils =
{
    .log_get_record = &darshan_log_get_pnetcdf_file,
    .log_put_record = &darshan_log_put_pnetcdf_file,
    .log_print_record = &darshan_log_print_pnetcdf_file,
    .log_print_description = &darshan_log_print_pnetcdf_description,
    .log_print_diff = &darshan_log_print_pnetcdf_file_diff,
    .log_agg_records = &darshan_log_agg_pnetcdf_files
};

static int darshan_log_get_pnetcdf_file(darshan_fd fd, void** pnetcdf_buf_p)
{
    struct darshan_pnetcdf_file *file = *((struct darshan_pnetcdf_file **)pnetcdf_buf_p);
    int rec_len;
    int i;
    int ret;

    if(fd->mod_map[DARSHAN_PNETCDF_MOD].len == 0)
        return(0);

    if(fd->mod_ver[DARSHAN_PNETCDF_MOD] == 0 ||
        fd->mod_ver[DARSHAN_PNETCDF_MOD] > DARSHAN_PNETCDF_VER)
    {
        fprintf(stderr, "Error: Invalid PNetCDF module version number (got %d)\n",
            fd->mod_ver[DARSHAN_PNETCDF_MOD]);
        return(-1);
    }

    if(*pnetcdf_buf_p == NULL)
    {
        file = malloc(sizeof(*file));
        if(!file)
            return(-1);
    }

    if(fd->mod_ver[DARSHAN_PNETCDF_MOD] == DARSHAN_PNETCDF_VER)
    {
        /* log format is in current version, so we don't need to do any
         * translation of counters while reading
         */
        rec_len = sizeof(struct darshan_pnetcdf_file);
        ret = darshan_log_get_mod(fd, DARSHAN_PNETCDF_MOD, file, rec_len);
    }
    else
    {
        char scratch[1024] = {0};
        char *src_p, *dest_p;
        int len;

        rec_len = DARSHAN_PNETCDF_FILE_SIZE_1;
        ret = darshan_log_get_mod(fd, DARSHAN_PNETCDF_MOD, scratch, rec_len);
        if(ret != rec_len)
            goto exit;

        /* upconvert version 1 to version 2 in-place */
        dest_p = scratch + (sizeof(struct darshan_base_record) +
            (2 * sizeof(int64_t)) + (3 * sizeof(double)));
        src_p = dest_p - (2 * sizeof(double));
        len = sizeof(double);
        memmove(dest_p, src_p, len);
        /* set F_CLOSE_START and F_OPEN_END to -1 */
        *((double *)src_p) = -1;
        *((double *)(src_p + sizeof(double))) = -1;

        memcpy(file, scratch, sizeof(struct darshan_pnetcdf_file));
    }

exit:
    if(*pnetcdf_buf_p == NULL)
    {
        if(ret == rec_len)
            *pnetcdf_buf_p = file;
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
            for(i=0; i<PNETCDF_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->counters[i]);
            for(i=0; i<PNETCDF_F_NUM_INDICES; i++)
            {
                /* skip counters we explicitly set to -1 since they don't
                 * need to be byte swapped
                 */
                if((fd->mod_ver[DARSHAN_PNETCDF_MOD] == 1) &&
                    ((i == PNETCDF_F_CLOSE_START_TIMESTAMP) ||
                     (i == PNETCDF_F_OPEN_END_TIMESTAMP)))
                    continue;
                DARSHAN_BSWAP64(&file->fcounters[i]);
            }
        }

        return(1);
    }
}

static int darshan_log_put_pnetcdf_file(darshan_fd fd, void* pnetcdf_buf)
{
    struct darshan_pnetcdf_file *file = (struct darshan_pnetcdf_file *)pnetcdf_buf;
    int ret;

    ret = darshan_log_put_mod(fd, DARSHAN_PNETCDF_MOD, file,
        sizeof(struct darshan_pnetcdf_file), DARSHAN_PNETCDF_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

static void darshan_log_print_pnetcdf_file(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type)
{
    int i;
    struct darshan_pnetcdf_file *pnetcdf_file_rec =
        (struct darshan_pnetcdf_file *)file_rec;

    for(i=0; i<PNETCDF_NUM_INDICES; i++)
    {
        DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_MOD],
            pnetcdf_file_rec->base_rec.rank, pnetcdf_file_rec->base_rec.id,
            pnetcdf_counter_names[i], pnetcdf_file_rec->counters[i],
            file_name, mnt_pt, fs_type);
    }

    for(i=0; i<PNETCDF_F_NUM_INDICES; i++)
    {
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_MOD],
            pnetcdf_file_rec->base_rec.rank, pnetcdf_file_rec->base_rec.id,
            pnetcdf_f_counter_names[i], pnetcdf_file_rec->fcounters[i],
            file_name, mnt_pt, fs_type);
    }

    return;
}

static void darshan_log_print_pnetcdf_description(int ver)
{
    printf("\n# description of PNETCDF counters:\n");
    printf("#   PNETCDF_INDEP_OPENS: PNETCDF independent file open operation counts.\n");
    printf("#   PNETCDF_COLL_OPENS: PNETCDF collective file open operation counts.\n");
    printf("#   PNETCDF_F_*_START_TIMESTAMP: timestamp of first PNETCDF file open/close.\n");
    printf("#   PNETCDF_F_*_END_TIMESTAMP: timestamp of last PNETCDF file open/close.\n");

    if(ver == 1)
    {
        printf("\n# WARNING: PNETCDF module log format version 1 does not support the following counters:\n");
        printf("# - PNETCDF_F_CLOSE_START_TIMESTAMP\n");
        printf("# - PNETCDF_F_OPEN_END_TIMESTAMP\n");
    }

    return;
}

static void darshan_log_print_pnetcdf_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2)
{
    struct darshan_pnetcdf_file *file1 = (struct darshan_pnetcdf_file *)file_rec1;
    struct darshan_pnetcdf_file *file2 = (struct darshan_pnetcdf_file *)file_rec2;
    int i;

    /* NOTE: we assume that both input records are the same module format version */

    for(i=0; i<PNETCDF_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_MOD],
                file1->base_rec.rank, file1->base_rec.id, pnetcdf_counter_names[i],
                file1->counters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_MOD],
                file2->base_rec.rank, file2->base_rec.id, pnetcdf_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
        else if(file1->counters[i] != file2->counters[i])
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_MOD],
                file1->base_rec.rank, file1->base_rec.id, pnetcdf_counter_names[i],
                file1->counters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_MOD],
                file2->base_rec.rank, file2->base_rec.id, pnetcdf_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
    }

    for(i=0; i<PNETCDF_F_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_MOD],
                file1->base_rec.rank, file1->base_rec.id, pnetcdf_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_MOD],
                file2->base_rec.rank, file2->base_rec.id, pnetcdf_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
        else if(file1->fcounters[i] != file2->fcounters[i])
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_MOD],
                file1->base_rec.rank, file1->base_rec.id, pnetcdf_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_MOD],
                file2->base_rec.rank, file2->base_rec.id, pnetcdf_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
    }

    return;
}

static void darshan_log_agg_pnetcdf_files(void *rec, void *agg_rec, int init_flag)
{
    struct darshan_pnetcdf_file *pnc_rec = (struct darshan_pnetcdf_file *)rec;
    struct darshan_pnetcdf_file *agg_pnc_rec = (struct darshan_pnetcdf_file *)agg_rec;
    int i;

    for(i = 0; i < PNETCDF_NUM_INDICES; i++)
    {
        switch(i)
        {
            case PNETCDF_INDEP_OPENS:
            case PNETCDF_COLL_OPENS:
                /* sum */
                agg_pnc_rec->counters[i] += pnc_rec->counters[i];
                break;
            default:
                agg_pnc_rec->counters[i] = -1;
                break;
        }
    }

    for(i = 0; i < PNETCDF_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case PNETCDF_F_OPEN_START_TIMESTAMP:
            case PNETCDF_F_CLOSE_START_TIMESTAMP:
                /* minimum non-zero */
                if((pnc_rec->fcounters[i] > 0)  &&
                    ((agg_pnc_rec->fcounters[i] == 0) ||
                    (pnc_rec->fcounters[i] < agg_pnc_rec->fcounters[i])))
                {
                    agg_pnc_rec->fcounters[i] = pnc_rec->fcounters[i];
                }
                break;
            case PNETCDF_F_OPEN_END_TIMESTAMP:
            case PNETCDF_F_CLOSE_END_TIMESTAMP:
                /* maximum */
                if(pnc_rec->fcounters[i] > agg_pnc_rec->fcounters[i])
                {
                    agg_pnc_rec->fcounters[i] = pnc_rec->fcounters[i];
                }
                break;
            default:
                agg_pnc_rec->fcounters[i] = -1;
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
