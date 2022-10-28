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

/* counter name strings for the PnetCDF module */
#define X(a) #a,
char *pnetcdf_file_counter_names[] = {
    PNETCDF_FILE_COUNTERS
};

char *pnetcdf_file_f_counter_names[] = {
    PNETCDF_FILE_F_COUNTERS
};

char *pnetcdf_var_counter_names[] = {
    PNETCDF_VAR_COUNTERS
};

char *pnetcdf_var_f_counter_names[] = {
    PNETCDF_VAR_F_COUNTERS
};
#undef X

#define DARSHAN_PNETCDF_FILE_SIZE_1 48
#define DARSHAN_PNETCDF_FILE_SIZE_2 64

static int darshan_log_get_pnetcdf_file(darshan_fd fd, void** pnetcdf_buf_p);
static int darshan_log_put_pnetcdf_file(darshan_fd fd, void* pnetcdf_buf);
static void darshan_log_print_pnetcdf_file(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type);
static void darshan_log_print_pnetcdf_file_description(int ver);
static void darshan_log_print_pnetcdf_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2);
static void darshan_log_agg_pnetcdf_files(void *rec, void *agg_rec, int init_flag);

static int darshan_log_get_pnetcdf_var(darshan_fd fd, void** pnetcdf_buf_p);
static int darshan_log_put_pnetcdf_var(darshan_fd fd, void* pnetcdf_buf);
static void darshan_log_print_pnetcdf_var(void *var_rec,
    char *var_name, char *mnt_pt, char *fs_type);
static void darshan_log_print_pnetcdf_var_description(int ver);
static void darshan_log_print_pnetcdf_var_diff(void *var_rec1, char *var_name1,
    void *var_rec2, char *var_name2);
static void darshan_log_agg_pnetcdf_vars(void *rec, void *agg_rec, int init_flag);

struct darshan_mod_logutil_funcs pnetcdf_file_logutils =
{
    .log_get_record = &darshan_log_get_pnetcdf_file,
    .log_put_record = &darshan_log_put_pnetcdf_file,
    .log_print_record = &darshan_log_print_pnetcdf_file,
    .log_print_description = &darshan_log_print_pnetcdf_file_description,
    .log_print_diff = &darshan_log_print_pnetcdf_file_diff,
    .log_agg_records = &darshan_log_agg_pnetcdf_files
};

struct darshan_mod_logutil_funcs pnetcdf_var_logutils =
{
    .log_get_record = &darshan_log_get_pnetcdf_var,
    .log_put_record = &darshan_log_put_pnetcdf_var,
    .log_print_record = &darshan_log_print_pnetcdf_var,
    .log_print_description = &darshan_log_print_pnetcdf_var_description,
    .log_print_diff = &darshan_log_print_pnetcdf_var_diff,
    .log_agg_records = &darshan_log_agg_pnetcdf_vars
};

static int darshan_log_get_pnetcdf_file(darshan_fd fd, void** pnetcdf_buf_p)
{
    struct darshan_pnetcdf_file *file = *((struct darshan_pnetcdf_file **)pnetcdf_buf_p);
    int rec_len;
    int i;
    int ret;

    if(fd->mod_map[DARSHAN_PNETCDF_FILE_MOD].len == 0)
        return(0);

    if(fd->mod_ver[DARSHAN_PNETCDF_FILE_MOD] == 0 ||
        fd->mod_ver[DARSHAN_PNETCDF_FILE_MOD] > DARSHAN_PNETCDF_FILE_VER)
    {
        fprintf(stderr, "Error: Invalid PNetCDF module version number (got %d)\n",
            fd->mod_ver[DARSHAN_PNETCDF_FILE_MOD]);
        return(-1);
    }

    if(*pnetcdf_buf_p == NULL)
    {
        file = malloc(sizeof(*file));
        if(!file)
            return(-1);
    }

    if(fd->mod_ver[DARSHAN_PNETCDF_FILE_MOD] == DARSHAN_PNETCDF_FILE_VER)
    {
        /* log format is in current version, so we don't need to do any
         * translation of counters while reading
         */
        rec_len = sizeof(struct darshan_pnetcdf_file);
        ret = darshan_log_get_mod(fd, DARSHAN_PNETCDF_FILE_MOD, file, rec_len);
    }
    else
    {
        char scratch[1024] = {0};
        char *src_p, *dest_p;
        int len;

        if(fd->mod_ver[DARSHAN_PNETCDF_FILE_MOD] == 1)
        {
            rec_len = DARSHAN_PNETCDF_FILE_SIZE_1;
            ret = darshan_log_get_mod(fd, DARSHAN_PNETCDF_FILE_MOD, scratch, rec_len);
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
        }
        if(fd->mod_ver[DARSHAN_PNETCDF_FILE_MOD] <= 2)
        {
            if(fd->mod_ver[DARSHAN_PNETCDF_FILE_MOD] == 2)
            {
                rec_len = DARSHAN_PNETCDF_FILE_SIZE_2;
                ret = darshan_log_get_mod(fd, DARSHAN_PNETCDF_FILE_MOD, scratch, rec_len);
                if(ret != rec_len)
                    goto exit;
            }

            /* upconvert version 2 to version 3 in-place */
            src_p = scratch + sizeof(struct darshan_base_record);
            dest_p = src_p + sizeof(int64_t);
            /* combine old INDEP_OPENS and COLL_OPENS counters,
             * and assign to index of new FILE_OPENS counter
             */
            *((int64_t *)dest_p) += *((int64_t *)src_p);
            /* move START_TIMESTAMPS up to proper location in new format */
            src_p = scratch + sizeof(struct darshan_base_record) +
                (2 * sizeof(int64_t));
            dest_p = scratch + sizeof(struct darshan_base_record) +
                (9 * sizeof(int64_t));
            len = 2 * sizeof(double);
            memmove(dest_p, src_p, len);
            /* move END_TIMESTAMPS up to proper location in new format */
            src_p = scratch + sizeof(struct darshan_base_record) +
                (2 * sizeof(int64_t)) + (2 * sizeof(double));
            dest_p = scratch + sizeof(struct darshan_base_record) +
                (9 * sizeof(int64_t)) + (3 * sizeof(double));
            len = 2 * sizeof(double);
            memmove(dest_p, src_p, len);
            /* set new FILE_CREATES counter to -1 */
            dest_p = scratch + sizeof(struct darshan_base_record);
            *(int64_t *)dest_p = -1;
            /* set new FILE_REDEFS .. FILE_WAIT_FAILURES all to -1 */
            dest_p += (2 * sizeof(int64_t));
            for(i = 0; i < 7; i++)
            {
                *((int64_t *)dest_p) = -1;
                dest_p += sizeof(int64_t);
            }
            /* set WAIT/META timers/timestamps to -1 */
            dest_p += (2 * sizeof(double));
            *(double *)dest_p = -1;
            dest_p += (3 * sizeof(double));
            *(double *)dest_p = -1;
            dest_p += sizeof(double);
            *(double *)dest_p = -1;
            dest_p += sizeof(double);
            *(double *)dest_p = -1;
        }

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
            for(i=0; i<PNETCDF_FILE_NUM_INDICES; i++)
            {
                /* skip counters we explicitly set to -1 since they don't
                 * need to be byte swapped
                 */
                if((fd->mod_ver[DARSHAN_PNETCDF_FILE_MOD] < 3) &&
                    ((i == PNETCDF_FILE_CREATES) || (i == PNETCDF_FILE_REDEFS) ||
                     (i == PNETCDF_FILE_INDEP_WAITS) || (i == PNETCDF_FILE_COLL_WAITS) ||
                     (i == PNETCDF_FILE_SYNCS) || (i == PNETCDF_FILE_BYTES_READ) ||
                     (i == PNETCDF_FILE_BYTES_WRITTEN) || (i == PNETCDF_FILE_WAIT_FAILURES)))
                    continue;
                DARSHAN_BSWAP64(&file->counters[i]);
            }
            for(i=0; i<PNETCDF_FILE_F_NUM_INDICES; i++)
            {
                /* skip counters we explicitly set to -1 since they don't
                 * need to be byte swapped
                 */
                if((fd->mod_ver[DARSHAN_PNETCDF_FILE_MOD] == 1) &&
                    ((i == PNETCDF_FILE_F_CLOSE_START_TIMESTAMP) ||
                     (i == PNETCDF_FILE_F_OPEN_END_TIMESTAMP)))
                    continue;
                if((fd->mod_ver[DARSHAN_PNETCDF_FILE_MOD] < 3) &&
                    ((i == PNETCDF_FILE_F_WAIT_START_TIMESTAMP) ||
                     (i == PNETCDF_FILE_F_WAIT_END_TIMESTAMP) ||
                     (i == PNETCDF_FILE_F_META_TIME) || (i == PNETCDF_FILE_F_WAIT_TIME)))
                    continue;
                DARSHAN_BSWAP64(&file->fcounters[i]);
            }
        }

        return(1);
    }
}

static int darshan_log_get_pnetcdf_var(darshan_fd fd, void** pnetcdf_buf_p)
{
    struct darshan_pnetcdf_var *var = *((struct darshan_pnetcdf_var **)pnetcdf_buf_p);
    int rec_len;
    int i;
    int ret;

    if(fd->mod_map[DARSHAN_PNETCDF_VAR_MOD].len == 0)
        return(0);

    if(fd->mod_ver[DARSHAN_PNETCDF_VAR_MOD] == 0 ||
        fd->mod_ver[DARSHAN_PNETCDF_VAR_MOD] > DARSHAN_PNETCDF_VAR_VER)
    {
        fprintf(stderr, "Error: Invalid PNETCDF_VAR module version number (got %d)\n",
            fd->mod_ver[DARSHAN_PNETCDF_VAR_MOD]);
        goto exit;
    }

    if(*pnetcdf_buf_p == NULL)
    {
        var = malloc(sizeof(*var));
        if(!var)
            goto exit;
    }

    if(fd->mod_ver[DARSHAN_PNETCDF_VAR_MOD] == DARSHAN_PNETCDF_VAR_VER)
    {
        /* log format is in current version, so we don't need to do any
         * translation of counters while reading
         */
        rec_len = sizeof(struct darshan_pnetcdf_var);
        ret = darshan_log_get_mod(fd, DARSHAN_PNETCDF_VAR_MOD, var, rec_len);
    }

exit:
    if(*pnetcdf_buf_p == NULL)
    {
        if(ret == rec_len)
            *pnetcdf_buf_p = var;
        else
            free(var);
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
            DARSHAN_BSWAP64(&(var->base_rec.id));
            DARSHAN_BSWAP64(&(var->base_rec.rank));
            DARSHAN_BSWAP64(&(var->file_rec_id));
            for(i=0; i<PNETCDF_VAR_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&var->counters[i]);
            for(i=0; i<PNETCDF_VAR_F_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&var->fcounters[i]);
        }

        return(1);
    }
}

static int darshan_log_put_pnetcdf_file(darshan_fd fd, void* pnetcdf_buf)
{
    struct darshan_pnetcdf_file *file = (struct darshan_pnetcdf_file *)pnetcdf_buf;
    int ret;

    ret = darshan_log_put_mod(fd, DARSHAN_PNETCDF_FILE_MOD, file,
        sizeof(struct darshan_pnetcdf_file), DARSHAN_PNETCDF_FILE_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

static int darshan_log_put_pnetcdf_var(darshan_fd fd, void* pnetcdf_buf)
{
    struct darshan_pnetcdf_var *var = (struct darshan_pnetcdf_var *)pnetcdf_buf;
    int ret;

    ret = darshan_log_put_mod(fd, DARSHAN_PNETCDF_VAR_MOD, var,
        sizeof(struct darshan_pnetcdf_var), DARSHAN_PNETCDF_VAR_VER);
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

    for(i=0; i<PNETCDF_FILE_NUM_INDICES; i++)
    {
        DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_FILE_MOD],
            pnetcdf_file_rec->base_rec.rank, pnetcdf_file_rec->base_rec.id,
            pnetcdf_file_counter_names[i], pnetcdf_file_rec->counters[i],
            file_name, mnt_pt, fs_type);
    }

    for(i=0; i<PNETCDF_FILE_F_NUM_INDICES; i++)
    {
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_FILE_MOD],
            pnetcdf_file_rec->base_rec.rank, pnetcdf_file_rec->base_rec.id,
            pnetcdf_file_f_counter_names[i], pnetcdf_file_rec->fcounters[i],
            file_name, mnt_pt, fs_type);
    }

    return;
}

static void darshan_log_print_pnetcdf_var(void *var_rec, char *var_name,
    char *mnt_pt, char *fs_type)
{
    int i;
    struct darshan_pnetcdf_var *pnetcdf_var_rec =
        (struct darshan_pnetcdf_var *)var_rec;

    for(i=0; i<PNETCDF_VAR_NUM_INDICES; i++)
    {
        DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_VAR_MOD],
            pnetcdf_var_rec->base_rec.rank, pnetcdf_var_rec->base_rec.id,
            pnetcdf_var_counter_names[i], pnetcdf_var_rec->counters[i],
            var_name, mnt_pt, fs_type);
    }

    for(i=0; i<PNETCDF_VAR_F_NUM_INDICES; i++)
    {
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_VAR_MOD],
            pnetcdf_var_rec->base_rec.rank, pnetcdf_var_rec->base_rec.id,
            pnetcdf_var_f_counter_names[i], pnetcdf_var_rec->fcounters[i],
            var_name, mnt_pt, fs_type);
    }

    DARSHAN_U_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_VAR_MOD],
        pnetcdf_var_rec->base_rec.rank, pnetcdf_var_rec->base_rec.id,
        "PNETCDF_VAR_FILE_REC_ID", pnetcdf_var_rec->file_rec_id,
        var_name, mnt_pt, fs_type);

    return;
}

static void darshan_log_print_pnetcdf_file_description(int ver)
{
    printf("\n# description of PnetCDF counters:\n");
    printf("#   PNETCDF_FILE_CREATES: PnetCDF file create operation counts.\n");
    printf("#   PNETCDF_FILE_OPENS: PnetCDF file open operation counts.\n");
    printf("#   PNETCDF_FILE_REDEFS: PnetCDF file re-define operation counts.\n");
    printf("#   PNETCDF_FILE_INDEP_WAITS: PnetCDF independent file wait operation counts.\n");
    printf("#   PNETCDF_FILE_COLL_WAITS: PnetCDF collective file wait operation counts.\n");
    printf("#   PNETCDF_FILE_SYNCS: PnetCDF file sync operation counts.\n");
    printf("#   PNETCDF_FILE_BYTES_READ: PnetCDF total bytes read for all file variables.\n");
    printf("#   PNETCDF_FILE_BYTES_WRITTEN: PnetCDF total bytes written for all file variables.\n");
    printf("#   PNETCDF_FILE_WAIT_FAILURES: PnetCDF file wait operation failure counts.\n");
    printf("#   PNETCDF_FILE_F_*_START_TIMESTAMP: timestamp of first PnetCDF file open/close/wait operation.\n");
    printf("#   PNETCDF_FILE_F_*_END_TIMESTAMP: timestamp of last PnetCDF file open/close/wait operation.\n");
    printf("#   PNETCDF_FILE_F_META_TIME: Cumulative time spent in file metadata operations.\n");
    printf("#   PNETCDF_FILE_F_WAIT_TIME: Cumulative time spent in file wait operations.\n");

    if(ver == 1)
    {
        printf("\n# WARNING: PnetCDF file module log format version 1 does not support the following counters:\n");
        printf("# - PNETCDF_FILE_F_CLOSE_START_TIMESTAMP\n");
        printf("# - PNETCDF_FILE_F_OPEN_END_TIMESTAMP\n");
    }
    if(ver <= 2)
    {
        printf("\n# WARNING: PnetCDF file module log format version <=2 does not support the following counters:\n");
        printf("# - PNETCDF_FILE_CREATES\n");
        printf("# - PNETCDF_FILE_REDEFS\n");
        printf("# - PNETCDF_FILE_INDEP_WAITS\n");
        printf("# - PNETCDF_FILE_COLL_WAITS\n");
        printf("# - PNETCDF_FILE_SYNCS\n");
        printf("# - PNETCDF_FILE_BYTES_READ\n");
        printf("# - PNETCDF_FILE_BYTES_WRITTEN\n");
        printf("# - PNETCDF_FILE_WAIT_FAILURES\n");
        printf("# - PNETCDF_FILE_F_WAIT_START_TIMESTAMP\n");
        printf("# - PNETCDF_FILE_F_WAIT_END_TIMESTAMP\n");
        printf("# - PNETCDF_FILE_F_META_TIME\n");
        printf("# - PNETCDF_FILE_F_WAIT_TIME\n");
    }

    return;
}

static void darshan_log_print_pnetcdf_var_description(int ver)
{
    printf("\n# description of PnetCDF counters:\n");
    printf("#   PNETCDF_VAR_OPENS: PnetCDF variable define/inquire operation counts.\n");
    printf("#   PNETCDF_VAR_INDEP_READS: PnetCDF variable independent read operation counts.\n");
    printf("#   PNETCDF_VAR_INDEP_WRITES: PnetCDF variable independent write operation counts.\n");
    printf("#   PNETCDF_VAR_COLL_READS: PnetCDF variable collective read operation counts.\n");
    printf("#   PNETCDF_VAR_COLL_WRITES: PnetCDF variable collective write operation counts.\n");
    printf("#   PNETCDF_VAR_NB_READS: PnetCDF variable nonblocking read operation counts.\n");
    printf("#   PNETCDF_VAR_NB_WRITES: PnetCDF variable nonblocking write operation counts.\n");
    printf("#   PNETCDF_VAR_BYTES_*: total bytes read and written at PnetCDF variable layer.\n");
    printf("#   PNETCDF_VAR_RW_SWITCHES: number of times access alternated between read and write.\n");
    printf("#   PNETCDF_VAR_PUT_VAR*: number of calls to ncmpi_put_var* APIs (var, var1, vara, vars, varm, varn, vard).\n");
    printf("#   PNETCDF_VAR_GET_VAR*: number of calls to ncmpi_get_var* APIs (var, var1, vara, vars, varm, varn, vard).\n");
    printf("#   PNETCDF_VAR_IPUT_VAR*: number of calls to ncmpi_iput_var* APIs (var, var1, vara, vars, varm, varn).\n");
    printf("#   PNETCDF_VAR_IGET_VAR*: number of calls to ncmpi_iget_var* APIs (var, var1, vara, vars, varm, varn).\n");
    printf("#   PNETCDF_VAR_BPUT_VAR*: number of calls to ncmpi_bput_var* APIs (var, var1, vara, vars, varm, varn).\n");
    printf("#   PNETCDF_VAR_MAX_*_TIME_SIZE: size of the slowest read and write operations.\n");
    printf("#   PNETCDF_VAR_SIZE_*_AGG_*: histogram of PnetCDf total access sizes for read and write operations.\n");
    printf("#   PNETCDF_VAR_ACCESS*_*: the four most common total accesses, in terms of size and length/stride (in last 5 dimensions).\n");
    printf("#   PNETCDF_VAR_ACCESS*_COUNT: count of the four most common total access sizes.\n");
    printf("#   PNETCDF_VAR_NDIMS: number of dimensions in the variable.\n");
    printf("#   PNETCDF_VAR_NPOINTS: number of points in the variable.\n");
    printf("#   PNETCDF_VAR_DATATYPE_SIZE: size of each variable element.\n");
    printf("#   PNETCDF_VAR_*_RANK: rank of the processes that were the fastest and slowest at I/O (for shared datasets).\n");
    printf("#   PNETCDF_VAR_*_RANK_BYTES: total bytes transferred at PnetCDF layer by the fastest and slowest ranks (for shared datasets).\n");
    printf("#   PNETCDF_VAR_F_*_START_TIMESTAMP: timestamp of first PnetCDF variable open/read/write/close.\n");
    printf("#   PNETCDF_VAR_F_*_END_TIMESTAMP: timestamp of last PnetCDF variable open/read/write/close.\n");
    printf("#   PNETCDF_VAR_F_READ/WRITE/META_TIME: cumulative time spent in PnetCDF read, write, or metadata operations.\n");
    printf("#   PNETCDF_VAR_F_MAX_*_TIME: duration of the slowest PnetCDF read and write operations.\n");
    printf("#   PNETCDF_VAR_F_*_RANK_TIME: fastest and slowest I/O time for a single rank (for shared datasets).\n");
    printf("#   PNETCDF_VAR_F_VARIANCE_RANK_*: variance of total I/O time and bytes moved for all ranks (for shared datasets).\n");
    printf("#   PNETCDF_VAR_FILE_REC_ID: Darshan file record ID of the file the variable belongs to.\n");

    return;
}

static void darshan_log_print_pnetcdf_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2)
{
    struct darshan_pnetcdf_file *file1 = (struct darshan_pnetcdf_file *)file_rec1;
    struct darshan_pnetcdf_file *file2 = (struct darshan_pnetcdf_file *)file_rec2;
    int i;

    /* NOTE: we assume that both input records are the same module format version */

    for(i=0; i<PNETCDF_FILE_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_FILE_MOD],
                file1->base_rec.rank, file1->base_rec.id, pnetcdf_file_counter_names[i],
                file1->counters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_FILE_MOD],
                file2->base_rec.rank, file2->base_rec.id, pnetcdf_file_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
        else if(file1->counters[i] != file2->counters[i])
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_FILE_MOD],
                file1->base_rec.rank, file1->base_rec.id, pnetcdf_file_counter_names[i],
                file1->counters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_FILE_MOD],
                file2->base_rec.rank, file2->base_rec.id, pnetcdf_file_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
    }

    for(i=0; i<PNETCDF_FILE_F_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_FILE_MOD],
                file1->base_rec.rank, file1->base_rec.id, pnetcdf_file_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_FILE_MOD],
                file2->base_rec.rank, file2->base_rec.id, pnetcdf_file_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
        else if(file1->fcounters[i] != file2->fcounters[i])
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_FILE_MOD],
                file1->base_rec.rank, file1->base_rec.id, pnetcdf_file_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_FILE_MOD],
                file2->base_rec.rank, file2->base_rec.id, pnetcdf_file_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
    }

    return;
}

static void darshan_log_print_pnetcdf_var_diff(void *var_rec1, char *var_name1,
    void *var_rec2, char *var_name2)
{
    struct darshan_pnetcdf_var *var1 = (struct darshan_pnetcdf_var *)var_rec1;
    struct darshan_pnetcdf_var *var2 = (struct darshan_pnetcdf_var *)var_rec2;
    int i;

    /* NOTE: we assume that both input recorvar are the same module format version */

    for(i=0; i<PNETCDF_VAR_NUM_INDICES; i++)
    {
        if(!var2)
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_VAR_MOD],
                var1->base_rec.rank, var1->base_rec.id, pnetcdf_var_counter_names[i],
                var1->counters[i], var_name1, "", "");

        }
        else if(!var1)
        {
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_VAR_MOD],
                var2->base_rec.rank, var2->base_rec.id, pnetcdf_var_counter_names[i],
                var2->counters[i], var_name2, "", "");
        }
        else if(var1->counters[i] != var2->counters[i])
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_VAR_MOD],
                var1->base_rec.rank, var1->base_rec.id, pnetcdf_var_counter_names[i],
                var1->counters[i], var_name1, "", "");
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_VAR_MOD],
                var2->base_rec.rank, var2->base_rec.id, pnetcdf_var_counter_names[i],
                var2->counters[i], var_name2, "", "");
        }
    }

    for(i=0; i<PNETCDF_VAR_F_NUM_INDICES; i++)
    {
        if(!var2)
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_VAR_MOD],
                var1->base_rec.rank, var1->base_rec.id, pnetcdf_var_f_counter_names[i],
                var1->fcounters[i], var_name1, "", "");

        }
        else if(!var1)
        {
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_VAR_MOD],
                var2->base_rec.rank, var2->base_rec.id, pnetcdf_var_f_counter_names[i],
                var2->fcounters[i], var_name2, "", "");
        }
        else if(var1->fcounters[i] != var2->fcounters[i])
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_VAR_MOD],
                var1->base_rec.rank, var1->base_rec.id, pnetcdf_var_f_counter_names[i],
                var1->fcounters[i], var_name1, "", "");
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_PNETCDF_VAR_MOD],
                var2->base_rec.rank, var2->base_rec.id, pnetcdf_var_f_counter_names[i],
                var2->fcounters[i], var_name2, "", "");
        }
    }

    return;
}

static void darshan_log_agg_pnetcdf_files(void *rec, void *agg_rec, int init_flag)
{
    struct darshan_pnetcdf_file *pnetcdf_rec = (struct darshan_pnetcdf_file *)rec;
    struct darshan_pnetcdf_file *agg_pnetcdf_rec = (struct darshan_pnetcdf_file *)agg_rec;
    int i;

    for(i = 0; i < PNETCDF_FILE_NUM_INDICES; i++)
    {
        switch(i)
        {
            case PNETCDF_FILE_CREATES:
            case PNETCDF_FILE_OPENS:
            case PNETCDF_FILE_REDEFS:
            case PNETCDF_FILE_INDEP_WAITS:
            case PNETCDF_FILE_COLL_WAITS:
            case PNETCDF_FILE_SYNCS:
            case PNETCDF_FILE_BYTES_READ:
            case PNETCDF_FILE_BYTES_WRITTEN:
            case PNETCDF_FILE_WAIT_FAILURES:
                /* sum */
                agg_pnetcdf_rec->counters[i] += pnetcdf_rec->counters[i];
                break;
            default:
                agg_pnetcdf_rec->counters[i] = -1;
                break;
        }
    }

    for(i = 0; i < PNETCDF_FILE_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case PNETCDF_FILE_F_OPEN_START_TIMESTAMP:
            case PNETCDF_FILE_F_CLOSE_START_TIMESTAMP:
            case PNETCDF_FILE_F_WAIT_START_TIMESTAMP:
                /* minimum non-zero */
                if((pnetcdf_rec->fcounters[i] > 0)  &&
                    ((agg_pnetcdf_rec->fcounters[i] == 0) ||
                    (pnetcdf_rec->fcounters[i] < agg_pnetcdf_rec->fcounters[i])))
                {
                    agg_pnetcdf_rec->fcounters[i] = pnetcdf_rec->fcounters[i];
                }
                break;
            case PNETCDF_FILE_F_OPEN_END_TIMESTAMP:
            case PNETCDF_FILE_F_CLOSE_END_TIMESTAMP:
            case PNETCDF_FILE_F_WAIT_END_TIMESTAMP:
                /* maximum */
                if(pnetcdf_rec->fcounters[i] > agg_pnetcdf_rec->fcounters[i])
                {
                    agg_pnetcdf_rec->fcounters[i] = pnetcdf_rec->fcounters[i];
                }
                break;
            case PNETCDF_FILE_F_META_TIME:
            case PNETCDF_FILE_F_WAIT_TIME:
                /* sum */
                agg_pnetcdf_rec->fcounters[i] += pnetcdf_rec->fcounters[i];
                break;
            default:
                agg_pnetcdf_rec->fcounters[i] = -1;
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

static void darshan_log_agg_pnetcdf_vars(void *rec, void *agg_rec, int init_flag)
{
    struct darshan_pnetcdf_var *pnetcdf_rec = (struct darshan_pnetcdf_var *)rec;
    struct darshan_pnetcdf_var *agg_pnetcdf_rec = (struct darshan_pnetcdf_var *)agg_rec;
    int i, j, j2, k, k2;
    int total_count;
    int64_t tmp_val[4 * (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1)];
    int64_t tmp_cnt[4];
    int tmp_ndx;
    double old_M;
    double pnetcdf_time = pnetcdf_rec->fcounters[PNETCDF_VAR_F_READ_TIME] +
        pnetcdf_rec->fcounters[PNETCDF_VAR_F_WRITE_TIME] +
        pnetcdf_rec->fcounters[PNETCDF_VAR_F_META_TIME];
    double pnetcdf_bytes = (double)pnetcdf_rec->counters[PNETCDF_VAR_BYTES_READ] +
        pnetcdf_rec->counters[PNETCDF_VAR_BYTES_WRITTEN];
    struct var_t *var_time_p = (struct var_t *)
        ((char *)rec + sizeof(struct darshan_pnetcdf_var));
    struct var_t *var_bytes_p = (struct var_t *)
        ((char *)var_time_p + sizeof(struct var_t));

    agg_pnetcdf_rec->file_rec_id = pnetcdf_rec->file_rec_id;
    for(i = 0; i < PNETCDF_VAR_NUM_INDICES; i++)
    {
        switch(i)
        {
            case PNETCDF_VAR_OPENS:
            case PNETCDF_VAR_COLL_READS:
            case PNETCDF_VAR_COLL_WRITES:
            case PNETCDF_VAR_INDEP_READS:
            case PNETCDF_VAR_INDEP_WRITES:
            case PNETCDF_VAR_BYTES_READ:
            case PNETCDF_VAR_BYTES_WRITTEN:
            case PNETCDF_VAR_RW_SWITCHES:
            case PNETCDF_VAR_PUT_VAR:
            case PNETCDF_VAR_PUT_VAR1:
            case PNETCDF_VAR_PUT_VARA:
            case PNETCDF_VAR_PUT_VARS:
            case PNETCDF_VAR_PUT_VARM:
            case PNETCDF_VAR_PUT_VARN:
            case PNETCDF_VAR_PUT_VARD:
            case PNETCDF_VAR_GET_VAR:
            case PNETCDF_VAR_GET_VAR1:
            case PNETCDF_VAR_GET_VARA:
            case PNETCDF_VAR_GET_VARS:
            case PNETCDF_VAR_GET_VARM:
            case PNETCDF_VAR_GET_VARN:
            case PNETCDF_VAR_GET_VARD:
            case PNETCDF_VAR_IPUT_VAR:
            case PNETCDF_VAR_IPUT_VAR1:
            case PNETCDF_VAR_IPUT_VARA:
            case PNETCDF_VAR_IPUT_VARS:
            case PNETCDF_VAR_IPUT_VARM:
            case PNETCDF_VAR_IPUT_VARN:
            case PNETCDF_VAR_IGET_VAR:
            case PNETCDF_VAR_IGET_VAR1:
            case PNETCDF_VAR_IGET_VARA:
            case PNETCDF_VAR_IGET_VARS:
            case PNETCDF_VAR_IGET_VARM:
            case PNETCDF_VAR_IGET_VARN:
            case PNETCDF_VAR_BPUT_VAR:
            case PNETCDF_VAR_BPUT_VAR1:
            case PNETCDF_VAR_BPUT_VARA:
            case PNETCDF_VAR_BPUT_VARS:
            case PNETCDF_VAR_BPUT_VARM:
            case PNETCDF_VAR_BPUT_VARN:
            case PNETCDF_VAR_SIZE_READ_AGG_0_100:
            case PNETCDF_VAR_SIZE_READ_AGG_100_1K:
            case PNETCDF_VAR_SIZE_READ_AGG_1K_10K:
            case PNETCDF_VAR_SIZE_READ_AGG_10K_100K:
            case PNETCDF_VAR_SIZE_READ_AGG_100K_1M:
            case PNETCDF_VAR_SIZE_READ_AGG_1M_4M:
            case PNETCDF_VAR_SIZE_READ_AGG_4M_10M:
            case PNETCDF_VAR_SIZE_READ_AGG_10M_100M:
            case PNETCDF_VAR_SIZE_READ_AGG_100M_1G:
            case PNETCDF_VAR_SIZE_READ_AGG_1G_PLUS:
            case PNETCDF_VAR_SIZE_WRITE_AGG_0_100:
            case PNETCDF_VAR_SIZE_WRITE_AGG_100_1K:
            case PNETCDF_VAR_SIZE_WRITE_AGG_1K_10K:
            case PNETCDF_VAR_SIZE_WRITE_AGG_10K_100K:
            case PNETCDF_VAR_SIZE_WRITE_AGG_100K_1M:
            case PNETCDF_VAR_SIZE_WRITE_AGG_1M_4M:
            case PNETCDF_VAR_SIZE_WRITE_AGG_4M_10M:
            case PNETCDF_VAR_SIZE_WRITE_AGG_10M_100M:
            case PNETCDF_VAR_SIZE_WRITE_AGG_100M_1G:
            case PNETCDF_VAR_SIZE_WRITE_AGG_1G_PLUS:
                /* sum */
                agg_pnetcdf_rec->counters[i] += pnetcdf_rec->counters[i];
                break;
            case PNETCDF_VAR_MAX_READ_TIME_SIZE:
            case PNETCDF_VAR_MAX_WRITE_TIME_SIZE:
            case PNETCDF_VAR_FASTEST_RANK:
            case PNETCDF_VAR_FASTEST_RANK_BYTES:
            case PNETCDF_VAR_SLOWEST_RANK:
            case PNETCDF_VAR_SLOWEST_RANK_BYTES:
                /* these are set with the FP counters */
                break;
            case PNETCDF_VAR_ACCESS1_ACCESS:
                /* increment common value counters */
                if(pnetcdf_rec->counters[i] == 0) break;

                /* first, collapse duplicates */
                for(j = i, j2 = PNETCDF_VAR_ACCESS1_COUNT; j <= PNETCDF_VAR_ACCESS4_ACCESS;
                    j += (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1), j2++)
                {
                    for(k = i, k2 = PNETCDF_VAR_ACCESS1_COUNT; k <= PNETCDF_VAR_ACCESS4_ACCESS;
                        k += (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1), k2++)
                    {
                        if(!memcmp(&pnetcdf_rec->counters[j], &agg_pnetcdf_rec->counters[k],
                            sizeof(int64_t) * (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1)))
                        {
                            memset(&pnetcdf_rec->counters[j], 0, sizeof(int64_t) *
                                (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1));
                            agg_pnetcdf_rec->counters[k2] += pnetcdf_rec->counters[j2];
                            pnetcdf_rec->counters[j2] = 0;
                        }
                    }
                }

                /* second, add new counters */
                for(j = i, j2 = PNETCDF_VAR_ACCESS1_COUNT; j <= PNETCDF_VAR_ACCESS4_ACCESS;
                    j += (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1), j2++)
                {
                    tmp_ndx = 0;
                    memset(tmp_val, 0, 4 * (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1) * sizeof(int64_t));
                    memset(tmp_cnt, 0, 4 * sizeof(int64_t));

                    if(pnetcdf_rec->counters[j] == 0) break;
                    for(k = i, k2 = PNETCDF_VAR_ACCESS1_COUNT; k <= PNETCDF_VAR_ACCESS4_ACCESS;
                        k += (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1), k2++)
                    {
                        if(!memcmp(&pnetcdf_rec->counters[j], &agg_pnetcdf_rec->counters[k],
                            sizeof(int64_t) * (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1)))
                        {
                            total_count = agg_pnetcdf_rec->counters[k2] +
                                pnetcdf_rec->counters[j2];
                            break;
                        }
                    }
                    if(k > PNETCDF_VAR_ACCESS4_ACCESS) total_count = pnetcdf_rec->counters[j2];

                    for(k = i, k2 = PNETCDF_VAR_ACCESS1_COUNT; k <= PNETCDF_VAR_ACCESS4_ACCESS;
                        k += (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1), k2++)
                    {
                        if((agg_pnetcdf_rec->counters[k2] > total_count) ||
                           ((agg_pnetcdf_rec->counters[k2] == total_count) &&
                            (agg_pnetcdf_rec->counters[k] > pnetcdf_rec->counters[j])))
                        {
                            memcpy(&tmp_val[tmp_ndx * (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1)],
                                &agg_pnetcdf_rec->counters[k],
                                sizeof(int64_t) * (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1));
                            tmp_cnt[tmp_ndx] = agg_pnetcdf_rec->counters[k2];
                            tmp_ndx++;
                        }
                        else break;
                    }
                    if(tmp_ndx == 4) break;

                    memcpy(&tmp_val[tmp_ndx * (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1)],
                        &pnetcdf_rec->counters[j],
                        sizeof(int64_t) * (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1));
                    tmp_cnt[tmp_ndx] = pnetcdf_rec->counters[j2];
                    tmp_ndx++;

                    while(tmp_ndx != 4)
                    {
                        if(memcmp(&agg_pnetcdf_rec->counters[k], &pnetcdf_rec->counters[j],
                            sizeof(int64_t) * (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1)))
                        {
                            memcpy(&tmp_val[tmp_ndx * (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1)],
                                &agg_pnetcdf_rec->counters[k],
                                sizeof(int64_t) * (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1));
                            tmp_cnt[tmp_ndx] = agg_pnetcdf_rec->counters[k2];
                            tmp_ndx++;
                        }
                        k += (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1);
                        k2++;
                    }
                    memcpy(&(agg_pnetcdf_rec->counters[PNETCDF_VAR_ACCESS1_ACCESS]), tmp_val,
                        4 * (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1) * sizeof(int64_t));
                    memcpy(&(agg_pnetcdf_rec->counters[PNETCDF_VAR_ACCESS1_COUNT]), tmp_cnt,
                        4 * sizeof(int64_t));
                }
                break;
            case PNETCDF_VAR_ACCESS1_LENGTH_D1:
            case PNETCDF_VAR_ACCESS1_LENGTH_D2:
            case PNETCDF_VAR_ACCESS1_LENGTH_D3:
            case PNETCDF_VAR_ACCESS1_LENGTH_D4:
            case PNETCDF_VAR_ACCESS1_LENGTH_D5:
            case PNETCDF_VAR_ACCESS1_STRIDE_D1:
            case PNETCDF_VAR_ACCESS1_STRIDE_D2:
            case PNETCDF_VAR_ACCESS1_STRIDE_D3:
            case PNETCDF_VAR_ACCESS1_STRIDE_D4:
            case PNETCDF_VAR_ACCESS1_STRIDE_D5:
            case PNETCDF_VAR_ACCESS2_ACCESS:
            case PNETCDF_VAR_ACCESS2_LENGTH_D1:
            case PNETCDF_VAR_ACCESS2_LENGTH_D2:
            case PNETCDF_VAR_ACCESS2_LENGTH_D3:
            case PNETCDF_VAR_ACCESS2_LENGTH_D4:
            case PNETCDF_VAR_ACCESS2_LENGTH_D5:
            case PNETCDF_VAR_ACCESS2_STRIDE_D1:
            case PNETCDF_VAR_ACCESS2_STRIDE_D2:
            case PNETCDF_VAR_ACCESS2_STRIDE_D3:
            case PNETCDF_VAR_ACCESS2_STRIDE_D4:
            case PNETCDF_VAR_ACCESS2_STRIDE_D5:
            case PNETCDF_VAR_ACCESS3_ACCESS:
            case PNETCDF_VAR_ACCESS3_LENGTH_D1:
            case PNETCDF_VAR_ACCESS3_LENGTH_D2:
            case PNETCDF_VAR_ACCESS3_LENGTH_D3:
            case PNETCDF_VAR_ACCESS3_LENGTH_D4:
            case PNETCDF_VAR_ACCESS3_LENGTH_D5:
            case PNETCDF_VAR_ACCESS3_STRIDE_D1:
            case PNETCDF_VAR_ACCESS3_STRIDE_D2:
            case PNETCDF_VAR_ACCESS3_STRIDE_D3:
            case PNETCDF_VAR_ACCESS3_STRIDE_D4:
            case PNETCDF_VAR_ACCESS3_STRIDE_D5:
            case PNETCDF_VAR_ACCESS4_ACCESS:
            case PNETCDF_VAR_ACCESS4_LENGTH_D1:
            case PNETCDF_VAR_ACCESS4_LENGTH_D2:
            case PNETCDF_VAR_ACCESS4_LENGTH_D3:
            case PNETCDF_VAR_ACCESS4_LENGTH_D4:
            case PNETCDF_VAR_ACCESS4_LENGTH_D5:
            case PNETCDF_VAR_ACCESS4_STRIDE_D1:
            case PNETCDF_VAR_ACCESS4_STRIDE_D2:
            case PNETCDF_VAR_ACCESS4_STRIDE_D3:
            case PNETCDF_VAR_ACCESS4_STRIDE_D4:
            case PNETCDF_VAR_ACCESS4_STRIDE_D5:
            case PNETCDF_VAR_ACCESS1_COUNT:
            case PNETCDF_VAR_ACCESS2_COUNT:
            case PNETCDF_VAR_ACCESS3_COUNT:
            case PNETCDF_VAR_ACCESS4_COUNT:
                /* these are set all at once with common counters above */
                break;
            case PNETCDF_VAR_NDIMS:
            case PNETCDF_VAR_NPOINTS:
            case PNETCDF_VAR_DATATYPE_SIZE:
                /* just set to the input value */
                agg_pnetcdf_rec->counters[i] = pnetcdf_rec->counters[i];
                break;
            case PNETCDF_VAR_IS_RECORD_VAR:
                if(pnetcdf_rec->counters[i] > 0)
                    agg_pnetcdf_rec->counters[i] = 1;
                break;
            default:
                agg_pnetcdf_rec->counters[i] = -1;
                break;
        }
    }

    for(i = 0; i < PNETCDF_VAR_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case PNETCDF_VAR_F_READ_TIME:
            case PNETCDF_VAR_F_WRITE_TIME:
            case PNETCDF_VAR_F_META_TIME:
                /* sum */
                agg_pnetcdf_rec->fcounters[i] += pnetcdf_rec->fcounters[i];
                break;
            case PNETCDF_VAR_F_OPEN_START_TIMESTAMP:
            case PNETCDF_VAR_F_READ_START_TIMESTAMP:
            case PNETCDF_VAR_F_WRITE_START_TIMESTAMP:
            case PNETCDF_VAR_F_CLOSE_START_TIMESTAMP:
                /* minimum non-zero */
                if((pnetcdf_rec->fcounters[i] > 0)  &&
                    ((agg_pnetcdf_rec->fcounters[i] == 0) ||
                    (pnetcdf_rec->fcounters[i] < agg_pnetcdf_rec->fcounters[i])))
                {
                    agg_pnetcdf_rec->fcounters[i] = pnetcdf_rec->fcounters[i];
                }
                break;
            case PNETCDF_VAR_F_OPEN_END_TIMESTAMP:
            case PNETCDF_VAR_F_READ_END_TIMESTAMP:
            case PNETCDF_VAR_F_WRITE_END_TIMESTAMP:
            case PNETCDF_VAR_F_CLOSE_END_TIMESTAMP:
                /* maximum */
                if(pnetcdf_rec->fcounters[i] > agg_pnetcdf_rec->fcounters[i])
                {
                    agg_pnetcdf_rec->fcounters[i] = pnetcdf_rec->fcounters[i];
                }
                break;
            case PNETCDF_VAR_F_MAX_READ_TIME:
                if(pnetcdf_rec->fcounters[i] > agg_pnetcdf_rec->fcounters[i])
                {
                    agg_pnetcdf_rec->fcounters[i] = pnetcdf_rec->fcounters[i];
                    agg_pnetcdf_rec->counters[PNETCDF_VAR_MAX_READ_TIME_SIZE] =
                        pnetcdf_rec->counters[PNETCDF_VAR_MAX_READ_TIME_SIZE];
                }
                break;
            case PNETCDF_VAR_F_MAX_WRITE_TIME:
                if(pnetcdf_rec->fcounters[i] > agg_pnetcdf_rec->fcounters[i])
                {
                    agg_pnetcdf_rec->fcounters[i] = pnetcdf_rec->fcounters[i];
                    agg_pnetcdf_rec->counters[PNETCDF_VAR_MAX_WRITE_TIME_SIZE] =
                        pnetcdf_rec->counters[PNETCDF_VAR_MAX_WRITE_TIME_SIZE];
                }
                break;
            case PNETCDF_VAR_F_FASTEST_RANK_TIME:
                if(init_flag)
                {
                    /* set fastest rank counters according to root rank. these counters
                     * will be determined as the aggregation progresses.
                     */
                    agg_pnetcdf_rec->counters[PNETCDF_VAR_FASTEST_RANK] = pnetcdf_rec->base_rec.rank;
                    agg_pnetcdf_rec->counters[PNETCDF_VAR_FASTEST_RANK_BYTES] = pnetcdf_bytes;
                    agg_pnetcdf_rec->fcounters[PNETCDF_VAR_F_FASTEST_RANK_TIME] = pnetcdf_time;
                }

                if(pnetcdf_time < agg_pnetcdf_rec->fcounters[PNETCDF_VAR_F_FASTEST_RANK_TIME])
                {
                    agg_pnetcdf_rec->counters[PNETCDF_VAR_FASTEST_RANK] = pnetcdf_rec->base_rec.rank;
                    agg_pnetcdf_rec->counters[PNETCDF_VAR_FASTEST_RANK_BYTES] = pnetcdf_bytes;
                    agg_pnetcdf_rec->fcounters[PNETCDF_VAR_F_FASTEST_RANK_TIME] = pnetcdf_time;
                }
                break;
            case PNETCDF_VAR_F_SLOWEST_RANK_TIME:
                if(init_flag)
                {
                    /* set slowest rank counters according to root rank. these counters
                     * will be determined as the aggregation progresses.
                     */
                    agg_pnetcdf_rec->counters[PNETCDF_VAR_SLOWEST_RANK] = pnetcdf_rec->base_rec.rank;
                    agg_pnetcdf_rec->counters[PNETCDF_VAR_SLOWEST_RANK_BYTES] = pnetcdf_bytes;
                    agg_pnetcdf_rec->fcounters[PNETCDF_VAR_F_SLOWEST_RANK_TIME] = pnetcdf_time;
                }

                if(pnetcdf_time > agg_pnetcdf_rec->fcounters[PNETCDF_VAR_F_SLOWEST_RANK_TIME])
                {
                    agg_pnetcdf_rec->counters[PNETCDF_VAR_SLOWEST_RANK] = pnetcdf_rec->base_rec.rank;
                    agg_pnetcdf_rec->counters[PNETCDF_VAR_SLOWEST_RANK_BYTES] = pnetcdf_bytes;
                    agg_pnetcdf_rec->fcounters[PNETCDF_VAR_F_SLOWEST_RANK_TIME] = pnetcdf_time;
                }
                break;
            case PNETCDF_VAR_F_VARIANCE_RANK_TIME:
                if(init_flag)
                {
                    var_time_p->n = 1;
                    var_time_p->M = pnetcdf_time;
                    var_time_p->S = 0;
                }
                else
                {
                    old_M = var_time_p->M;

                    var_time_p->n++;
                    var_time_p->M += (pnetcdf_time - var_time_p->M) / var_time_p->n;
                    var_time_p->S += (pnetcdf_time - var_time_p->M) * (pnetcdf_time - old_M);

                    agg_pnetcdf_rec->fcounters[PNETCDF_VAR_F_VARIANCE_RANK_TIME] =
                        var_time_p->S / var_time_p->n;
                }
                break;
            case PNETCDF_VAR_F_VARIANCE_RANK_BYTES:
                if(init_flag)
                {
                    var_bytes_p->n = 1;
                    var_bytes_p->M = pnetcdf_bytes;
                    var_bytes_p->S = 0;
                }
                else
                {
                    old_M = var_bytes_p->M;

                    var_bytes_p->n++;
                    var_bytes_p->M += (pnetcdf_bytes - var_bytes_p->M) / var_bytes_p->n;
                    var_bytes_p->S += (pnetcdf_bytes - var_bytes_p->M) * (pnetcdf_bytes - old_M);

                    agg_pnetcdf_rec->fcounters[PNETCDF_VAR_F_VARIANCE_RANK_BYTES] =
                        var_bytes_p->S / var_bytes_p->n;
                }
                break;
            default:
                agg_pnetcdf_rec->fcounters[i] = -1;
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
