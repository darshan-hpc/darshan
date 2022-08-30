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

#define max(a,b) (((a) > (b)) ? (a) : (b))

/* counter name strings for the POSIX module */
#define X(a) #a,
char *posix_counter_names[] = {
    POSIX_COUNTERS
};

char *posix_f_counter_names[] = {
    POSIX_F_COUNTERS
};
#undef X

#define DARSHAN_POSIX_FILE_SIZE_1 680
#define DARSHAN_POSIX_FILE_SIZE_2 648
#define DARSHAN_POSIX_FILE_SIZE_3 664

static int darshan_log_get_posix_file(darshan_fd fd, void** posix_buf_p);
static int darshan_log_put_posix_file(darshan_fd fd, void* posix_buf);
static void darshan_log_print_posix_file(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type);
static void darshan_log_print_posix_description(int ver);
static void darshan_log_print_posix_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2);
static void darshan_log_agg_posix_files(void *rec, void *agg_rec, int init_flag);
static int darshan_log_sizeof_posix_file(void* posix_buf_p);
static int darshan_log_record_metrics_posix_file(void*    posix_buf_p,
                                                 uint64_t* rec_id,
                                                 int64_t* r_bytes,
                                                 int64_t* w_bytes,
                                                 int64_t* max_offset,
                                                 double* io_total_time,
                                                 double* md_only_time,
                                                 double* rw_only_time,
                                                 int64_t* rank,
                                                 int64_t* nprocs);

struct darshan_mod_logutil_funcs posix_logutils =
{
    .log_get_record = &darshan_log_get_posix_file,
    .log_put_record = &darshan_log_put_posix_file,
    .log_print_record = &darshan_log_print_posix_file,
    .log_print_description = &darshan_log_print_posix_description,
    .log_print_diff = &darshan_log_print_posix_file_diff,
    .log_agg_records = &darshan_log_agg_posix_files,
    .log_sizeof_record = &darshan_log_sizeof_posix_file,
    .log_record_metrics = &darshan_log_record_metrics_posix_file
};

static int darshan_log_sizeof_posix_file(void* posix_buf_p)
{
    /* posix records have a fixed size */
    return(sizeof(struct darshan_posix_file));
}

static int darshan_log_record_metrics_posix_file(void*    posix_buf_p,
                                         uint64_t* rec_id,
                                         int64_t* r_bytes,
                                         int64_t* w_bytes,
                                         int64_t* max_offset,
                                         double* io_total_time,
                                         double* md_only_time,
                                         double* rw_only_time,
                                         int64_t* rank,
                                         int64_t* nprocs)
{
    struct darshan_posix_file *psx_rec = (struct darshan_posix_file *)posix_buf_p;

    *rec_id = psx_rec->base_rec.id;
    *r_bytes = psx_rec->counters[POSIX_BYTES_READ];
    *w_bytes = psx_rec->counters[POSIX_BYTES_WRITTEN];

    *max_offset = max(psx_rec->counters[POSIX_MAX_BYTE_READ], psx_rec->counters[POSIX_MAX_BYTE_WRITTEN]);

    *rank = psx_rec->base_rec.rank;
    /* nprocs is 1 per record, unless rank is negative, in which case we
     * report -1 as the rank value to represent "all"
     */
    if(psx_rec->base_rec.rank < 0)
        *nprocs = -1;
    else
        *nprocs = 1;

    if(psx_rec->base_rec.rank < 0) {
        /* shared file records populate a counter with the slowest rank time
         * (derived during reduction).  They do not have a breakdown of meta
         * and rw time, though.
         */
        *io_total_time = psx_rec->fcounters[POSIX_F_SLOWEST_RANK_TIME];
        *md_only_time = 0;
        *rw_only_time = 0;
    }
    else {
        /* non-shared records have separate meta, read, and write values
         * that we can combine as needed
         */
        *io_total_time = psx_rec->fcounters[POSIX_F_META_TIME] +
                         psx_rec->fcounters[POSIX_F_READ_TIME] +
                         psx_rec->fcounters[POSIX_F_WRITE_TIME];
        *md_only_time = psx_rec->fcounters[POSIX_F_META_TIME];
        *rw_only_time = psx_rec->fcounters[POSIX_F_READ_TIME] +
                        psx_rec->fcounters[POSIX_F_WRITE_TIME];
    }

    return(0);
}

static int darshan_log_get_posix_file(darshan_fd fd, void** posix_buf_p)
{
    struct darshan_posix_file *file = *((struct darshan_posix_file **)posix_buf_p);
    int rec_len;
    int i;
    int ret = -1;

    if(fd->mod_map[DARSHAN_POSIX_MOD].len == 0)
        return(0);

    if(fd->mod_ver[DARSHAN_POSIX_MOD] == 0 ||
        fd->mod_ver[DARSHAN_POSIX_MOD] > DARSHAN_POSIX_VER)
    {
        fprintf(stderr, "Error: Invalid POSIX module version number (got %d)\n",
            fd->mod_ver[DARSHAN_POSIX_MOD]);
        return(-1);
    }

    if(*posix_buf_p == NULL)
    {
        file = malloc(sizeof(*file));
        if(!file)
            return(-1);
    }

    if(fd->mod_ver[DARSHAN_POSIX_MOD] == DARSHAN_POSIX_VER) 
    {
        /* log format is in current version, so we don't need to do any
         * translation of counters while reading
         */
        rec_len = sizeof(struct darshan_posix_file);
        ret = darshan_log_get_mod(fd, DARSHAN_POSIX_MOD, file, rec_len);
    }
    else
    {
        char scratch[1024] = {0};
        char *src_p, *dest_p;
        int len;

        if(fd->mod_ver[DARSHAN_POSIX_MOD] == 1)
        {
            rec_len = DARSHAN_POSIX_FILE_SIZE_1;
            int64_t *fopen_counter;
            
            /* This version of the posix module had some stdio counters
             * mixed in with the posix counters.  If the fopen counters are
             * 0, then we can simply update the record format to skip those
             * counters.  If the fopen counters are non-zero, then we omit
             * the entire record because there is no clean way to properly
             * up-convert it.
             */
            dest_p = scratch + (sizeof(struct darshan_base_record) + 
                (6 * sizeof(int64_t)));
            fopen_counter = (int64_t*)dest_p;
            do
            {
                /* pull POSIX records until we find one that doesn't have STDIO data */
                ret = darshan_log_get_mod(fd, DARSHAN_POSIX_MOD, scratch, rec_len);
            } while(ret == rec_len && *fopen_counter > 0);
            if(ret != rec_len)
                goto exit;

            /* upconvert version 1 to version 2 in-place */
            src_p = dest_p + (4 * sizeof(int64_t));
            len = rec_len - (src_p - scratch);
            memmove(dest_p, src_p, len);
        }
        if(fd->mod_ver[DARSHAN_POSIX_MOD] <= 2)
        {
            if(fd->mod_ver[DARSHAN_POSIX_MOD] == 2)
            {
                rec_len = DARSHAN_POSIX_FILE_SIZE_2;
                ret = darshan_log_get_mod(fd, DARSHAN_POSIX_MOD, scratch, rec_len);
                if(ret != rec_len)
                    goto exit;
            }

            /* upconvert version 2 to version 3 in-place */
            dest_p = scratch + (sizeof(struct darshan_base_record) +
                (64 * sizeof(int64_t)) + (5 * sizeof(double)));
            src_p = dest_p - (2 * sizeof(double));
            len = (12 * sizeof(double));
            memmove(dest_p, src_p, len);
            /* set F_CLOSE_START and F_OPEN_END to -1 */
            *((double *)src_p) = -1;
            *((double *)(src_p + sizeof(double))) = -1;
        }
        if(fd->mod_ver[DARSHAN_POSIX_MOD] <=3)
        {
            if(fd->mod_ver[DARSHAN_POSIX_MOD] == 3)
            {
                rec_len = DARSHAN_POSIX_FILE_SIZE_3;
                ret = darshan_log_get_mod(fd, DARSHAN_POSIX_MOD, scratch, rec_len);
                if(ret != rec_len)
                    goto exit;
            }

            /* upconvert version 3 to version 4 in-place */
            dest_p = scratch + sizeof(struct darshan_base_record) +
                (3 * sizeof(int64_t));
            src_p = dest_p - (2 * sizeof(int64_t));
            len = rec_len - (src_p - scratch);
            memmove(dest_p, src_p, len);
            /* set FILENOS and DUPS to -1 */
            *((int64_t *)src_p) = -1;
            *((int64_t *)(src_p + sizeof(int64_t))) = -1;

            dest_p = scratch + sizeof(struct darshan_base_record) +
                (13 * sizeof(int64_t));
            src_p = dest_p - (3 * sizeof(int64_t));
            len = rec_len - (src_p - scratch);
            memmove(dest_p, src_p, len);
            /* set RENAME_SOURCES and RENAME_TARGETS to -1 */
            *((int64_t *)src_p) = -1;
            *((int64_t *)(src_p + sizeof(int64_t))) = -1;
            /* set RENAMED_FROM to 0 (-1 not possible since this is a uint) */
            *((int64_t *)(src_p + (2 * sizeof(int64_t)))) = 0;
        }
        
        memcpy(file, scratch, sizeof(struct darshan_posix_file));
    }

exit:
    if(*posix_buf_p == NULL)
    {
        if(ret == rec_len)
            *posix_buf_p = file;
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
            DARSHAN_BSWAP64(&file->base_rec.id);
            DARSHAN_BSWAP64(&file->base_rec.rank);
            for(i=0; i<POSIX_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->counters[i]);
            for(i=0; i<POSIX_F_NUM_INDICES; i++)
            {
                /* skip counters we explicitly set since they don't
                 * need to be byte swapped
                 */
                if((fd->mod_ver[DARSHAN_POSIX_MOD] < 3) &&
                    ((i == POSIX_F_CLOSE_START_TIMESTAMP) ||
                     (i == POSIX_F_OPEN_END_TIMESTAMP)))
                    continue;
                if((fd->mod_ver[DARSHAN_POSIX_MOD] < 4) &&
                     ((i == POSIX_RENAME_SOURCES) || (i == POSIX_RENAME_TARGETS) ||
                      (i == POSIX_RENAMED_FROM)))
                    continue;
                DARSHAN_BSWAP64(&file->fcounters[i]);
            }
        }

        return(1);
    }
}

static int darshan_log_put_posix_file(darshan_fd fd, void* posix_buf)
{
    struct darshan_posix_file *file = (struct darshan_posix_file *)posix_buf;
    int ret;

    ret = darshan_log_put_mod(fd, DARSHAN_POSIX_MOD, file,
        sizeof(struct darshan_posix_file), DARSHAN_POSIX_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

static void darshan_log_print_posix_file(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type)
{
    int i;
    struct darshan_posix_file *posix_file_rec =
        (struct darshan_posix_file *)file_rec;

    for(i=0; i<POSIX_NUM_INDICES; i++)
    {
        if(i == POSIX_RENAMED_FROM)
            DARSHAN_U_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                posix_file_rec->base_rec.rank, posix_file_rec->base_rec.id,
                posix_counter_names[i], posix_file_rec->counters[i],
                file_name, mnt_pt, fs_type);
        else
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                posix_file_rec->base_rec.rank, posix_file_rec->base_rec.id,
                posix_counter_names[i], posix_file_rec->counters[i],
                file_name, mnt_pt, fs_type);
    }

    for(i=0; i<POSIX_F_NUM_INDICES; i++)
    {
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
            posix_file_rec->base_rec.rank, posix_file_rec->base_rec.id,
            posix_f_counter_names[i], posix_file_rec->fcounters[i],
            file_name, mnt_pt, fs_type);
    }

    return;
}

static void darshan_log_print_posix_description(int ver)
{
    printf("\n# description of POSIX counters:\n");
    printf("#   POSIX_*: posix operation counts.\n");
    printf("#   READS,WRITES,OPENS,SEEKS,STATS,MMAPS,SYNCS,FILENOS,DUPS are types of operations.\n");
    printf("#   POSIX_RENAME_SOURCES/TARGETS: total count file was source or target of a rename operation\n");
    printf("#   POSIX_RENAMED_FROM: Darshan record ID of the first rename source, if file was a rename target\n");
    printf("#   POSIX_MODE: mode that file was opened in.\n");
    printf("#   POSIX_BYTES_*: total bytes read and written.\n");
    printf("#   POSIX_MAX_BYTE_*: highest offset byte read and written.\n");
    printf("#   POSIX_CONSEC_*: number of exactly adjacent reads and writes.\n");
    printf("#   POSIX_SEQ_*: number of reads and writes from increasing offsets.\n");
    printf("#   POSIX_RW_SWITCHES: number of times access alternated between read and write.\n");
    printf("#   POSIX_*_ALIGNMENT: memory and file alignment.\n");
    printf("#   POSIX_*_NOT_ALIGNED: number of reads and writes that were not aligned.\n");
    printf("#   POSIX_MAX_*_TIME_SIZE: size of the slowest read and write operations.\n");
    printf("#   POSIX_SIZE_*_*: histogram of read and write access sizes.\n");
    printf("#   POSIX_STRIDE*_STRIDE: the four most common strides detected.\n");
    printf("#   POSIX_STRIDE*_COUNT: count of the four most common strides.\n");
    printf("#   POSIX_ACCESS*_ACCESS: the four most common access sizes.\n");
    printf("#   POSIX_ACCESS*_COUNT: count of the four most common access sizes.\n");
    printf("#   POSIX_*_RANK: rank of the processes that were the fastest and slowest at I/O (for shared files).\n");
    printf("#   POSIX_*_RANK_BYTES: bytes transferred by the fastest and slowest ranks (for shared files).\n");
    printf("#   POSIX_F_*_START_TIMESTAMP: timestamp of first open/read/write/close.\n");
    printf("#   POSIX_F_*_END_TIMESTAMP: timestamp of last open/read/write/close.\n");
    printf("#   POSIX_F_READ/WRITE/META_TIME: cumulative time spent in read, write, or metadata operations.\n");
    printf("#   POSIX_F_MAX_*_TIME: duration of the slowest read and write operations.\n");
    printf("#   POSIX_F_*_RANK_TIME: fastest and slowest I/O time for a single rank (for shared files).\n");
    printf("#   POSIX_F_VARIANCE_RANK_*: variance of total I/O time and bytes moved for all ranks (for shared files).\n");

    if(ver == 1)
    {
        printf("\n# WARNING: POSIX module log format version 1 has the following limitations:\n");
        printf("# - Darshan version 3.1.0 and earlier had only partial instrumentation of stdio stream I/O functions.\n");
        printf("#   File records with stdio counters present will be omitted from output.\n");
        printf("#   Use darshan-logutils < 3.1.0 to retrieve those records.\n");
    }
    if(ver <= 2)
    {
        printf("\n# WARNING: POSIX module log format version <=2 has the following limitations:\n");
        printf("# - No support for the following timers:\n");
        printf("# \t- POSIX_F_CLOSE_START_TIMESTAMP\n");
        printf("# \t- POSIX_F_OPEN_END_TIMESTAMP\n");
    }
    if(ver <=3)
    {
        printf("\n# WARNING: POSIX module log format version <=3 has the following limitations:\n");
        printf("# - No support for the following counters to properly instrument dup, fileno, and rename operations:\n");
        printf("# \t- POSIX_FILENOS\n");
        printf("# \t- POSIX_DUPS\n");
        printf("# \t- POSIX_RENAME_SOURCES\n");
        printf("# \t- POSIX_RENAME_TARGETS\n");
        printf("# \t- POSIX_RENAMED_FROM\n");
    }

    if(ver >= 4)
    {
        printf("\n# WARNING: POSIX_OPENS counter includes both POSIX_FILENOS and POSIX_DUPS counts\n");
        printf("\n# WARNING: POSIX counters related to file offsets may be incorrect if a file is simultaneously accessed by both POSIX and STDIO (e.g., using fileno())\n");
        printf("# \t- Affected counters include: MAX_BYTE_{READ|WRITTEN}, CONSEC_{READS|WRITES}, SEQ_{READS|WRITES}, {MEM|FILE}_NOT_ALIGNED, STRIDE*_STRIDE\n");
    }

    return;
}

static void darshan_log_print_posix_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2)
{
    struct darshan_posix_file *file1 = (struct darshan_posix_file *)file_rec1;
    struct darshan_posix_file *file2 = (struct darshan_posix_file *)file_rec2;
    int i;

    /* NOTE: we assume that both input records are the same module format version */

    for(i=0; i<POSIX_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            if(i == POSIX_RENAMED_FROM)
                DARSHAN_U_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                    file1->base_rec.rank, file1->base_rec.id, posix_counter_names[i],
                    file1->counters[i], file_name1, "", "");
            else
                DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                    file1->base_rec.rank, file1->base_rec.id, posix_counter_names[i],
                    file1->counters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            if(i == POSIX_RENAMED_FROM)
                DARSHAN_U_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                    file2->base_rec.rank, file2->base_rec.id, posix_counter_names[i],
                    file2->counters[i], file_name2, "", "");
            else
                DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                    file2->base_rec.rank, file2->base_rec.id, posix_counter_names[i],
                    file2->counters[i], file_name2, "", "");
        }
        else if(file1->counters[i] != file2->counters[i])
        {
            printf("- ");
            if(i == POSIX_RENAMED_FROM)
                DARSHAN_U_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                    file1->base_rec.rank, file1->base_rec.id, posix_counter_names[i],
                    file1->counters[i], file_name1, "", "");
            else
                DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                    file1->base_rec.rank, file1->base_rec.id, posix_counter_names[i],
                    file1->counters[i], file_name1, "", "");
            printf("+ ");
            if(i == POSIX_RENAMED_FROM)
                DARSHAN_U_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                    file2->base_rec.rank, file2->base_rec.id, posix_counter_names[i],
                    file2->counters[i], file_name2, "", "");
            else
                DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                    file2->base_rec.rank, file2->base_rec.id, posix_counter_names[i],
                    file2->counters[i], file_name2, "", "");
        }
    }

    for(i=0; i<POSIX_F_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                file1->base_rec.rank, file1->base_rec.id, posix_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                file2->base_rec.rank, file2->base_rec.id, posix_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
        else if(file1->fcounters[i] != file2->fcounters[i])
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                file1->base_rec.rank, file1->base_rec.id, posix_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                file2->base_rec.rank, file2->base_rec.id, posix_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
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

static void darshan_log_agg_posix_files(void *rec, void *agg_rec, int init_flag)
{
    struct darshan_posix_file *psx_rec = (struct darshan_posix_file *)rec;
    struct darshan_posix_file *agg_psx_rec = (struct darshan_posix_file *)agg_rec;
    int i, j, k;
    int total_count;
    int64_t tmp_val[4];
    int64_t tmp_cnt[4];
    int duplicate_mask[4] = {0};
    int tmp_ndx;
    int64_t psx_fastest_rank, psx_slowest_rank,
        psx_fastest_bytes, psx_slowest_bytes;
    double psx_fastest_time, psx_slowest_time;
    int shared_file_flag = 0;

    /* For the incoming record, we need to determine what values to use for
     * subsequent comparision against the aggregate record's fastest and
     * slowest fields. This is is complicated by the fact that shared file
     * records already have derived values, while unique file records do
     * not.  Handle both cases here so that this function can be generic.
     */
    if(psx_rec->base_rec.rank == -1)
    {
        /* shared files should have pre-calculated fastest and slowest
         * counters */
        psx_fastest_rank = psx_rec->counters[POSIX_FASTEST_RANK];
        psx_slowest_rank = psx_rec->counters[POSIX_SLOWEST_RANK];
        psx_fastest_bytes = psx_rec->counters[POSIX_FASTEST_RANK_BYTES];
        psx_slowest_bytes = psx_rec->counters[POSIX_SLOWEST_RANK_BYTES];
        psx_fastest_time = psx_rec->fcounters[POSIX_F_FASTEST_RANK_TIME];
        psx_slowest_time = psx_rec->fcounters[POSIX_F_SLOWEST_RANK_TIME];
    }
    else
    {
        /* for non-shared files, derive bytes and time using data from this
         * rank
         */
        psx_fastest_rank = psx_rec->base_rec.rank;
        psx_slowest_rank = psx_fastest_rank;
        psx_fastest_bytes = psx_rec->counters[POSIX_BYTES_READ] +
            psx_rec->counters[POSIX_BYTES_WRITTEN];
        psx_slowest_bytes = psx_fastest_bytes;
        psx_fastest_time = psx_rec->fcounters[POSIX_F_READ_TIME] +
            psx_rec->fcounters[POSIX_F_WRITE_TIME] +
            psx_rec->fcounters[POSIX_F_META_TIME];
        psx_slowest_time = psx_fastest_time;
    }

#if 0
    /* NOTE: the code commented out in this function is used for variance
     * calculation.  This metric is most helpful for shared file records,
     * but this function has now been generalized to allow for aggregation
     * of arbitrary records in a log.
     *
     * This functionality could be reinstated as an optional feature.
     * Ideally in that case the caller would also provide a buffer for
     * stateful calculations; the current logic assumes that it is safe to
     * use additional bytes off of the end of the rec argument buffer.
     */
    struct var_t *var_time_p = (struct var_t *)
        ((char *)rec + sizeof(struct darshan_posix_file));
    struct var_t *var_bytes_p = (struct var_t *)
        ((char *)var_time_p + sizeof(struct var_t));
    double old_M;
#endif
    /* if this is our first record, store base id and rank */
    if(init_flag)
    {
        agg_psx_rec->base_rec.rank = psx_rec->base_rec.rank;
        agg_psx_rec->base_rec.id = psx_rec->base_rec.id;
    }

    /* so far do all of the records reference the same file? */
    if(agg_psx_rec->base_rec.id == psx_rec->base_rec.id)
        shared_file_flag = 1;
    else
        agg_psx_rec->base_rec.id = 0;

    /* so far do all of the records reference the same rank? */
    if(agg_psx_rec->base_rec.rank != psx_rec->base_rec.rank)
        agg_psx_rec->base_rec.rank = -1;

    for(i = 0; i < POSIX_NUM_INDICES; i++)
    {
        switch(i)
        {
            case POSIX_OPENS:
            case POSIX_FILENOS:
            case POSIX_DUPS:
            case POSIX_READS:
            case POSIX_WRITES:
            case POSIX_SEEKS:
            case POSIX_STATS:
            case POSIX_MMAPS:
            case POSIX_FSYNCS:
            case POSIX_FDSYNCS:
            case POSIX_RENAME_SOURCES:
            case POSIX_RENAME_TARGETS:
            case POSIX_BYTES_READ:
            case POSIX_BYTES_WRITTEN:
            case POSIX_CONSEC_READS:
            case POSIX_CONSEC_WRITES:
            case POSIX_SEQ_READS:
            case POSIX_SEQ_WRITES:
            case POSIX_RW_SWITCHES:
            case POSIX_MEM_NOT_ALIGNED:
            case POSIX_FILE_NOT_ALIGNED:
            case POSIX_SIZE_READ_0_100:
            case POSIX_SIZE_READ_100_1K:
            case POSIX_SIZE_READ_1K_10K:
            case POSIX_SIZE_READ_10K_100K:
            case POSIX_SIZE_READ_100K_1M:
            case POSIX_SIZE_READ_1M_4M:
            case POSIX_SIZE_READ_4M_10M:
            case POSIX_SIZE_READ_10M_100M:
            case POSIX_SIZE_READ_100M_1G:
            case POSIX_SIZE_READ_1G_PLUS:
            case POSIX_SIZE_WRITE_0_100:
            case POSIX_SIZE_WRITE_100_1K:
            case POSIX_SIZE_WRITE_1K_10K:
            case POSIX_SIZE_WRITE_10K_100K:
            case POSIX_SIZE_WRITE_100K_1M:
            case POSIX_SIZE_WRITE_1M_4M:
            case POSIX_SIZE_WRITE_4M_10M:
            case POSIX_SIZE_WRITE_10M_100M:
            case POSIX_SIZE_WRITE_100M_1G:
            case POSIX_SIZE_WRITE_1G_PLUS:
                /* sum */
                agg_psx_rec->counters[i] += psx_rec->counters[i];
                if(agg_psx_rec->counters[i] < 0) /* make sure invalid counters are -1 exactly */
                    agg_psx_rec->counters[i] = -1;
                break;
            case POSIX_RENAMED_FROM:
            case POSIX_MODE:
            case POSIX_MEM_ALIGNMENT:
            case POSIX_FILE_ALIGNMENT:
                /* just set to the input value */
                agg_psx_rec->counters[i] = psx_rec->counters[i];
                break;
            case POSIX_MAX_BYTE_READ:
            case POSIX_MAX_BYTE_WRITTEN:
                /* max */
                if(psx_rec->counters[i] > agg_psx_rec->counters[i])
                {
                    agg_psx_rec->counters[i] = psx_rec->counters[i];
                }
                break;
            case POSIX_MAX_READ_TIME_SIZE:
            case POSIX_MAX_WRITE_TIME_SIZE:
            case POSIX_FASTEST_RANK:
            case POSIX_FASTEST_RANK_BYTES:
            case POSIX_SLOWEST_RANK:
            case POSIX_SLOWEST_RANK_BYTES:
                /* these are set with the FP counters */
                break;
            case POSIX_STRIDE1_STRIDE:
            case POSIX_ACCESS1_ACCESS:
                /* NOTE: this same code block is used to collapse both the
                 * ACCESS and STRIDE counter sets (see the drop through in
                 * the case above). We therefore have to take care to zero
                 * any stateful variables that might get reused.
                 */
                memset(duplicate_mask, 0, 4*sizeof(duplicate_mask[0]));
                /* increment common value counters */

                /* first, collapse duplicates */
                for(j = i; j < i + 4; j++)
                {
                    for(k = 0; k < 4; k++)
                    {
                        if(agg_psx_rec->counters[i + k] == psx_rec->counters[j])
                        {
                            agg_psx_rec->counters[i + k + 4] += psx_rec->counters[j + 4];
                            /* flag that we should ignore this one now */
                            duplicate_mask[j-i] = 1;
                        }
                    }
                }

                /* second, add new counters */
                for(j = i; j < i + 4; j++)
                {
                    /* skip any that were handled above already */
                    if(duplicate_mask[j-i])
                        continue;
                    tmp_ndx = 0;
                    memset(tmp_val, 0, 4 * sizeof(int64_t));
                    memset(tmp_cnt, 0, 4 * sizeof(int64_t));

                    if(psx_rec->counters[j] == 0) break;
                    for(k = 0; k < 4; k++)
                    {
                        if(agg_psx_rec->counters[i + k] == psx_rec->counters[j])
                        {
                            total_count = agg_psx_rec->counters[i + k + 4] +
                                psx_rec->counters[j + 4];
                            break;
                        }
                    }
                    if(k == 4) total_count = psx_rec->counters[j + 4];

                    for(k = 0; k < 4; k++)
                    {
                        if((agg_psx_rec->counters[i + k + 4] > total_count) ||
                           ((agg_psx_rec->counters[i + k + 4] == total_count) &&
                            (agg_psx_rec->counters[i + k] > psx_rec->counters[j])))
                        {
                            tmp_val[tmp_ndx] = agg_psx_rec->counters[i + k];
                            tmp_cnt[tmp_ndx] = agg_psx_rec->counters[i + k + 4];
                            tmp_ndx++;
                        }
                        else break;
                    }
                    if(tmp_ndx == 4) break;

                    tmp_val[tmp_ndx] = psx_rec->counters[j];
                    tmp_cnt[tmp_ndx] = psx_rec->counters[j + 4];
                    tmp_ndx++;

                    while(tmp_ndx != 4)
                    {
                        if(agg_psx_rec->counters[i + k] != psx_rec->counters[j])
                        {
                            tmp_val[tmp_ndx] = agg_psx_rec->counters[i + k];
                            tmp_cnt[tmp_ndx] = agg_psx_rec->counters[i + k + 4];
                            tmp_ndx++;
                        }
                        k++;
                    }
                    memcpy(&(agg_psx_rec->counters[i]), tmp_val, 4 * sizeof(int64_t));
                    memcpy(&(agg_psx_rec->counters[i + 4]), tmp_cnt, 4 * sizeof(int64_t));
                }
                break;
            case POSIX_STRIDE2_STRIDE:
            case POSIX_STRIDE3_STRIDE:
            case POSIX_STRIDE4_STRIDE:
            case POSIX_STRIDE1_COUNT:
            case POSIX_STRIDE2_COUNT:
            case POSIX_STRIDE3_COUNT:
            case POSIX_STRIDE4_COUNT:
            case POSIX_ACCESS2_ACCESS:
            case POSIX_ACCESS3_ACCESS:
            case POSIX_ACCESS4_ACCESS:
            case POSIX_ACCESS1_COUNT:
            case POSIX_ACCESS2_COUNT:
            case POSIX_ACCESS3_COUNT:
            case POSIX_ACCESS4_COUNT:
                /* these are set all at once with common counters above */
                break;
                /* intentionally do not include a default block; we want to
                 * get a compile-time warning in this function when new
                 * counters are added to the enumeration to make sure we
                 * handle them all correctly.
                 */
#if 0
            default:
                agg_psx_rec->counters[i] = -1;
                break;
#endif
        }
    }

    for(i = 0; i < POSIX_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case POSIX_F_READ_TIME:
            case POSIX_F_WRITE_TIME:
            case POSIX_F_META_TIME:
                /* sum */
                agg_psx_rec->fcounters[i] += psx_rec->fcounters[i];
                break;
            case POSIX_F_OPEN_START_TIMESTAMP:
            case POSIX_F_READ_START_TIMESTAMP:
            case POSIX_F_WRITE_START_TIMESTAMP:
            case POSIX_F_CLOSE_START_TIMESTAMP:
                /* minimum non-zero */
                if((psx_rec->fcounters[i] > 0)  &&
                    ((agg_psx_rec->fcounters[i] == 0) ||
                    (psx_rec->fcounters[i] < agg_psx_rec->fcounters[i])))
                {
                    agg_psx_rec->fcounters[i] = psx_rec->fcounters[i];
                }
                break;
            case POSIX_F_OPEN_END_TIMESTAMP:
            case POSIX_F_READ_END_TIMESTAMP:
            case POSIX_F_WRITE_END_TIMESTAMP:
            case POSIX_F_CLOSE_END_TIMESTAMP:
                /* maximum */
                if(psx_rec->fcounters[i] > agg_psx_rec->fcounters[i])
                {
                    agg_psx_rec->fcounters[i] = psx_rec->fcounters[i];
                }
                break;
            case POSIX_F_MAX_READ_TIME:
                if(psx_rec->fcounters[i] > agg_psx_rec->fcounters[i])
                {
                    agg_psx_rec->fcounters[i] = psx_rec->fcounters[i];
                    agg_psx_rec->counters[POSIX_MAX_READ_TIME_SIZE] =
                        psx_rec->counters[POSIX_MAX_READ_TIME_SIZE];
                }
                break;
            case POSIX_F_MAX_WRITE_TIME:
                if(psx_rec->fcounters[i] > agg_psx_rec->fcounters[i])
                {
                    agg_psx_rec->fcounters[i] = psx_rec->fcounters[i];
                    agg_psx_rec->counters[POSIX_MAX_WRITE_TIME_SIZE] =
                        psx_rec->counters[POSIX_MAX_WRITE_TIME_SIZE];
                }
                break;
            case POSIX_F_FASTEST_RANK_TIME:

                if(!shared_file_flag)
                {
                    /* The fastest counters are only valid under these
                     * conditions when aggregating records that all refer to
                     * the same file.
                     */
                    agg_psx_rec->counters[POSIX_FASTEST_RANK] = -1;
                    agg_psx_rec->counters[POSIX_FASTEST_RANK_BYTES] = -1;
                    agg_psx_rec->fcounters[POSIX_F_FASTEST_RANK_TIME] = 0.0;
                    break;
                }
                if (init_flag ||
                    psx_fastest_time < agg_psx_rec->fcounters[POSIX_F_FASTEST_RANK_TIME]) {
                    /* The incoming record wins if a) this is the first
                     * record we are aggregating or b) it is the fastest
                     * record we have seen so far.
                     */
                    agg_psx_rec->counters[POSIX_FASTEST_RANK]
                        = psx_fastest_rank;
                    agg_psx_rec->counters[POSIX_FASTEST_RANK_BYTES]
                        = psx_fastest_bytes;
                    agg_psx_rec->fcounters[POSIX_F_FASTEST_RANK_TIME]
                        = psx_fastest_time;
                }
                break;
            case POSIX_F_SLOWEST_RANK_TIME:
                if(!shared_file_flag)
                {
                    /* The slowest counters are only valid under these
                     * conditions when aggregating records that all refer to
                     * the same file.
                     */
                    agg_psx_rec->counters[POSIX_SLOWEST_RANK] = -1;
                    agg_psx_rec->counters[POSIX_SLOWEST_RANK_BYTES] = -1;
                    agg_psx_rec->fcounters[POSIX_F_SLOWEST_RANK_TIME] = 0.0;
                    break;
                }
                if (init_flag ||
                    psx_slowest_time > agg_psx_rec->fcounters[POSIX_F_SLOWEST_RANK_TIME]) {
                    /* The incoming record wins if a) this is the first
                     * record we are aggregating or b) it is the slowest
                     * record we have seen so far.
                     */
                    agg_psx_rec->counters[POSIX_SLOWEST_RANK]
                        = psx_slowest_rank;
                    agg_psx_rec->counters[POSIX_SLOWEST_RANK_BYTES]
                        = psx_slowest_bytes;
                    agg_psx_rec->fcounters[POSIX_F_SLOWEST_RANK_TIME]
                        = psx_slowest_time;
                }
                break;
            case POSIX_F_VARIANCE_RANK_TIME:
#if 0
/* NOTE: see comment at the top of this function about the var_* variables */
                if(init_flag)
                {
                    var_time_p->n = 1;
                    var_time_p->M = psx_time;
                    var_time_p->S = 0;
                }
                else
                {
                    old_M = var_time_p->M;

                    var_time_p->n++;
                    var_time_p->M += (psx_time - var_time_p->M) / var_time_p->n;
                    var_time_p->S += (psx_time - var_time_p->M) * (psx_time - old_M);

                    agg_psx_rec->fcounters[POSIX_F_VARIANCE_RANK_TIME] =
                        var_time_p->S / var_time_p->n;
                }
#else
                agg_psx_rec->fcounters[i] = 0;
#endif
                break;
            case POSIX_F_VARIANCE_RANK_BYTES:
#if 0
/* NOTE: see comment at the top of this function about the var_* variables */
                if(init_flag)
                {
                    var_bytes_p->n = 1;
                    var_bytes_p->M = psx_bytes;
                    var_bytes_p->S = 0;
                }
                else
                {
                    old_M = var_bytes_p->M;

                    var_bytes_p->n++;
                    var_bytes_p->M += (psx_bytes - var_bytes_p->M) / var_bytes_p->n;
                    var_bytes_p->S += (psx_bytes - var_bytes_p->M) * (psx_bytes - old_M);

                    agg_psx_rec->fcounters[POSIX_F_VARIANCE_RANK_BYTES] =
                        var_bytes_p->S / var_bytes_p->n;
                }
#else
                agg_psx_rec->fcounters[i] = 0;
#endif
                break;
                /* intentionally do not include a default block; we want to
                 * get a compile-time warning in this function when new
                 * counters are added to the enumeration to make sure we
                 * handle them all correctly.
                 */
#if 0
            default:
                agg_psx_rec->fcounters[i] = -1;
                break;
#endif
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
