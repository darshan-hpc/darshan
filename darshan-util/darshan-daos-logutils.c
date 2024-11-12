/*
 * Copyright (C) 2020 University of Chicago.
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
#include <errno.h>

#include "darshan-logutils.h"

#ifdef HAVE_LIBUUID
#include <uuid/uuid.h>
#endif

/* counter name strings for the DAOS module */
#define X(a) #a,
char *daos_counter_names[] = {
    DAOS_COUNTERS
};

char *daos_f_counter_names[] = {
    DAOS_F_COUNTERS
};
#undef X

static int darshan_log_get_daos_object(darshan_fd fd, void** daos_buf_p);
static int darshan_log_put_daos_object(darshan_fd fd, void* daos_buf);
static void darshan_log_print_daos_object(void *object_rec,
    char *object_name, char *mnt_pt, char *fs_type);
static void darshan_log_print_daos_description(int ver);
static void darshan_log_print_daos_object_diff(void *obj_rec1, char *obj_name1,
    void *obj_rec2, char *obj_name2);
static void darshan_log_agg_daos_objects(void *rec, void *agg_rec, int init_flag);
static int darshan_log_sizeof_daos_object(void* daos_buf_p);
static int darshan_log_record_metrics_daos_object(void* daos_buf_p,
                                                 uint64_t* rec_id,
                                                 int64_t* r_bytes,
                                                 int64_t* w_bytes,
                                                 int64_t* max_offset,
                                                 double* io_total_time,
                                                 double* md_only_time,
                                                 double* rw_only_time,
                                                 int64_t* rank,
                                                 int64_t* nprocs);

struct darshan_mod_logutil_funcs daos_logutils =
{
    .log_get_record = &darshan_log_get_daos_object,
    .log_put_record = &darshan_log_put_daos_object,
    .log_print_record = &darshan_log_print_daos_object,
    .log_print_description = &darshan_log_print_daos_description,
    .log_print_diff = &darshan_log_print_daos_object_diff,
    .log_agg_records = &darshan_log_agg_daos_objects,
    .log_sizeof_record = &darshan_log_sizeof_daos_object,
    .log_record_metrics = &darshan_log_record_metrics_daos_object
};

static int darshan_log_sizeof_daos_object(void* daos_buf_p)
{
    /* daos records have a fixed size */
    return(sizeof(struct darshan_daos_object));
}

static int darshan_log_record_metrics_daos_object(void* daos_buf_p,
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
    struct darshan_daos_object *daos_rec = (struct darshan_daos_object *)daos_buf_p;

    *rec_id = daos_rec->base_rec.id;
    *r_bytes = daos_rec->counters[DAOS_BYTES_READ];
    *w_bytes = daos_rec->counters[DAOS_BYTES_WRITTEN];

    /* the daos module doesn't report this */
    *max_offset = -1;

    *rank = daos_rec->base_rec.rank;
    /* nprocs is 1 per record, unless rank is negative, in which case we
     * report -1 as the rank value to represent "all"
     */
    if(daos_rec->base_rec.rank < 0)
        *nprocs = -1;
    else
        *nprocs = 1;

    if(daos_rec->base_rec.rank < 0) {
        /* shared object records populate a counter with the slowest rank time
         * (derived during reduction).  They do not have a breakdown of meta
         * and rw time, though.
         */
        *io_total_time = daos_rec->fcounters[DAOS_F_SLOWEST_RANK_TIME];
        *md_only_time = 0;
        *rw_only_time = 0;
    }
    else {
        /* non-shared records have separate meta, read, and write values
         * that we can combine as needed
         */
        *io_total_time = daos_rec->fcounters[DAOS_F_META_TIME] +
                         daos_rec->fcounters[DAOS_F_READ_TIME] +
                         daos_rec->fcounters[DAOS_F_WRITE_TIME];
        *md_only_time = daos_rec->fcounters[DAOS_F_META_TIME];
        *rw_only_time = daos_rec->fcounters[DAOS_F_READ_TIME] +
                        daos_rec->fcounters[DAOS_F_WRITE_TIME];
    }

    return(0);
}

static int darshan_log_get_daos_object(darshan_fd fd, void** daos_buf_p)
{
    struct darshan_daos_object *obj = *((struct darshan_daos_object **)daos_buf_p);
    int rec_len;
    int i;
    int ret = -1;

    if(fd->mod_map[DARSHAN_DAOS_MOD].len == 0)
        return(0);

    if(fd->mod_ver[DARSHAN_DAOS_MOD] == 0 ||
        fd->mod_ver[DARSHAN_DAOS_MOD] > DARSHAN_DAOS_VER)
    {
        fprintf(stderr, "Error: Invalid DAOS module version number (got %d)\n",
            fd->mod_ver[DARSHAN_DAOS_MOD]);
        return(-1);
    }

    if(*daos_buf_p == NULL)
    {
        obj = malloc(sizeof(*obj));
        if(!obj)
            return(-1);
    }

    if(fd->mod_ver[DARSHAN_DAOS_MOD] == DARSHAN_DAOS_VER)
    {
        /* log format is in current version, so we don't need to do any
         * translation of counters while reading
         */
        rec_len = sizeof(struct darshan_daos_object);
        ret = darshan_log_get_mod(fd, DARSHAN_DAOS_MOD, obj, rec_len);
    }
    else
    {
        assert(0);
    }

    if(*daos_buf_p == NULL)
    {
        if(ret == rec_len)
            *daos_buf_p = obj;
        else
            free(obj);
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
            DARSHAN_BSWAP64(&obj->base_rec.id);
            DARSHAN_BSWAP64(&obj->base_rec.rank);
            for(i=0; i<DAOS_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&obj->counters[i]);
            for(i=0; i<DAOS_F_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&obj->fcounters[i]);
            DARSHAN_BSWAP128(&obj->pool_uuid);
            DARSHAN_BSWAP128(&obj->cont_uuid);
            DARSHAN_BSWAP64(&obj->oid_hi);
            DARSHAN_BSWAP64(&obj->oid_lo);
        }

        return(1);
    }
}

static int darshan_log_put_daos_object(darshan_fd fd, void* daos_buf)
{
    struct darshan_daos_object *obj = (struct darshan_daos_object *)daos_buf;
    int ret;

    ret = darshan_log_put_mod(fd, DARSHAN_DAOS_MOD, obj,
        sizeof(struct darshan_daos_object), DARSHAN_DAOS_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

static void darshan_log_print_daos_object(void *object_rec, char *object_name,
    char *mnt_pt, char *fs_type)
{
    int i;
    struct darshan_daos_object *daos_object_rec =
        (struct darshan_daos_object *)object_rec;
    char oid[64];
    char pool_cont_uuid_str[128] = {0};

    sprintf(oid, "%lu.%lu", daos_object_rec->oid_hi, daos_object_rec->oid_lo);
    object_name = oid;

#ifdef HAVE_LIBUUID
    uuid_unparse(daos_object_rec->pool_uuid, pool_cont_uuid_str);
#else
    strcat(pool_cont_uuid_str, "N/A");
#endif
    strcat(pool_cont_uuid_str, ":");
#ifdef HAVE_LIBUUID
    uuid_unparse(daos_object_rec->cont_uuid, pool_cont_uuid_str+strlen(pool_cont_uuid_str));
#else
    strcat(pool_cont_uuid_str, "N/A");
#endif

    mnt_pt = pool_cont_uuid_str;
    fs_type = "N/A";

    for(i=0; i<DAOS_NUM_INDICES; i++)
    {
        DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_DAOS_MOD],
            daos_object_rec->base_rec.rank, daos_object_rec->base_rec.id,
            daos_counter_names[i], daos_object_rec->counters[i],
            object_name, mnt_pt, fs_type);
    }

    for(i=0; i<DAOS_F_NUM_INDICES; i++)
    {
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_DAOS_MOD],
            daos_object_rec->base_rec.rank, daos_object_rec->base_rec.id,
            daos_f_counter_names[i], daos_object_rec->fcounters[i],
            object_name, mnt_pt, fs_type);
    }

    return;
}

static void darshan_log_print_daos_description(int ver)
{
    printf("\n# description of DAOS counters:\n");
    printf("#   DAOS_*: DAOS operation counts.\n");
    printf("#   OBJ_OPENS,OBJ_FETCHES,OBJ_UPDATES,OBJ_PUNCHES,OBJ_DKEY_PUNCHES,OBJ_AKEY_PUNCHES,OBJ_DKEY_LISTS,OBJ_AKEY_LISTS,OBJ_RECX_LISTS are types of DAOS object operations.\n");
    printf("#   ARRAY_OPENS,ARRAY_READS,ARRAY_WRITES,ARRAY_GET_SIZES,ARRAY_SET_SIZES,ARRAY_STATS,ARRAY_PUNCHES,ARRAY_DESTROYS are types of DAOS array operations\n");
    printf("#   KV_OPENS,KV_GETS,KV_PUTS,KV_REMOVES,KV_LISTS,KV_DESTROYS are types of DAOS kv operations\n");
    printf("#   DAOS_BYTES_*: total bytes read and written using all DAOS object APIs.\n");
    printf("#   DAOS_RW_SWITCHES: number of times access alternated between read and write.\n");
    printf("#   DAOS_MAX_*_TIME_SIZE: size of the slowest read and write operations.\n");
    printf("#   DAOS_SIZE_*_*: histogram of read and write access sizes.\n");
    printf("#   DAOS_ACCESS*_ACCESS: the four most common access sizes.\n");
    printf("#   DAOS_ACCESS*_COUNT: count of the four most common access sizes.\n");
    printf("#   DAOS_OBJ_OTYPE: DAOS otype value for the object.\n");
    printf("#   DAOS_ARRAY_CELL_SIZE: the cell size for DAOS array objects\n");
    printf("#   DAOS_ARRAY_CHUNK_SIZE: the chunk size for DAOS array objects\n");
    printf("#   DAOS_*_RANK: rank of the processes that were the fastest and slowest at I/O (for shared objects).\n");
    printf("#   DAOS_*_RANK_BYTES: bytes transferred by the fastest and slowest ranks (for shared objects).\n");
    printf("#   DAOS_F_*_START_TIMESTAMP: timestamp of first open/read/write/close.\n");
    printf("#   DAOS_F_*_END_TIMESTAMP: timestamp of last open/read/write/close.\n");
    printf("#   DAOS_F_READ/WRITE/META_TIME: cumulative time spent in read, write, or metadata operations.\n");
    printf("#   DAOS_F_MAX_*_TIME: duration of the slowest read and write operations.\n");
    printf("#   DAOS_F_*_RANK_TIME: fastest and slowest I/O time for a single rank (for shared objects).\n");

    return;
}

static void darshan_log_print_daos_object_diff(void *obj_rec1, char *obj_name1,
    void *obj_rec2, char *obj_name2)
{
    struct darshan_daos_object *obj1 = (struct darshan_daos_object *)obj_rec1;
    struct darshan_daos_object *obj2 = (struct darshan_daos_object *)obj_rec2;
    char obj_oid1[64], obj_oid2[64];
    int i;

    sprintf(obj_oid1, "%lu.%lu", obj1->oid_hi, obj1->oid_lo);
    sprintf(obj_oid2, "%lu.%lu", obj2->oid_hi, obj2->oid_lo);

    /* NOTE: we assume that both input records are the same module format version */

    for(i=0; i<DAOS_NUM_INDICES; i++)
    {
        if(!obj2)
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_DAOS_MOD],
                obj1->base_rec.rank, obj1->base_rec.id, daos_counter_names[i],
                obj1->counters[i], obj_oid1, "", "");
        }
        else if(!obj1)
        {
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_DAOS_MOD],
                obj2->base_rec.rank, obj2->base_rec.id, daos_counter_names[i],
                obj2->counters[i], obj_oid2, "", "");
        }
        else if(obj1->counters[i] != obj2->counters[i])
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_DAOS_MOD],
                obj1->base_rec.rank, obj1->base_rec.id, daos_counter_names[i],
                obj1->counters[i], obj_oid1, "", "");
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_DAOS_MOD],
                obj2->base_rec.rank, obj2->base_rec.id, daos_counter_names[i],
                obj2->counters[i], obj_oid2, "", "");
        }
    }

    for(i=0; i<DAOS_F_NUM_INDICES; i++)
    {
        if(!obj2)
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_DAOS_MOD],
                obj1->base_rec.rank, obj1->base_rec.id, daos_f_counter_names[i],
                obj1->fcounters[i], obj_oid1, "", "");
        }
        else if(!obj1)
        {
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_DAOS_MOD],
                obj2->base_rec.rank, obj2->base_rec.id, daos_f_counter_names[i],
                obj2->fcounters[i], obj_oid2, "", "");
        }
        else if(obj1->fcounters[i] != obj2->fcounters[i])
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_DAOS_MOD],
                obj1->base_rec.rank, obj1->base_rec.id, daos_f_counter_names[i],
                obj1->fcounters[i], obj_oid1, "", "");
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_DAOS_MOD],
                obj2->base_rec.rank, obj2->base_rec.id, daos_f_counter_names[i],
                obj2->fcounters[i], obj_oid2, "", "");
        }
    }

    return;
}

static void darshan_log_agg_daos_objects(void *rec, void *agg_rec, int init_flag)
{
    struct darshan_daos_object *daos_rec = (struct darshan_daos_object *)rec;
    struct darshan_daos_object *agg_daos_rec = (struct darshan_daos_object *)agg_rec;
    int i, j, k;
    int total_count;
    int64_t tmp_val[4];
    int64_t tmp_cnt[4];
    int duplicate_mask[4] = {0};
    int tmp_ndx;
    int64_t daos_fastest_rank, daos_slowest_rank,
        daos_fastest_bytes, daos_slowest_bytes;
    double daos_fastest_time, daos_slowest_time;
    int shared_file_flag = 0;

    /* For the incoming record, we need to determine what values to use for
     * subsequent comparision against the aggregate record's fastest and
     * slowest fields. This is is complicated by the fact that shared file
     * records already have derived values, while unique file records do
     * not.  Handle both cases here so that this function can be generic.
     */
    if(daos_rec->base_rec.rank == -1)
    {
        /* shared files should have pre-calculated fastest and slowest
         * counters */
        daos_fastest_rank = daos_rec->counters[DAOS_FASTEST_RANK];
        daos_slowest_rank = daos_rec->counters[DAOS_SLOWEST_RANK];
        daos_fastest_bytes = daos_rec->counters[DAOS_FASTEST_RANK_BYTES];
        daos_slowest_bytes = daos_rec->counters[DAOS_SLOWEST_RANK_BYTES];
        daos_fastest_time = daos_rec->fcounters[DAOS_F_FASTEST_RANK_TIME];
        daos_slowest_time = daos_rec->fcounters[DAOS_F_SLOWEST_RANK_TIME];
    }
    else
    {
        /* for non-shared files, derive bytes and time using data from this
         * rank
         */
        daos_fastest_rank = daos_rec->base_rec.rank;
        daos_slowest_rank = daos_fastest_rank;
        daos_fastest_bytes = daos_rec->counters[DAOS_BYTES_READ] +
            daos_rec->counters[DAOS_BYTES_WRITTEN];
        daos_slowest_bytes = daos_fastest_bytes;
        daos_fastest_time = daos_rec->fcounters[DAOS_F_READ_TIME] +
            daos_rec->fcounters[DAOS_F_WRITE_TIME] +
            daos_rec->fcounters[DAOS_F_META_TIME];
        daos_slowest_time = daos_fastest_time;
    }

    /* if this is our first record, store base id and rank */
    if(init_flag)
    {
        agg_daos_rec->base_rec.rank = daos_rec->base_rec.rank;
        agg_daos_rec->base_rec.id = daos_rec->base_rec.id;
    }

    /* so far do all of the records reference the same file? */
    if(agg_daos_rec->base_rec.id == daos_rec->base_rec.id)
        shared_file_flag = 1;
    else
        agg_daos_rec->base_rec.id = 0;

    /* so far do all of the records reference the same rank? */
    if(agg_daos_rec->base_rec.rank != daos_rec->base_rec.rank)
        agg_daos_rec->base_rec.rank = -1;

    for(i = 0; i < DAOS_NUM_INDICES; i++)
    {
        switch(i)
        {
            case DAOS_OBJ_OPENS:
            case DAOS_OBJ_FETCHES:
            case DAOS_OBJ_UPDATES:
            case DAOS_OBJ_PUNCHES:
            case DAOS_OBJ_DKEY_PUNCHES:
            case DAOS_OBJ_AKEY_PUNCHES:
            case DAOS_OBJ_DKEY_LISTS:
            case DAOS_OBJ_AKEY_LISTS:
            case DAOS_OBJ_RECX_LISTS:
            case DAOS_ARRAY_OPENS:
            case DAOS_ARRAY_READS:
            case DAOS_ARRAY_WRITES:
            case DAOS_ARRAY_GET_SIZES:
            case DAOS_ARRAY_SET_SIZES:
            case DAOS_ARRAY_STATS:
            case DAOS_ARRAY_PUNCHES:
            case DAOS_ARRAY_DESTROYS:
            case DAOS_KV_OPENS:
            case DAOS_KV_GETS:
            case DAOS_KV_PUTS:
            case DAOS_KV_REMOVES:
            case DAOS_KV_LISTS:
            case DAOS_KV_DESTROYS:
            case DAOS_BYTES_READ:
            case DAOS_BYTES_WRITTEN:
            case DAOS_RW_SWITCHES:
            case DAOS_SIZE_READ_0_100:
            case DAOS_SIZE_READ_100_1K:
            case DAOS_SIZE_READ_1K_10K:
            case DAOS_SIZE_READ_10K_100K:
            case DAOS_SIZE_READ_100K_1M:
            case DAOS_SIZE_READ_1M_4M:
            case DAOS_SIZE_READ_4M_10M:
            case DAOS_SIZE_READ_10M_100M:
            case DAOS_SIZE_READ_100M_1G:
            case DAOS_SIZE_READ_1G_PLUS:
            case DAOS_SIZE_WRITE_0_100:
            case DAOS_SIZE_WRITE_100_1K:
            case DAOS_SIZE_WRITE_1K_10K:
            case DAOS_SIZE_WRITE_10K_100K:
            case DAOS_SIZE_WRITE_100K_1M:
            case DAOS_SIZE_WRITE_1M_4M:
            case DAOS_SIZE_WRITE_4M_10M:
            case DAOS_SIZE_WRITE_10M_100M:
            case DAOS_SIZE_WRITE_100M_1G:
            case DAOS_SIZE_WRITE_1G_PLUS:
                /* sum */
                agg_daos_rec->counters[i] += daos_rec->counters[i];
                if(agg_daos_rec->counters[i] < 0) /* make sure invalid counters are -1 exactly */
                    agg_daos_rec->counters[i] = -1;
                break;
            case DAOS_OBJ_OTYPE:
            case DAOS_ARRAY_CELL_SIZE:
            case DAOS_ARRAY_CHUNK_SIZE:
                /* just set to the input value */
                agg_daos_rec->counters[i] = daos_rec->counters[i];
                break;
            case DAOS_MAX_READ_TIME_SIZE:
            case DAOS_MAX_WRITE_TIME_SIZE:
            case DAOS_FASTEST_RANK:
            case DAOS_FASTEST_RANK_BYTES:
            case DAOS_SLOWEST_RANK:
            case DAOS_SLOWEST_RANK_BYTES:
                /* these are set with the FP counters */
                break;
            case DAOS_ACCESS1_ACCESS:
                /* increment common value counters */

                /* first, collapse duplicates */
                for(j = i; j < i + 4; j++)
                {
                    for(k = 0; k < 4; k++)
                    {
                        if(agg_daos_rec->counters[i + k] == daos_rec->counters[j])
                        {
                            agg_daos_rec->counters[i + k + 4] += daos_rec->counters[j + 4];
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

                    if(daos_rec->counters[j] == 0) break;
                    for(k = 0; k < 4; k++)
                    {
                        if(agg_daos_rec->counters[i + k] == daos_rec->counters[j])
                        {
                            total_count = agg_daos_rec->counters[i + k + 4] +
                                daos_rec->counters[j + 4];
                            break;
                        }
                    }
                    if(k == 4) total_count = daos_rec->counters[j + 4];

                    for(k = 0; k < 4; k++)
                    {
                        if((agg_daos_rec->counters[i + k + 4] > total_count) ||
                           ((agg_daos_rec->counters[i + k + 4] == total_count) &&
                            (agg_daos_rec->counters[i + k] > daos_rec->counters[j])))
                        {
                            tmp_val[tmp_ndx] = agg_daos_rec->counters[i + k];
                            tmp_cnt[tmp_ndx] = agg_daos_rec->counters[i + k + 4];
                            tmp_ndx++;
                        }
                        else break;
                    }
                    if(tmp_ndx == 4) break;

                    tmp_val[tmp_ndx] = daos_rec->counters[j];
                    tmp_cnt[tmp_ndx] = daos_rec->counters[j + 4];
                    tmp_ndx++;

                    while(tmp_ndx != 4)
                    {
                        if(agg_daos_rec->counters[i + k] != daos_rec->counters[j])
                        {
                            tmp_val[tmp_ndx] = agg_daos_rec->counters[i + k];
                            tmp_cnt[tmp_ndx] = agg_daos_rec->counters[i + k + 4];
                            tmp_ndx++;
                        }
                        k++;
                    }
                    memcpy(&(agg_daos_rec->counters[i]), tmp_val, 4 * sizeof(int64_t));
                    memcpy(&(agg_daos_rec->counters[i + 4]), tmp_cnt, 4 * sizeof(int64_t));
                }
                break;
            case DAOS_ACCESS2_ACCESS:
            case DAOS_ACCESS3_ACCESS:
            case DAOS_ACCESS4_ACCESS:
            case DAOS_ACCESS1_COUNT:
            case DAOS_ACCESS2_COUNT:
            case DAOS_ACCESS3_COUNT:
            case DAOS_ACCESS4_COUNT:
                /* these are set all at once with common counters above */
                break;
        }
    }

    for(i = 0; i < DAOS_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case DAOS_F_READ_TIME:
            case DAOS_F_WRITE_TIME:
            case DAOS_F_META_TIME:
                /* sum */
                agg_daos_rec->fcounters[i] += daos_rec->fcounters[i];
                break;
            case DAOS_F_OPEN_START_TIMESTAMP:
            case DAOS_F_READ_START_TIMESTAMP:
            case DAOS_F_WRITE_START_TIMESTAMP:
            case DAOS_F_CLOSE_START_TIMESTAMP:
                /* minimum non-zero */
                if((daos_rec->fcounters[i] > 0)  &&
                    ((agg_daos_rec->fcounters[i] == 0) ||
                    (daos_rec->fcounters[i] < agg_daos_rec->fcounters[i])))
                {
                    agg_daos_rec->fcounters[i] = daos_rec->fcounters[i];
                }
                break;
            case DAOS_F_OPEN_END_TIMESTAMP:
            case DAOS_F_READ_END_TIMESTAMP:
            case DAOS_F_WRITE_END_TIMESTAMP:
            case DAOS_F_CLOSE_END_TIMESTAMP:
                /* maximum */
                if(daos_rec->fcounters[i] > agg_daos_rec->fcounters[i])
                {
                    agg_daos_rec->fcounters[i] = daos_rec->fcounters[i];
                }
                break;
            case DAOS_F_MAX_READ_TIME:
                if(daos_rec->fcounters[i] > agg_daos_rec->fcounters[i])
                {
                    agg_daos_rec->fcounters[i] = daos_rec->fcounters[i];
                    agg_daos_rec->counters[DAOS_MAX_READ_TIME_SIZE] =
                        daos_rec->counters[DAOS_MAX_READ_TIME_SIZE];
                }
                break;
            case DAOS_F_MAX_WRITE_TIME:
                if(daos_rec->fcounters[i] > agg_daos_rec->fcounters[i])
                {
                    agg_daos_rec->fcounters[i] = daos_rec->fcounters[i];
                    agg_daos_rec->counters[DAOS_MAX_WRITE_TIME_SIZE] =
                        daos_rec->counters[DAOS_MAX_WRITE_TIME_SIZE];
                }
                break;
            case DAOS_F_FASTEST_RANK_TIME:

                if(!shared_file_flag)
                {
                    /* The fastest counters are only valid under these
                     * conditions when aggregating records that all refer to
                     * the same file.
                     */
                    agg_daos_rec->counters[DAOS_FASTEST_RANK] = -1;
                    agg_daos_rec->counters[DAOS_FASTEST_RANK_BYTES] = -1;
                    agg_daos_rec->fcounters[DAOS_F_FASTEST_RANK_TIME] = 0.0;
                    break;
                }
                if (init_flag ||
                    daos_fastest_time < agg_daos_rec->fcounters[DAOS_F_FASTEST_RANK_TIME]) {
                    /* The incoming record wins if a) this is the first
                     * record we are aggregating or b) it is the fastest
                     * record we have seen so far.
                     */
                    agg_daos_rec->counters[DAOS_FASTEST_RANK]
                        = daos_fastest_rank;
                    agg_daos_rec->counters[DAOS_FASTEST_RANK_BYTES]
                        = daos_fastest_bytes;
                    agg_daos_rec->fcounters[DAOS_F_FASTEST_RANK_TIME]
                        = daos_fastest_time;
                }
                break;
            case DAOS_F_SLOWEST_RANK_TIME:
                if(!shared_file_flag)
                {
                    /* The slowest counters are only valid under these
                     * conditions when aggregating records that all refer to
                     * the same file.
                     */
                    agg_daos_rec->counters[DAOS_SLOWEST_RANK] = -1;
                    agg_daos_rec->counters[DAOS_SLOWEST_RANK_BYTES] = -1;
                    agg_daos_rec->fcounters[DAOS_F_SLOWEST_RANK_TIME] = 0.0;
                    break;
                }
                if (init_flag ||
                    daos_slowest_time > agg_daos_rec->fcounters[DAOS_F_SLOWEST_RANK_TIME]) {
                    /* The incoming record wins if a) this is the first
                     * record we are aggregating or b) it is the slowest
                     * record we have seen so far.
                     */
                    agg_daos_rec->counters[DAOS_SLOWEST_RANK]
                        = daos_slowest_rank;
                    agg_daos_rec->counters[DAOS_SLOWEST_RANK_BYTES]
                        = daos_slowest_bytes;
                    agg_daos_rec->fcounters[DAOS_F_SLOWEST_RANK_TIME]
                        = daos_slowest_time;
                }
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
