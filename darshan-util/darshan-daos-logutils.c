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
    char *file_name, char *mnt_pt, char *fs_type);
static void darshan_log_print_daos_description(int ver);
static void darshan_log_print_daos_object_diff(void *obj_rec1, char *obj_name1,
    void *obj_rec2, char *obj_name2);
static void darshan_log_agg_daos_objects(void *rec, void *agg_rec, int init_flag);

struct darshan_mod_logutil_funcs daos_logutils =
{
    .log_get_record = &darshan_log_get_daos_object,
    .log_put_record = &darshan_log_put_daos_object,
    .log_print_record = &darshan_log_print_daos_object,
    .log_print_description = &darshan_log_print_daos_description,
    .log_print_diff = &darshan_log_print_daos_object_diff,
    .log_agg_records = &darshan_log_agg_daos_objects,
};

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

    sprintf(oid, "%lu.%lu", daos_object_rec->oid_hi, daos_object_rec->oid_lo);
    object_name = oid;

    // XXX what bout mnt_pt/fs_type?
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
    printf("#   OPENS are types of operations.\n");
    printf("#   DAOS_F_*_START_TIMESTAMP: timestamp of first open.\n");
    printf("#   DAOS_F_*_END_TIMESTAMP: timestamp of last open.\n");
    printf("#   DAOS_F_READ/WRITE/META_TIME: cumulative time spent in read, write, or metadata operations.\n");

    return;
}

static void darshan_log_print_daos_object_diff(void *obj_rec1, char *obj_name1,
    void *obj_rec2, char *obj_name2)
{
    /* XXX */
    return;
}

static void darshan_log_agg_daos_objects(void *rec, void *agg_rec, int init_flag)
{
    /* XXX */
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
