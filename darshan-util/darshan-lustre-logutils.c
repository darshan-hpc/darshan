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

#include "darshan-logutils.h"

/* counter name strings for the LUSTRE module */
#define X(a) #a,
char *lustre_comp_counter_names[] = {
    LUSTRE_COMP_COUNTERS
};
#undef X

static int darshan_log_get_lustre_record(darshan_fd fd, void** lustre_buf_p);
static int darshan_log_put_lustre_record(darshan_fd fd, void* lustre_buf);
static void darshan_log_print_lustre_record(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type);
static void darshan_log_print_lustre_description(int ver);
static void darshan_log_print_lustre_record_diff(void *rec1, char *file_name1,
    void *rec2, char *file_name2);
static void darshan_log_agg_lustre_records(void *rec, void *agg_rec, int init_flag);

static int darshan_log_get_lustre_record_v1(darshan_fd fd, void** lustre_buf_p);

struct darshan_mod_logutil_funcs lustre_logutils =
{
    .log_get_record = &darshan_log_get_lustre_record,
    .log_put_record = &darshan_log_put_lustre_record,
    .log_print_record = &darshan_log_print_lustre_record,
    .log_print_description = &darshan_log_print_lustre_description,
    .log_print_diff = &darshan_log_print_lustre_record_diff,
    .log_agg_records = &darshan_log_agg_lustre_records
};

static int darshan_log_get_lustre_record(darshan_fd fd, void** lustre_buf_p)
{
    struct darshan_lustre_record *rec = *((struct darshan_lustre_record **)lustre_buf_p);
    struct darshan_lustre_record tmp_rec;
    int fixed_size, comps_size, osts_size;
    int new_comps_size, new_osts_size;
    int i, j;
    int ret;

    if(fd->mod_map[DARSHAN_LUSTRE_MOD].len == 0)
        return(0);

    if(fd->mod_ver[DARSHAN_LUSTRE_MOD] == 0 ||
        fd->mod_ver[DARSHAN_LUSTRE_MOD] > DARSHAN_LUSTRE_VER)
    {
        fprintf(stderr, "Error: Invalid Lustre module version number (got %d)\n",
            fd->mod_ver[DARSHAN_LUSTRE_MOD]);
        return(-1);
    }

    /* backwards compatibility support for version 1 records */
    if(fd->mod_ver[DARSHAN_LUSTRE_MOD] == 1)
        return darshan_log_get_lustre_record_v1(fd, lustre_buf_p);

    /* retrieve the fixed-size portion of the record */
    fixed_size = sizeof(struct darshan_base_record) + (2*sizeof(int64_t));
    ret = darshan_log_get_mod(fd, DARSHAN_LUSTRE_MOD, &tmp_rec, fixed_size);
    if(ret < 0)
        return(-1);
    else if(ret < fixed_size)
        return(0);

    /* swap bytes if necessary */
    if(fd->swap_flag)
    {
        DARSHAN_BSWAP64(&tmp_rec.base_rec.id);
        DARSHAN_BSWAP64(&tmp_rec.base_rec.rank);
        DARSHAN_BSWAP64(&tmp_rec.num_comps);
        DARSHAN_BSWAP64(&tmp_rec.num_stripes);
    }

    comps_size = tmp_rec.num_comps * sizeof(*tmp_rec.comps);
    osts_size = tmp_rec.num_stripes * sizeof(*tmp_rec.ost_ids);
    if(*lustre_buf_p == NULL)
    {
        rec = malloc(sizeof(struct darshan_lustre_record) + comps_size + osts_size);
        if(!rec)
            return(-1);
    }
    memcpy(rec, &tmp_rec, fixed_size);
    if(tmp_rec.num_comps < 1)
    {
        rec->comps = NULL;
        rec->ost_ids = NULL;
    }
    else
    {
        rec->comps = (struct darshan_lustre_component *)
            ((void *)rec + sizeof(struct darshan_lustre_record));
        rec->ost_ids = (OST_ID *)((void *)rec->comps + comps_size);

        /* now read all record components and OST IDs */
        ret = darshan_log_get_mod(
            fd,
            DARSHAN_LUSTRE_MOD,
            (void*)(rec->comps),
            comps_size + osts_size
        );
        if(ret < comps_size + osts_size)
            ret = -1;
        else
        {
            ret = 1;

            /* swap bytes if necessary */
            if (fd->swap_flag)
            {
                for (i = 0; i < rec->num_comps; i++)
                    for(j=0; j<LUSTRE_COMP_NUM_INDICES; j++)
                        DARSHAN_BSWAP64(&rec->comps[i].counters[j]);
                for (i = 0; i < rec->num_stripes; i++)
                    DARSHAN_BSWAP64(&rec->ost_ids[i]);
            }

            /* truncate any unused components/stripes leftover from runtime */
            rec->num_stripes = 0;
            for (i = 0; i < rec->num_comps; i++)
            {
                /* NOTE: at runtime, unused stripe components are marked with size of -1 */
                if (rec->comps[i].counters[LUSTRE_COMP_STRIPE_SIZE] == -1)
                {
                    rec->num_comps = i;
                    break;
                }
                rec->num_stripes += rec->comps[i].counters[LUSTRE_COMP_STRIPE_COUNT];
            }
            new_comps_size = rec->num_comps * sizeof(*rec->comps);
            new_osts_size = rec->num_stripes * sizeof(*rec->ost_ids);
            if (new_comps_size != comps_size)
            {
                memmove(((void*)rec->comps + new_comps_size),
                        ((void*)rec->comps + comps_size),
                        new_osts_size);
            }
            if ((new_comps_size != comps_size) || (new_osts_size != osts_size))
            {
                if(*lustre_buf_p == NULL)
                {
                    /* record buffer size changes, so we should realloc to match it */
                    rec = realloc(rec, sizeof(struct darshan_lustre_record) + new_comps_size + new_osts_size);
                    if(!rec)
                        return(-1);
                }
            }
        }
    }

    if(*lustre_buf_p == NULL)
    {
        if(ret == 1)
            *lustre_buf_p = rec;
        else
            free(rec);
    }

    return(ret);
}

static int darshan_log_get_lustre_record_v1(darshan_fd fd, void** lustre_buf_p)
{
    struct darshan_lustre_record *rec = *((struct darshan_lustre_record **)lustre_buf_p);
    int64_t fixed_record[7];
    int64_t stripe_size, stripe_count;
    int i;
    int ret;

    /* retrieve the fixed-size portion of the record */
    ret = darshan_log_get_mod(fd, DARSHAN_LUSTRE_MOD, &fixed_record, sizeof(fixed_record));
    if(ret < 0)
        return(-1);
    else if(ret < sizeof(fixed_record))
        return(0);

    /* swap bytes if necessary */
    if(fd->swap_flag)
        for (i = 0; i < 7; i++)
            DARSHAN_BSWAP64(&fixed_record[i]);

    stripe_size = fixed_record[5];
    stripe_count = fixed_record[6];

    if(*lustre_buf_p == NULL)
    {
        rec = malloc(sizeof(struct darshan_lustre_record) +
            sizeof(struct darshan_lustre_component) + (stripe_count * sizeof(OST_ID)));
        if(!rec)
            return(-1);
    }

    /* copy over base record first */
    memcpy(rec, &fixed_record, sizeof(struct darshan_base_record));
    rec->num_comps = 1; // only 1 component for old Lustre records
    rec->num_stripes = stripe_count; // newer records have separate field for total stripes
    rec->comps = (struct darshan_lustre_component *)
        ((void *)rec + sizeof(struct darshan_lustre_record));
    rec->ost_ids = (OST_ID *)
        ((void *)rec->comps + sizeof(struct darshan_lustre_component));
    /* fill in known parts of the component structure */
    rec->comps[0].counters[LUSTRE_COMP_STRIPE_SIZE] = stripe_size;
    rec->comps[0].counters[LUSTRE_COMP_STRIPE_COUNT] = stripe_count;
    rec->comps[0].counters[LUSTRE_COMP_STRIPE_PATTERN] = -1;
    rec->comps[0].counters[LUSTRE_COMP_FLAGS] = -1;
    rec->comps[0].counters[LUSTRE_COMP_EXT_START] = 0;
    rec->comps[0].counters[LUSTRE_COMP_EXT_END] = -1;
    rec->comps[0].counters[LUSTRE_COMP_MIRROR_ID] = -1;
    rec->comps[0].pool_name[0] = '\0';

    /* read the OST list */
    ret = darshan_log_get_mod(fd,
                              DARSHAN_LUSTRE_MOD,
                              (void*)(rec->ost_ids),
                              stripe_count * sizeof(OST_ID));
    if(ret < (stripe_count * sizeof(OST_ID)))
        ret = -1;
    else
    {
        ret = 1;
        /* swap bytes if necessary */
        if (fd->swap_flag)
            for (i = 0; i < stripe_count; i++)
                DARSHAN_BSWAP64(&rec->ost_ids[i]);
    }

    if(*lustre_buf_p == NULL)
    {
        if(ret == 1)
            *lustre_buf_p = rec;
        else
            free(rec);
    }

    return(ret);
}

static int darshan_log_put_lustre_record(darshan_fd fd, void* lustre_buf)
{
    struct darshan_lustre_record *rec = (struct darshan_lustre_record *)lustre_buf;
    int fixed_size, comps_size, osts_size;
    int ret;

    /* write fixed length and variable length portion of the record
     * seperately since they aren't contiguous in memory
     */

    fixed_size = sizeof(struct darshan_base_record) + (2*sizeof(int64_t));
    ret = darshan_log_put_mod(fd, DARSHAN_LUSTRE_MOD, rec,
        fixed_size, DARSHAN_LUSTRE_VER);
    if(ret < 0)
        return(-1);

    comps_size = rec->num_comps * sizeof(*rec->comps);
    osts_size = rec->num_stripes * sizeof(*rec->ost_ids);
    ret = darshan_log_put_mod(fd, DARSHAN_LUSTRE_MOD, rec->comps,
        comps_size+osts_size, DARSHAN_LUSTRE_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

static void darshan_log_print_lustre_record(void *rec, char *file_name,
    char *mnt_pt, char *fs_type)
{
    int i, j;
    struct darshan_lustre_record *lustre_rec =
        (struct darshan_lustre_record *)rec;
    char tmp_counter_str[64] = {0};
    char *ptr;
    int idx;
    int global_ost_idx = 0;

    DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_LUSTRE_MOD],
        lustre_rec->base_rec.rank, lustre_rec->base_rec.id, "LUSTRE_NUM_COMPONENTS",
        lustre_rec->num_comps, file_name, mnt_pt, fs_type);
    DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_LUSTRE_MOD],
        lustre_rec->base_rec.rank, lustre_rec->base_rec.id, "LUSTRE_NUM_STRIPES",
        lustre_rec->num_stripes, file_name, mnt_pt, fs_type);
    for(i=0; i<lustre_rec->num_comps; i++)
    {
        for(j=0; j < LUSTRE_COMP_NUM_INDICES; j++)
        {
            strcpy(tmp_counter_str, lustre_comp_counter_names[j]);
            ptr = strstr(tmp_counter_str, "_");
            ptr = strstr(ptr+1, "_");
            idx = ptr-tmp_counter_str;
            snprintf(ptr, 64-idx, "%d%s", i+1, &lustre_comp_counter_names[j][idx]);
            if(j == LUSTRE_COMP_STRIPE_PATTERN)
            {
                /* XXX copied from Lustre header (lustreapi.h) to avoid dependencies ... */
                #define LUSTRE_LAYOUT_RAID0      0ULL
                #define LUSTRE_LAYOUT_MDT        2ULL
                #define LUSTRE_LAYOUT_OVERSTRIPING   4ULL
                #define LUSTRE_LAYOUT_FOREIGN        8ULL
                uint64_t pattern = (uint64_t)lustre_rec->comps[i].counters[j];
                char *pattern_str = "N/A";
                if(pattern == LUSTRE_LAYOUT_RAID0) pattern_str = "raid0";
                else if(pattern == LUSTRE_LAYOUT_MDT) pattern_str = "mdt";
                else if(pattern == LUSTRE_LAYOUT_OVERSTRIPING) pattern_str = "raid0,overstriped";
                else if(pattern == LUSTRE_LAYOUT_FOREIGN) pattern_str = "foreign";
                DARSHAN_S_COUNTER_PRINT(darshan_module_names[DARSHAN_LUSTRE_MOD],
                    lustre_rec->base_rec.rank, lustre_rec->base_rec.id,
                    tmp_counter_str, pattern_str, file_name, mnt_pt, fs_type);
            }
            else if(j == LUSTRE_COMP_FLAGS)
            {
                int k;
                /* XXX copied from Lustre header (lustreapi.h) to avoid dependencies ... */
                char flag_str_table[12][32] = {
                    "stale,",
                    "prefrd,",
                    "prefwr,",
                    "offline,",
                    "init,",
                    "nosync,",
                    "extension,",
                    "parity,",
                    "compress,",
                    "partial,",
                    "nocompr,",
                    "neg,"
                };
                uint64_t flag = 1, flags = (uint64_t)lustre_rec->comps[i].counters[j];
                char flag_str[64] = {0};
                for(k = 0; k < sizeof(flag_str_table)/sizeof(flag_str_table[0]); k++)
                {
                    if(flags & flag)
                        strncat(flag_str, flag_str_table[k], 64 - strlen(flag_str));
                    flag = flag << 1;
                }
                /* drop last ',' separator */
                if(flag_str[0] != '\0') flag_str[strlen(flag_str)-1] = '\0';
                else strcpy(flag_str, "0");
                if (flags == -1) strcpy(flag_str, "N/A");
                DARSHAN_S_COUNTER_PRINT(darshan_module_names[DARSHAN_LUSTRE_MOD],
                    lustre_rec->base_rec.rank, lustre_rec->base_rec.id,
                    tmp_counter_str, flag_str, file_name, mnt_pt, fs_type);
            }
            else
            {
                DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_LUSTRE_MOD],
                    lustre_rec->base_rec.rank, lustre_rec->base_rec.id, tmp_counter_str,
                    lustre_rec->comps[i].counters[j], file_name, mnt_pt, fs_type);
            }
        }
        snprintf(ptr, 64-idx, "%d_POOL_NAME", i+1);
        char *pool_name = "N/A";
        if(lustre_rec->comps[i].pool_name[0] != '\0')
            pool_name = lustre_rec->comps[i].pool_name;
        DARSHAN_S_COUNTER_PRINT(darshan_module_names[DARSHAN_LUSTRE_MOD],
                lustre_rec->base_rec.rank, lustre_rec->base_rec.id,
                tmp_counter_str, pool_name, file_name, mnt_pt, fs_type);
        for(j = 0; j < lustre_rec->comps[i].counters[LUSTRE_COMP_STRIPE_COUNT]; j++)
        {
            snprintf(ptr, 64-idx, "%d_OST_ID_%d", i+1, j);
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_LUSTRE_MOD],
                    lustre_rec->base_rec.rank, lustre_rec->base_rec.id,
                    tmp_counter_str, lustre_rec->ost_ids[global_ost_idx++],
                    file_name, mnt_pt, fs_type);
        }
    }

    return;
}

static void darshan_log_print_lustre_description(int ver)
{
    printf("\n# description of LUSTRE counters:\n");
    printf("#   LUSTRE_NUM_COMPONENTS: number of instrumented components in the Lustre layout.\n");
    printf("#   LUSTRE_NUM_STRIPES: number of active stripes in the Lustre layout components.\n");
    printf("#   LUSTRE_COMP*_STRIPE_SIZE: stripe size for this file layout component in bytes.\n");
    printf("#   LUSTRE_COMP*_STRIPE_COUNT: number of OSTs over which the file layout component is striped.\n");
    printf("#   LUSTRE_COMP*_STRIPE_PATTERN: pattern (e.g., raid0, mdt, overstriped) for this file layout component.\n");
    printf("#   LUSTRE_COMP*_FLAGS: captured flags (e.g. init, prefwr, stale) for this file layout component.\n");
    printf("#   LUSTRE_COMP*_EXT_START: starting file extent for this file layout component.\n");
    printf("#   LUSTRE_COMP*_EXT_END: ending file extent for this file layout component (-1 means EOF).\n");
    printf("#   LUSTRE_COMP*_MIRROR_ID: mirror ID for this file layout component, if mirrors are enabled.\n");
    printf("#   LUSTRE_COMP*_POOL_NAME: Lustre OST pool used for this file layout component.\n");
    printf("#   LUSTRE_COMP*_OST_ID_*: indices of OSTs over which this file layout component is striped.\n");

    return;
}

static void darshan_log_print_lustre_record_diff(void *rec1, char *file_name1,
    void *rec2, char *file_name2)
{
    fprintf(stderr, "Warning: Darshan Lustre module does not support printing record diffs.\n");
    return;
}

static void darshan_log_agg_lustre_records(void *rec, void *agg_rec, int init_flag)
{
    struct darshan_lustre_record *lustre_rec = (struct darshan_lustre_record *)rec;
    struct darshan_lustre_record *agg_lustre_rec = (struct darshan_lustre_record *)agg_rec;
    int comps_size = 0, osts_size = 0;

    if(init_flag)
    {
        /* just initialize to the first record we see and never change -- it's
         * possible different processes could see different Lustre parameters/flags,
         * so let's not even bother with trying to reconcile which to choose for the
         * final output record
         */
        memcpy(agg_lustre_rec, lustre_rec, sizeof(struct darshan_lustre_record));
        agg_lustre_rec->comps = (struct darshan_lustre_component *)
            ((void *)agg_lustre_rec + sizeof(struct darshan_lustre_record));
        comps_size = lustre_rec->num_comps * sizeof(*lustre_rec->comps);
        memcpy(agg_lustre_rec->comps, lustre_rec->comps, comps_size);
        agg_lustre_rec->ost_ids = (OST_ID *)((void *)agg_lustre_rec->comps + comps_size);
        osts_size = lustre_rec->num_stripes * sizeof(*lustre_rec->ost_ids);
        memcpy(agg_lustre_rec->ost_ids, lustre_rec->ost_ids, osts_size);
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
