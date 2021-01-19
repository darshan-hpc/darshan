/*
 * Copyright (C) 2020 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include "darshan-runtime-config.h"
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <errno.h>
#include <search.h>
#include <assert.h>
#include <pthread.h>
#include <limits.h>

#include "uthash.h"
#include "darshan.h"
#include "darshan-dynamic.h"

#include <daos_types.h>
#include <daos_prop.h>
#include <daos_pool.h>
#include <daos_cont.h>
#include <daos_obj.h>
#include <daos_obj_class.h>
#include <daos_array.h>
#include <daos_fs.h>


DARSHAN_FORWARD_DECL(dfs_mount, int, (daos_handle_t poh, daos_handle_t coh, int flags, dfs_t **dfs));
DARSHAN_FORWARD_DECL(dfs_lookup, int, (dfs_t *dfs, const char *path, int flags, dfs_obj_t **obj, mode_t *mode, struct stat *stbuf));
DARSHAN_FORWARD_DECL(dfs_lookup_rel, int, (dfs_t *dfs, dfs_obj_t *parent, const char *name, int flags, dfs_obj_t **obj, mode_t *mode, struct stat *stbuf));
DARSHAN_FORWARD_DECL(dfs_open, int, (dfs_t *dfs, dfs_obj_t *parent, const char *name, mode_t mode, int flags, daos_oclass_id_t cid, daos_size_t chunk_size, const char *value, dfs_obj_t **obj));
DARSHAN_FORWARD_DECL(dfs_dup, int, (dfs_t *dfs, dfs_obj_t *obj, int flags, dfs_obj_t **new_obj));
DARSHAN_FORWARD_DECL(dfs_obj_global2local, int, (dfs_t *dfs, int flags, d_iov_t glob, dfs_obj_t **obj));
DARSHAN_FORWARD_DECL(dfs_release, int, (dfs_obj_t *obj));

struct dfs_mount_info
{
    uuid_t pool_uuid;
    uuid_t cont_uuid;
    UT_hash_handle hlink;
};

struct dfs_runtime
{
    struct dfs_mount_info *mount_hash;
};

static struct dfs_runtime *dfs_runtime = NULL;

/*****************************************************
 *      Wrappers for DAOS functions of interest      * 
 *****************************************************/

int DARSHAN_DECL(dfs_mount)(daos_handle_t poh, daos_handle_t coh, int flags, dfs_t **dfs)
{
    int ret, query_ret;
    int success=0;
    daos_pool_info_t pool_info;
    daos_cont_info_t cont_info;
    struct dfs_mount_info  *mnt_info;

    fprintf(stderr, "***********DFS_MOUNT INTERCEPT\n");

    MAP_OR_FAIL(dfs_mount);

    ret = __real_dfs_mount(poh, coh, flags, dfs);

    /* TODO: Make sure we are init */
    if(ret == 0)
    {
        /* query pool UUID from open handle */
        query_ret = daos_pool_query(poh, NULL, &pool_info, NULL, NULL);
        if(query_ret == 0)
        {
            query_ret = daos_cont_query(coh, &cont_info, NULL, NULL);
            if(query_ret == 0)
                success = 1;
        }

        if(success)
        {
            /* create mapping from DFS mount to pool & container UUIDs */
            printf("**************FOUND\n");
            mnt_info = malloc(sizeof(*mnt_info));
            if(mnt_info)
            {
                uuid_copy(mnt_info->pool_uuid, pool_info.pi_uuid);
                uuid_copy(mnt_info->cont_uuid, cont_info.ci_uuid);
                HASH_ADD_KEYPTR(hlink, dfs_runtime->mount_hash, dfs, sizeof(dfs), mnt_info);
            }
        }
    }

    fprintf(stderr, "***********DFS_MOUNT INTERCEPT RETURN %d\n", ret);

    return(ret);
}

int DARSHAN_DECL(dfs_lookup)(dfs_t *dfs, const char *path, int flags, dfs_obj_t **obj, mode_t *mode, struct stat *stbuf)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(dfs_lookup);

    tm1 = darshan_core_wtime();
    ret = __real_dfs_lookup(dfs, path, flags, obj, mode, stbuf);
    tm2 = darshan_core_wtime();

    return(ret);
}

int DARSHAN_DECL(dfs_lookup_rel)(dfs_t *dfs, dfs_obj_t *parent, const char *name, int flags, dfs_obj_t **obj, mode_t *mode, struct stat *stbuf)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(dfs_lookup_rel);

    tm1 = darshan_core_wtime();
    ret = __real_dfs_lookup_rel(dfs, parent, name, flags, obj, mode, stbuf);
    tm2 = darshan_core_wtime();

    return(ret);
}

int DARSHAN_DECL(dfs_open)(dfs_t *dfs, dfs_obj_t *parent, const char *name, mode_t mode, int flags, daos_oclass_id_t cid, daos_size_t chunk_size, const char *value, dfs_obj_t **obj)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(dfs_open);

    tm1 = darshan_core_wtime();
    ret = __real_dfs_open(dfs, parent, name, mode, flags, cid, chunk_size, value, obj);
    tm2 = darshan_core_wtime();

    return(ret);
}

int DARSHAN_DECL(dfs_dup)(dfs_t *dfs, dfs_obj_t *obj, int flags, dfs_obj_t **new_obj)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(dfs_dup);

    tm1 = darshan_core_wtime();
    ret = __real_dfs_dup(dfs, obj, flags, new_obj);
    tm2 = darshan_core_wtime();

    return(ret);
}

int DARSHAN_DECL(dfs_obj_global2local)(dfs_t *dfs, int flags, d_iov_t glob, dfs_obj_t **obj)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(dfs_obj_global2local);

    tm1 = darshan_core_wtime();
    ret = __real_dfs_obj_global2local(dfs, flags, glob, obj);
    tm2 = darshan_core_wtime();

    return(ret);
}

int DARSHAN_DECL(dfs_release)(dfs_obj_t *obj)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(dfs_release);

    tm1 = darshan_core_wtime();
    ret = __real_dfs_release(obj);
    tm2 = darshan_core_wtime();

    return(ret);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */           
