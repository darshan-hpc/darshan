/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

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
#define __USE_GNU
#include <pthread.h>

#include "uthash.h"

#include "darshan.h"
#include "darshan-hdf5-log-format.h"
#include "darshan-dynamic.h"

/* hope this doesn't change any time soon */
typedef int hid_t;
typedef int herr_t;

DARSHAN_FORWARD_DECL(H5Fcreate, hid_t, (const char *filename, unsigned flags, hid_t create_plist, hid_t access_plist));
DARSHAN_FORWARD_DECL(H5Fopen, hid_t, (const char *filename, unsigned flags, hid_t access_plist));
DARSHAN_FORWARD_DECL(H5Fclose, herr_t, (hid_t file_id));

struct hdf5_file_runtime
{
    struct darshan_hdf5_file* file_record;
    UT_hash_handle hlink;
};

struct hdf5_runtime
{
    struct hdf5_file_runtime* file_runtime_array;
    struct darshan_hdf5_file* file_record_array;
    int file_array_size;
    int file_array_ndx;
    struct posix_file_runtime* hid_hash;
};

static struct hdf5_runtime *hdf5_runtime = NULL;
static pthread_mutex_t hdf5_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int instrumentation_disabled = 0;
static int my_rank = -1;

static void hdf5_runtime_initialize(void);

static void hdf5_begin_shutdown(void);
static void posix_get_output_data(MPI_Comm mod_comm, darshan_record_id *shared_recs,
    int shared_rec_count, void **hdf5_buf, int *hdf5_buf_sz);
static void hdf5_shutdown(void);

#define HDF5_LOCK() pthread_mutex_lock(&hdf5_runtime_mutex)
#define HDF5_UNLOCK() pthread_mutex_unlock(&hdf5_runtime_mutex)

/*********************************************************
 *        Wrappers for HDF5 functions of interest        * 
 *********************************************************/

hid_t DARSHAN_DECL(H5Fcreate)(const char *filename, unsigned flags,
    hid_t create_plist, hid_t access_plist)
{
    int ret;
    struct darshan_file_runtime* file;
    char* tmp;
    double tm1;

#if 0
    MAP_OR_FAIL(H5Fcreate);

    tm1 = darshan_wtime();
    ret = __real_H5Fcreate(filename, flags, create_plist, access_plist);
    if(ret >= 0)
    {
        CP_LOCK();
        /* use ROMIO approach to strip prefix if present */
        /* strip off prefix if there is one, but only skip prefixes
         * if they are greater than length one to allow for windows
         * drive specifications (e.g. c:\...) 
         */
        tmp = strchr(filename, ':');
        if (tmp > filename + 1) {
            filename = tmp + 1;
        }

        file = darshan_file_by_name_sethid(filename, ret);
        if(file)
        {
            if(CP_F_VALUE(file, CP_F_OPEN_TIMESTAMP) == 0)
                CP_F_SET(file, CP_F_OPEN_TIMESTAMP,
                tm1);
            CP_INC(file, CP_HDF5_OPENS, 1);
        }
        CP_UNLOCK();
    }
#endif

    return(ret);
}

hid_t DARSHAN_DECL(H5Fopen)(const char *filename, unsigned flags,
    hid_t access_plist)
{
    int ret;
    struct darshan_file_runtime* file;
    char* tmp;
    double tm1;

#if 0
    MAP_OR_FAIL(H5Fopen);

    tm1 = darshan_wtime();
    ret = __real_H5Fopen(filename, flags, access_plist);
    if(ret >= 0)
    {
        CP_LOCK();
        /* use ROMIO approach to strip prefix if present */
        /* strip off prefix if there is one, but only skip prefixes
         * if they are greater than length one to allow for windows
         * drive specifications (e.g. c:\...) 
         */
        tmp = strchr(filename, ':');
        if (tmp > filename + 1) {
            filename = tmp + 1;
        }

        file = darshan_file_by_name_sethid(filename, ret);
        if(file)
        {
            if(CP_F_VALUE(file, CP_F_OPEN_TIMESTAMP) == 0)
                CP_F_SET(file, CP_F_OPEN_TIMESTAMP,
                tm1);
            CP_INC(file, CP_HDF5_OPENS, 1);
        }

        CP_UNLOCK();
    }
#endif

    return(ret);

}

herr_t DARSHAN_DECL(H5Fclose)(hid_t file_id)
{
    struct darshan_file_runtime* file;
    int ret;

#if 0
    MAP_OR_FAIL(H5Fclose);

    ret = __real_H5Fclose(file_id);

    CP_LOCK();
    file = darshan_file_by_hid(file_id);
    if(file)
    {
        CP_F_SET(file, CP_F_CLOSE_TIMESTAMP, PMPI_Wtime());
        darshan_file_close_hid(file_id);
    }
    CP_UNLOCK();

#endif
    return(ret);

}

/*********************************************************
 * Internal functions for manipulating HDF5 module state *
 *********************************************************/


/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
