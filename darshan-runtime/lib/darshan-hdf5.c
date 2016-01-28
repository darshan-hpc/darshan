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
#include "darshan-dynamic.h"

/* hope this doesn't change any time soon */
typedef int hid_t;
typedef int herr_t;

DARSHAN_FORWARD_DECL(H5Fcreate, hid_t, (const char *filename, unsigned flags, hid_t create_plist, hid_t access_plist));
DARSHAN_FORWARD_DECL(H5Fopen, hid_t, (const char *filename, unsigned flags, hid_t access_plist));
DARSHAN_FORWARD_DECL(H5Fclose, herr_t, (hid_t file_id));

/* structure to track i/o stats for a given hdf5 file at runtime */
struct hdf5_file_runtime
{
    struct darshan_hdf5_file* file_record;
    UT_hash_handle hlink;
};

/* structure to associate a HDF5 hid with an existing file runtime structure */
struct hdf5_file_runtime_ref
{
    struct hdf5_file_runtime* file;
    hid_t hid;
    UT_hash_handle hlink;
};

/* necessary state for storing HDF5 file records and coordinating with
 * darshan-core at shutdown time
 */
struct hdf5_runtime
{
    struct hdf5_file_runtime* file_runtime_array;
    struct darshan_hdf5_file* file_record_array;
    int file_array_size;
    int file_array_ndx;
    struct hdf5_file_runtime *file_hash;
    struct hdf5_file_runtime_ref* hid_hash;
};

static struct hdf5_runtime *hdf5_runtime = NULL;
static pthread_mutex_t hdf5_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int instrumentation_disabled = 0;
static int my_rank = -1;

static void hdf5_runtime_initialize(void);
static struct hdf5_file_runtime* hdf5_file_by_name(const char *name);
static struct hdf5_file_runtime* hdf5_file_by_name_sethid(const char* name, hid_t hid);
static struct hdf5_file_runtime* hdf5_file_by_hid(hid_t hid);
static void hdf5_file_close_hid(hid_t hid);
static int hdf5_record_compare(const void* a, const void* b);
static void hdf5_record_reduction_op(void* infile_v, void* inoutfile_v,
    int *len, MPI_Datatype *datatype);

static void hdf5_begin_shutdown(void);
static void hdf5_get_output_data(MPI_Comm mod_comm, darshan_record_id *shared_recs,
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
    struct hdf5_file_runtime* file;
    char* tmp;
    double tm1;

    MAP_OR_FAIL(H5Fcreate);

    tm1 = darshan_core_wtime();
    ret = __real_H5Fcreate(filename, flags, create_plist, access_plist);
    if(ret >= 0)
    {
        /* use ROMIO approach to strip prefix if present */
        /* strip off prefix if there is one, but only skip prefixes
         * if they are greater than length one to allow for windows
         * drive specifications (e.g. c:\...) 
         */
        tmp = strchr(filename, ':');
        if (tmp > filename + 1) {
            filename = tmp + 1;
        }

        HDF5_LOCK();
        hdf5_runtime_initialize();
        file = hdf5_file_by_name_sethid(filename, ret);
        if(file)
        {
            if(file->file_record->fcounters[HDF5_F_OPEN_TIMESTAMP] == 0)
                file->file_record->fcounters[HDF5_F_OPEN_TIMESTAMP] = tm1;
            file->file_record->counters[HDF5_OPENS] += 1;
        }
        HDF5_UNLOCK();
    }

    return(ret);
}

hid_t DARSHAN_DECL(H5Fopen)(const char *filename, unsigned flags,
    hid_t access_plist)
{
    int ret;
    struct hdf5_file_runtime* file;
    char* tmp;
    double tm1;

    MAP_OR_FAIL(H5Fopen);

    tm1 = darshan_core_wtime();
    ret = __real_H5Fopen(filename, flags, access_plist);
    if(ret >= 0)
    {
        /* use ROMIO approach to strip prefix if present */
        /* strip off prefix if there is one, but only skip prefixes
         * if they are greater than length one to allow for windows
         * drive specifications (e.g. c:\...) 
         */
        tmp = strchr(filename, ':');
        if (tmp > filename + 1) {
            filename = tmp + 1;
        }

        HDF5_LOCK();
        hdf5_runtime_initialize();
        file = hdf5_file_by_name_sethid(filename, ret);
        if(file)
        {
            if(file->file_record->fcounters[HDF5_F_OPEN_TIMESTAMP] == 0)
                file->file_record->fcounters[HDF5_F_OPEN_TIMESTAMP] = tm1;
            file->file_record->counters[HDF5_OPENS] += 1;
        }
        HDF5_UNLOCK();
    }

    return(ret);

}

herr_t DARSHAN_DECL(H5Fclose)(hid_t file_id)
{
    struct hdf5_file_runtime* file;
    int ret;

    MAP_OR_FAIL(H5Fclose);

    ret = __real_H5Fclose(file_id);

    HDF5_LOCK();
    hdf5_runtime_initialize();
    file = hdf5_file_by_hid(file_id);
    if(file)
    {
        file->file_record->fcounters[HDF5_F_CLOSE_TIMESTAMP] =
            darshan_core_wtime();
        hdf5_file_close_hid(file_id);
    }
    HDF5_UNLOCK();

    return(ret);

}

/*********************************************************
 * Internal functions for manipulating HDF5 module state *
 *********************************************************/

/* initialize internal HDF5 module data strucutres and register with darshan-core */
static void hdf5_runtime_initialize()
{
    int mem_limit;
    struct darshan_module_funcs hdf5_mod_fns =
    {
        .begin_shutdown = &hdf5_begin_shutdown,
        .get_output_data = &hdf5_get_output_data,
        .shutdown = &hdf5_shutdown
    };

    /* don't do anything if already initialized or instrumenation is disabled */
    if(hdf5_runtime || instrumentation_disabled)
        return;

    /* register hdf5 module with darshan-core */
    darshan_core_register_module(
        DARSHAN_HDF5_MOD,
        &hdf5_mod_fns,
        &my_rank,
        &mem_limit,
        NULL);

    /* return if no memory assigned by darshan-core */
    if(mem_limit == 0)
        return;

    hdf5_runtime = malloc(sizeof(*hdf5_runtime));
    if(!hdf5_runtime)
        return;
    memset(hdf5_runtime, 0, sizeof(*hdf5_runtime));

    /* set maximum number of file records according to max memory limit */
    /* NOTE: maximum number of records is based on the size of a hdf5 file record */
    /* TODO: should we base memory usage off file record or total runtime structure sizes? */
    hdf5_runtime->file_array_size = mem_limit / sizeof(struct darshan_hdf5_file);
    hdf5_runtime->file_array_ndx = 0;

    /* allocate array of runtime file records */
    hdf5_runtime->file_runtime_array = malloc(hdf5_runtime->file_array_size *
                                              sizeof(struct hdf5_file_runtime));
    hdf5_runtime->file_record_array = malloc(hdf5_runtime->file_array_size *
                                             sizeof(struct darshan_hdf5_file));
    if(!hdf5_runtime->file_runtime_array || !hdf5_runtime->file_record_array)
    {
        hdf5_runtime->file_array_size = 0;
        return;
    }
    memset(hdf5_runtime->file_runtime_array, 0, hdf5_runtime->file_array_size *
           sizeof(struct hdf5_file_runtime));
    memset(hdf5_runtime->file_record_array, 0, hdf5_runtime->file_array_size *
           sizeof(struct darshan_hdf5_file));

    return;
}

/* get a HDF5 file record for the given file path */
static struct hdf5_file_runtime* hdf5_file_by_name(const char *name)
{
    struct hdf5_file_runtime *file = NULL;
    char *newname = NULL;
    darshan_record_id file_id;
    int limit_flag;

    if(!hdf5_runtime || instrumentation_disabled)
        return(NULL);

    newname = darshan_clean_file_path(name);
    if(!newname)
        newname = (char*)name;

    limit_flag = (hdf5_runtime->file_array_ndx >= hdf5_runtime->file_array_size);

    /* get a unique id for this file from darshan core */
    darshan_core_register_record(
        (void*)newname,
        strlen(newname),
        DARSHAN_HDF5_MOD,
        1,
        limit_flag,
        &file_id,
        NULL);

    /* if record is set to 0, darshan-core is out of space and will not
     * track this record, so we should avoid tracking it, too
     */
    if(file_id == 0)
    {
        if(newname != name)
            free(newname);
        return(NULL);
    }

    /* search the hash table for this file record, and return if found */
    HASH_FIND(hlink, hdf5_runtime->file_hash, &file_id, sizeof(darshan_record_id), file);
    if(file)
    {
        if(newname != name)
            free(newname);
        return(file);
    }

    /* no existing record, assign a new file record from the global array */
    file = &(hdf5_runtime->file_runtime_array[hdf5_runtime->file_array_ndx]);
    file->file_record = &(hdf5_runtime->file_record_array[hdf5_runtime->file_array_ndx]);
    file->file_record->f_id = file_id;
    file->file_record->rank = my_rank;

    /* add new record to file hash table */
    HASH_ADD(hlink, hdf5_runtime->file_hash, file_record->f_id, sizeof(darshan_record_id), file);
    hdf5_runtime->file_array_ndx++;

    if(newname != name)
        free(newname);
    return(file);
}

/* get a HDF5 file record for the given file path, and also create a
 * reference structure using the returned hid
 */
static struct hdf5_file_runtime* hdf5_file_by_name_sethid(const char* name, hid_t hid)
{
    struct hdf5_file_runtime* file;
    struct hdf5_file_runtime_ref* ref;

    if(!hdf5_runtime || instrumentation_disabled)
        return(NULL);

    /* find file record by name first */
    file = hdf5_file_by_name(name);

    if(!file)
        return(NULL);

    /* search hash table for existing file ref for this fd */
    HASH_FIND(hlink, hdf5_runtime->hid_hash, &hid, sizeof(hid_t), ref);
    if(ref)
    {
        /* we have a reference.  Make sure it points to the correct file
         * and return it
         */
        ref->file = file;
        return(file);
    }

    /* if we hit this point, then we don't have a reference for this fd
     * in the table yet.  Add it.
     */
    ref = malloc(sizeof(*ref));
    if(!ref)
        return(NULL);
    memset(ref, 0, sizeof(*ref));

    ref->file = file;
    ref->hid = hid;
    HASH_ADD(hlink, hdf5_runtime->hid_hash, hid, sizeof(hid_t), ref);

    return(file);
}

/* get a HDF5 file record for the given hid */
static struct hdf5_file_runtime* hdf5_file_by_hid(hid_t hid)
{
    struct hdf5_file_runtime_ref* ref;

    if(!hdf5_runtime || instrumentation_disabled)
        return(NULL);

    /* search hash table for existing file ref for this hid */
    HASH_FIND(hlink, hdf5_runtime->hid_hash, &hid, sizeof(hid_t), ref);
    if(ref)
        return(ref->file);

    return(NULL);
}

/* free up HDF5 reference data structures for the given hid */
static void hdf5_file_close_hid(hid_t hid)
{
    struct hdf5_file_runtime_ref* ref;

    if(!hdf5_runtime || instrumentation_disabled)
        return;

    /* search hash table for this hid */
    HASH_FIND(hlink, hdf5_runtime->hid_hash, &hid, sizeof(hid_t), ref);
    if(ref)
    {
        /* we have a reference, delete it */
        HASH_DELETE(hlink, hdf5_runtime->hid_hash, ref);
        free(ref);
    }

    return;
}

/* compare function for sorting file records by descending rank */
static int hdf5_record_compare(const void* a_p, const void* b_p)
{
    const struct darshan_hdf5_file* a = a_p;
    const struct darshan_hdf5_file* b = b_p;

    if(a->rank < b->rank)
        return 1;
    if(a->rank > b->rank)
        return -1;

    return 0;
}

static void hdf5_record_reduction_op(void* infile_v, void* inoutfile_v,
    int *len, MPI_Datatype *datatype)
{
    struct darshan_hdf5_file tmp_file;
    struct darshan_hdf5_file *infile = infile_v;
    struct darshan_hdf5_file *inoutfile = inoutfile_v;
    int i, j;

    assert(hdf5_runtime);

    for(i=0; i<*len; i++)
    {
        memset(&tmp_file, 0, sizeof(struct darshan_hdf5_file));
        tmp_file.f_id = infile->f_id;
        tmp_file.rank = -1;

        /* sum */
        for(j=HDF5_OPENS; j<=HDF5_OPENS; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }

        /* min non-zero (if available) value */
        for(j=HDF5_F_OPEN_TIMESTAMP; j<=HDF5_F_OPEN_TIMESTAMP; j++)
        {
            if(infile->fcounters[j] > inoutfile->fcounters[j] && inoutfile->fcounters[j] > 0)
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
            else
                tmp_file.fcounters[j] = infile->fcounters[j];
        }

        /* max */
        for(j=HDF5_F_CLOSE_TIMESTAMP; j<=HDF5_F_CLOSE_TIMESTAMP; j++)
        {
            if(infile->fcounters[j] > inoutfile->fcounters[j])
                tmp_file.fcounters[j] = infile->fcounters[j];
            else
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
        }

        /* update pointers */
        *inoutfile = tmp_file;
        inoutfile++;
        infile++;
    }

    return;
}

/************************************************************************
 * Functions exported by HDF5 module for coordinating with darshan-core *
 ************************************************************************/

static void hdf5_begin_shutdown()
{
    assert(hdf5_runtime);

    HDF5_LOCK();
    /* disable further instrumentation while Darshan shuts down */
    instrumentation_disabled = 1;
    HDF5_UNLOCK();

    return;
}

static void hdf5_get_output_data(
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count,
    void **hdf5_buf,
    int *hdf5_buf_sz)
{
    struct hdf5_file_runtime *file;
    int i;
    struct darshan_hdf5_file *red_send_buf = NULL;
    struct darshan_hdf5_file *red_recv_buf = NULL;
    MPI_Datatype red_type;
    MPI_Op red_op;

    assert(hdf5_runtime);

    /* if there are globally shared files, do a shared file reduction */
    /* NOTE: the shared file reduction is also skipped if the 
     * DARSHAN_DISABLE_SHARED_REDUCTION environment variable is set.
     */
    if(shared_rec_count && !getenv("DARSHAN_DISABLE_SHARED_REDUCTION"))
    {
        /* necessary initialization of shared records */
        for(i = 0; i < shared_rec_count; i++)
        {
            HASH_FIND(hlink, hdf5_runtime->file_hash, &shared_recs[i],
                sizeof(darshan_record_id), file);
            assert(file);

            file->file_record->rank = -1;
        }

        /* sort the array of files descending by rank so that we get all of the 
         * shared files (marked by rank -1) in a contiguous portion at end 
         * of the array
         */
        qsort(hdf5_runtime->file_record_array, hdf5_runtime->file_array_ndx,
            sizeof(struct darshan_hdf5_file), hdf5_record_compare);

        /* make *send_buf point to the shared files at the end of sorted array */
        red_send_buf =
            &(hdf5_runtime->file_record_array[hdf5_runtime->file_array_ndx-shared_rec_count]);

        /* allocate memory for the reduction output on rank 0 */
        if(my_rank == 0)
        {
            red_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_hdf5_file));
            if(!red_recv_buf)
            {
                return;
            }
        }

        /* construct a datatype for a HDF5 file record.  This is serving no purpose
         * except to make sure we can do a reduction on proper boundaries
         */
        DARSHAN_MPI_CALL(PMPI_Type_contiguous)(sizeof(struct darshan_hdf5_file),
            MPI_BYTE, &red_type);
        DARSHAN_MPI_CALL(PMPI_Type_commit)(&red_type);

        /* register a HDF5 file record reduction operator */
        DARSHAN_MPI_CALL(PMPI_Op_create)(hdf5_record_reduction_op, 1, &red_op);

        /* reduce shared HDF5 file records */
        DARSHAN_MPI_CALL(PMPI_Reduce)(red_send_buf, red_recv_buf,
            shared_rec_count, red_type, red_op, 0, mod_comm);

        /* clean up reduction state */
        if(my_rank == 0)
        {
            int tmp_ndx = hdf5_runtime->file_array_ndx - shared_rec_count;
            memcpy(&(hdf5_runtime->file_record_array[tmp_ndx]), red_recv_buf,
                shared_rec_count * sizeof(struct darshan_hdf5_file));
            free(red_recv_buf);
        }
        else
        {
            hdf5_runtime->file_array_ndx -= shared_rec_count;
        }

        DARSHAN_MPI_CALL(PMPI_Type_free)(&red_type);
        DARSHAN_MPI_CALL(PMPI_Op_free)(&red_op);
    }

    *hdf5_buf = (void *)(hdf5_runtime->file_record_array);
    *hdf5_buf_sz = hdf5_runtime->file_array_ndx * sizeof(struct darshan_hdf5_file);

    return;
}

static void hdf5_shutdown()
{
    struct hdf5_file_runtime_ref *ref, *tmp;

    assert(hdf5_runtime);

    darshan_core_unregister_module(DARSHAN_HDF5_MOD);

    HASH_ITER(hlink, hdf5_runtime->hid_hash, ref, tmp)
    {
        HASH_DELETE(hlink, hdf5_runtime->hid_hash, ref);
        free(ref);
    }

    HASH_CLEAR(hlink, hdf5_runtime->file_hash); /* these entries are freed all at once below */

    free(hdf5_runtime->file_runtime_array);
    free(hdf5_runtime->file_record_array);
    free(hdf5_runtime);
    hdf5_runtime = NULL;

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
