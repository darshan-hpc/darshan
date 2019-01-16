/*
 * Copyright (C) 2015 University of Chicago.
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

#include "darshan.h"
#include "darshan-dynamic.h"

/* hope this doesn't change any time soon */
typedef int herr_t;     //hf5-1.10.0p1: H5public.h:126

#ifdef __DARSHAN_ENABLE_HDF5110
  typedef int64_t hid_t;  //hf5-1.10.0p1: H5Ipublic.h:56
#else
  typedef int hid_t;
#endif

DARSHAN_FORWARD_DECL(H5Fcreate, hid_t, (const char *filename, unsigned flags, hid_t create_plist, hid_t access_plist));
DARSHAN_FORWARD_DECL(H5Fopen, hid_t, (const char *filename, unsigned flags, hid_t access_plist));
DARSHAN_FORWARD_DECL(H5Fclose, herr_t, (hid_t file_id));

/* prototype for HDF symbols that we will call directly from within other
 * wrappers if HDF is linked in
 */
extern herr_t H5get_libversion(unsigned *majnum, unsigned *minnum, unsigned *relnum);

/* structure that can track i/o stats for a given HDF5 file record at runtime */
struct hdf5_file_record_ref
{
    struct darshan_hdf5_file* file_rec;
};

/* struct to encapsulate runtime state for the HDF5 module */
struct hdf5_runtime
{
    void *rec_id_hash;
    void *hid_hash;
    int file_rec_count;
};

static void hdf5_runtime_initialize(
    void);
static struct hdf5_file_record_ref *hdf5_track_new_file_record(
    darshan_record_id rec_id, const char *path);
static void hdf5_record_reduction_op(
    void* infile_v, void* inoutfile_v, int *len, MPI_Datatype *datatype);
static void hdf5_cleanup_runtime(
    void);

static void hdf5_shutdown(
    MPI_Comm mod_comm, darshan_record_id *shared_recs,
    int shared_rec_count, void **hdf5_buf, int *hdf5_buf_sz);

static struct hdf5_runtime *hdf5_runtime = NULL;
static pthread_mutex_t hdf5_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int my_rank = -1;

#define HDF5_LOCK() pthread_mutex_lock(&hdf5_runtime_mutex)
#define HDF5_UNLOCK() pthread_mutex_unlock(&hdf5_runtime_mutex)

#define HDF5_PRE_RECORD() do { \
    HDF5_LOCK(); \
    if(!darshan_core_disabled_instrumentation()) { \
        if(!hdf5_runtime) hdf5_runtime_initialize(); \
        if(hdf5_runtime) break; \
    } \
    HDF5_UNLOCK(); \
    return(ret); \
} while(0)

#define HDF5_POST_RECORD() do { \
    HDF5_UNLOCK(); \
} while(0)

#define HDF5_RECORD_OPEN(__ret, __path, __tm1, __tm2) do { \
    darshan_record_id rec_id; \
    struct hdf5_file_record_ref *rec_ref; \
    char *newpath; \
    newpath = darshan_clean_file_path(__path); \
    if(!newpath) newpath = (char *)__path; \
    if(darshan_core_excluded_path(newpath)) { \
        if(newpath != __path) free(newpath); \
        break; \
    } \
    rec_id = darshan_core_gen_record_id(newpath); \
    rec_ref = darshan_lookup_record_ref(hdf5_runtime->rec_id_hash, &rec_id, sizeof(darshan_record_id)); \
    if(!rec_ref) rec_ref = hdf5_track_new_file_record(rec_id, newpath); \
    if(!rec_ref) { \
        if(newpath != __path) free(newpath); \
        break; \
    } \
    if(rec_ref->file_rec->fcounters[HDF5_F_OPEN_START_TIMESTAMP] == 0 || \
     rec_ref->file_rec->fcounters[HDF5_F_OPEN_START_TIMESTAMP] > __tm1) \
        rec_ref->file_rec->fcounters[HDF5_F_OPEN_START_TIMESTAMP] = __tm1; \
    rec_ref->file_rec->fcounters[HDF5_F_OPEN_END_TIMESTAMP] = __tm2; \
    rec_ref->file_rec->counters[HDF5_OPENS] += 1; \
    darshan_add_record_ref(&(hdf5_runtime->hid_hash), &__ret, sizeof(hid_t), rec_ref); \
    if(newpath != __path) free(newpath); \
} while(0)

/*********************************************************
 *        Wrappers for HDF5 functions of interest        * 
 *********************************************************/

hid_t DARSHAN_DECL(H5Fcreate)(const char *filename, unsigned flags,
    hid_t create_plist, hid_t access_plist)
{
    hid_t ret;
    char* tmp;
    double tm1, tm2;
    unsigned majnum, minnum, relnum;

    H5get_libversion(&majnum, &minnum, &relnum);
#ifdef __DARSHAN_ENABLE_HDF5110
    if(majnum < 1 || (majnum == 1 && minnum < 10))
#else
    if(majnum > 1 || (majnum == 1 && minnum >= 10))
#endif
    {
        if(my_rank < 0)
            MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
        if(my_rank == 0)
        {
            darshan_core_fprintf(stderr, "Darshan HDF5 module error: runtime library version does not match Darshan module.\n");
        }
        return(-1);
    }

    MAP_OR_FAIL(H5Fcreate);

    tm1 = darshan_core_wtime();
    ret = __real_H5Fcreate(filename, flags, create_plist, access_plist);
    tm2 = darshan_core_wtime();
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

        HDF5_PRE_RECORD();
        HDF5_RECORD_OPEN(ret, filename, tm1, tm2);
        HDF5_POST_RECORD();
    }

    return(ret);
}

hid_t DARSHAN_DECL(H5Fopen)(const char *filename, unsigned flags,
    hid_t access_plist)
{
    hid_t ret;
    char* tmp;
    double tm1, tm2;
    unsigned majnum, minnum, relnum;

    H5get_libversion(&majnum, &minnum, &relnum);
#ifdef __DARSHAN_ENABLE_HDF5110
    if(majnum < 1 || (majnum == 1 && minnum < 10))
#else
    if(majnum > 1 || (majnum == 1 && minnum >= 10))
#endif
    {
        if(my_rank < 0)
            MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
        if(my_rank == 0)
        {
            darshan_core_fprintf(stderr, "Darshan HDF5 module error: runtime library version does not match Darshan module.\n");
        }
        return(-1);
    }

    MAP_OR_FAIL(H5Fopen);

    tm1 = darshan_core_wtime();
    ret = __real_H5Fopen(filename, flags, access_plist);
    tm2 = darshan_core_wtime();
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

        HDF5_PRE_RECORD();
        HDF5_RECORD_OPEN(ret, filename, tm1, tm2);
        HDF5_POST_RECORD();
    }

    return(ret);

}

herr_t DARSHAN_DECL(H5Fclose)(hid_t file_id)
{
    struct hdf5_file_record_ref *rec_ref;
    double tm1, tm2;
    herr_t ret;

    MAP_OR_FAIL(H5Fclose);

    tm1 = darshan_core_wtime();
    ret = __real_H5Fclose(file_id);
    tm2 = darshan_core_wtime();

    HDF5_PRE_RECORD();
    rec_ref = darshan_lookup_record_ref(hdf5_runtime->hid_hash,
        &file_id, sizeof(hid_t));
    if(rec_ref)
    {
        if(rec_ref->file_rec->fcounters[HDF5_F_CLOSE_START_TIMESTAMP] == 0 ||
         rec_ref->file_rec->fcounters[HDF5_F_CLOSE_START_TIMESTAMP] > tm1)
           rec_ref->file_rec->fcounters[HDF5_F_CLOSE_START_TIMESTAMP] = tm1;
        rec_ref->file_rec->fcounters[HDF5_F_CLOSE_END_TIMESTAMP] = tm2;
        darshan_delete_record_ref(&(hdf5_runtime->hid_hash),
            &file_id, sizeof(hid_t));
    }
    HDF5_POST_RECORD();

    return(ret);

}

/*********************************************************
 * Internal functions for manipulating HDF5 module state *
 *********************************************************/

/* initialize internal HDF5 module data strucutres and register with darshan-core */
static void hdf5_runtime_initialize()
{
    int hdf5_buf_size;

    /* try and store the default number of records for this module */
    hdf5_buf_size = DARSHAN_DEF_MOD_REC_COUNT * sizeof(struct darshan_hdf5_file);

    /* register hdf5 module with darshan-core */
    darshan_core_register_module(
        DARSHAN_HDF5_MOD,
        &hdf5_shutdown,
        &hdf5_buf_size,
        &my_rank,
        NULL);

    /* return if darshan-core does not provide enough module memory */
    if(hdf5_buf_size < sizeof(struct darshan_hdf5_file))
    {
        darshan_core_unregister_module(DARSHAN_HDF5_MOD);
        return;
    }

    hdf5_runtime = malloc(sizeof(*hdf5_runtime));
    if(!hdf5_runtime)
    {
        darshan_core_unregister_module(DARSHAN_HDF5_MOD);
        return;
    }
    memset(hdf5_runtime, 0, sizeof(*hdf5_runtime));

    return;
}

static struct hdf5_file_record_ref *hdf5_track_new_file_record(
    darshan_record_id rec_id, const char *path)
{
    struct darshan_hdf5_file *file_rec = NULL;
    struct hdf5_file_record_ref *rec_ref = NULL;
    int ret;

    rec_ref = malloc(sizeof(*rec_ref));
    if(!rec_ref)
        return(NULL);
    memset(rec_ref, 0, sizeof(*rec_ref));

    /* add a reference to this file record based on record id */
    ret = darshan_add_record_ref(&(hdf5_runtime->rec_id_hash), &rec_id,
        sizeof(darshan_record_id), rec_ref);
    if(ret == 0)
    {
        free(rec_ref);
        return(NULL);
    }

    /* register the actual file record with darshan-core so it is persisted
     * in the log file
     */
    file_rec = darshan_core_register_record(
        rec_id,
        path,
        DARSHAN_HDF5_MOD,
        sizeof(struct darshan_hdf5_file),
        NULL);

    if(!file_rec)
    {
        darshan_delete_record_ref(&(hdf5_runtime->rec_id_hash),
            &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        return(NULL);
    }

    /* registering this file record was successful, so initialize some fields */
    file_rec->base_rec.id = rec_id;
    file_rec->base_rec.rank = my_rank;
    rec_ref->file_rec = file_rec;
    hdf5_runtime->file_rec_count++;

    return(rec_ref);
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
        tmp_file.base_rec.id = infile->base_rec.id;
        tmp_file.base_rec.rank = -1;

        /* sum */
        for(j=HDF5_OPENS; j<=HDF5_OPENS; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }

        /* min non-zero (if available) value */
        for(j=HDF5_F_OPEN_START_TIMESTAMP; j<=HDF5_F_CLOSE_START_TIMESTAMP; j++)
        {
            if((infile->fcounters[j] < inoutfile->fcounters[j] &&
               infile->fcounters[j] > 0) || inoutfile->fcounters[j] == 0) 
                tmp_file.fcounters[j] = infile->fcounters[j];
            else
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
        }

        /* max */
        for(j=HDF5_F_OPEN_END_TIMESTAMP; j<=HDF5_F_CLOSE_END_TIMESTAMP; j++)
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

static void hdf5_cleanup_runtime()
{
    darshan_clear_record_refs(&(hdf5_runtime->hid_hash), 0);
    darshan_clear_record_refs(&(hdf5_runtime->rec_id_hash), 1);

    free(hdf5_runtime);
    hdf5_runtime = NULL;

    return;
}

/************************************************************************
 * Functions exported by HDF5 module for coordinating with darshan-core *
 ************************************************************************/

static void hdf5_shutdown(
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count,
    void **hdf5_buf,
    int *hdf5_buf_sz)
{
    struct hdf5_file_record_ref *rec_ref;
    struct darshan_hdf5_file *hdf5_rec_buf = *(struct darshan_hdf5_file **)hdf5_buf;
    int hdf5_rec_count;
    struct darshan_hdf5_file *red_send_buf = NULL;
    struct darshan_hdf5_file *red_recv_buf = NULL;
    MPI_Datatype red_type;
    MPI_Op red_op;
    int i;

    HDF5_LOCK();
    assert(hdf5_runtime);

    hdf5_rec_count = hdf5_runtime->file_rec_count;

    /* if there are globally shared files, do a shared file reduction */
    /* NOTE: the shared file reduction is also skipped if the 
     * DARSHAN_DISABLE_SHARED_REDUCTION environment variable is set.
     */
    if(shared_rec_count && !getenv("DARSHAN_DISABLE_SHARED_REDUCTION"))
    {
        /* necessary initialization of shared records */
        for(i = 0; i < shared_rec_count; i++)
        {
            rec_ref = darshan_lookup_record_ref(hdf5_runtime->rec_id_hash,
                &shared_recs[i], sizeof(darshan_record_id));
            assert(rec_ref);

            rec_ref->file_rec->base_rec.rank = -1;
        }

        /* sort the array of records so we get all of the shared records
         * (marked by rank -1) in a contiguous portion at end of the array
         */
        darshan_record_sort(hdf5_rec_buf, hdf5_rec_count,
            sizeof(struct darshan_hdf5_file));

        /* make *send_buf point to the shared files at the end of sorted array */
        red_send_buf = &(hdf5_rec_buf[hdf5_rec_count-shared_rec_count]);

        /* allocate memory for the reduction output on rank 0 */
        if(my_rank == 0)
        {
            red_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_hdf5_file));
            if(!red_recv_buf)
            {
                HDF5_UNLOCK();
                return;
            }
        }

        /* construct a datatype for a HDF5 file record.  This is serving no purpose
         * except to make sure we can do a reduction on proper boundaries
         */
        PMPI_Type_contiguous(sizeof(struct darshan_hdf5_file),
            MPI_BYTE, &red_type);
        PMPI_Type_commit(&red_type);

        /* register a HDF5 file record reduction operator */
        PMPI_Op_create(hdf5_record_reduction_op, 1, &red_op);

        /* reduce shared HDF5 file records */
        PMPI_Reduce(red_send_buf, red_recv_buf,
            shared_rec_count, red_type, red_op, 0, mod_comm);

        /* clean up reduction state */
        if(my_rank == 0)
        {
            int tmp_ndx = hdf5_rec_count - shared_rec_count;
            memcpy(&(hdf5_rec_buf[tmp_ndx]), red_recv_buf,
                shared_rec_count * sizeof(struct darshan_hdf5_file));
            free(red_recv_buf);
        }
        else
        {
            hdf5_rec_count -= shared_rec_count;
        }

        PMPI_Type_free(&red_type);
        PMPI_Op_free(&red_op);
    }

    /* update output buffer size to account for shared file reduction */
    *hdf5_buf_sz = hdf5_rec_count * sizeof(struct darshan_hdf5_file);

    /* shutdown internal structures used for instrumenting */
    hdf5_cleanup_runtime();

    HDF5_UNLOCK();
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
