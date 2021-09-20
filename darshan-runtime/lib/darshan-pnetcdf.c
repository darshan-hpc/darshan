/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifdef HAVE_CONFIG_H
# include <darshan-runtime-config.h>
#endif

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

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

DARSHAN_FORWARD_DECL(ncmpi_create, int, (MPI_Comm comm, const char *path, int cmode, MPI_Info info, int *ncidp));
DARSHAN_FORWARD_DECL(ncmpi_open, int, (MPI_Comm comm, const char *path, int omode, MPI_Info info, int *ncidp));
DARSHAN_FORWARD_DECL(ncmpi_close, int, (int ncid));

/* structure that can track i/o stats for a given PNETCDF file record at runtime */
struct pnetcdf_file_record_ref
{
    struct darshan_pnetcdf_file* file_rec;
};

/* struct to encapsulate runtime state for the PNETCDF module */
struct pnetcdf_runtime
{
    void *rec_id_hash;
    void *ncid_hash;
    int file_rec_count;
};

static void pnetcdf_runtime_initialize(
    void);
static struct pnetcdf_file_record_ref *pnetcdf_track_new_file_record(
    darshan_record_id rec_id, const char *path);
#ifdef HAVE_MPI
static void pnetcdf_record_reduction_op(
    void* infile_v, void* inoutfile_v, int *len, MPI_Datatype *datatype);
static void pnetcdf_mpi_redux(
    void *pnetcdf_buf, MPI_Comm mod_comm,
    darshan_record_id *shared_recs, int shared_rec_count);
#endif
static void pnetcdf_output(
    void **pnetcdf_buf, int *pnetcdf_buf_sz);
static void pnetcdf_cleanup(
    void);

static struct pnetcdf_runtime *pnetcdf_runtime = NULL;
static pthread_mutex_t pnetcdf_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int my_rank = -1;

#define PNETCDF_LOCK() pthread_mutex_lock(&pnetcdf_runtime_mutex)
#define PNETCDF_UNLOCK() pthread_mutex_unlock(&pnetcdf_runtime_mutex)

#define PNETCDF_PRE_RECORD() do { \
    PNETCDF_LOCK(); \
    if(!darshan_core_disabled_instrumentation()) { \
        if(!pnetcdf_runtime) pnetcdf_runtime_initialize(); \
        if(pnetcdf_runtime) break; \
    } \
    PNETCDF_UNLOCK(); \
    return(ret); \
} while(0)

#define PNETCDF_POST_RECORD() do { \
    PNETCDF_UNLOCK(); \
} while(0)

#define PNETCDF_RECORD_OPEN(__ncidp, __path, __comm, __tm1, __tm2) do { \
    darshan_record_id rec_id; \
    struct pnetcdf_file_record_ref *rec_ref; \
    char *newpath; \
    int comm_size; \
    newpath = darshan_clean_file_path(__path); \
    if(!newpath) newpath = (char *)__path; \
    if(darshan_core_excluded_path(newpath)) { \
        if(newpath != __path) free(newpath); \
        break; \
    } \
    rec_id = darshan_core_gen_record_id(newpath); \
    rec_ref = darshan_lookup_record_ref(pnetcdf_runtime->rec_id_hash, &rec_id, sizeof(darshan_record_id)); \
    if(!rec_ref) rec_ref = pnetcdf_track_new_file_record(rec_id, newpath); \
    if(!rec_ref) { \
        if(newpath != __path) free(newpath); \
        break; \
    } \
    PMPI_Comm_size(__comm, &comm_size); \
    if(rec_ref->file_rec->fcounters[PNETCDF_F_OPEN_START_TIMESTAMP] == 0 || \
     rec_ref->file_rec->fcounters[PNETCDF_F_OPEN_START_TIMESTAMP] > __tm1) \
        rec_ref->file_rec->fcounters[PNETCDF_F_OPEN_START_TIMESTAMP] = __tm1; \
    rec_ref->file_rec->fcounters[PNETCDF_F_OPEN_END_TIMESTAMP] = __tm2; \
    if(comm_size == 1) rec_ref->file_rec->counters[PNETCDF_INDEP_OPENS] += 1; \
    else rec_ref->file_rec->counters[PNETCDF_COLL_OPENS] += 1; \
    darshan_add_record_ref(&(pnetcdf_runtime->ncid_hash), __ncidp, sizeof(int), rec_ref); \
    if(newpath != __path) free(newpath); \
} while(0)

/*********************************************************
 *      Wrappers for PNETCDF functions of interest       * 
 *********************************************************/

int DARSHAN_DECL(ncmpi_create)(MPI_Comm comm, const char *path,
    int cmode, MPI_Info info, int *ncidp)
{
    int ret;
    char* tmp;
    double tm1, tm2;

    MAP_OR_FAIL(ncmpi_create);

    tm1 = darshan_core_wtime();
    ret = __real_ncmpi_create(comm, path, cmode, info, ncidp);
    tm2 = darshan_core_wtime();
    if(ret == 0)
    {
        /* use ROMIO approach to strip prefix if present */
        /* strip off prefix if there is one, but only skip prefixes
         * if they are greater than length one to allow for windows
         * drive specifications (e.g. c:\...) 
         */
        tmp = strchr(path, ':');
        if (tmp > path + 1) {
            path = tmp + 1;
        }

        PNETCDF_PRE_RECORD();
        PNETCDF_RECORD_OPEN(ncidp, path, comm, tm1, tm2);
        PNETCDF_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(ncmpi_open)(MPI_Comm comm, const char *path,
    int omode, MPI_Info info, int *ncidp)
{
    int ret;
    char* tmp;
    double tm1, tm2;

    MAP_OR_FAIL(ncmpi_open);

    tm1 = darshan_core_wtime();
    ret = __real_ncmpi_open(comm, path, omode, info, ncidp);
    tm2 = darshan_core_wtime();
    if(ret == 0)
    {
        /* use ROMIO approach to strip prefix if present */
        /* strip off prefix if there is one, but only skip prefixes
         * if they are greater than length one to allow for windows
         * drive specifications (e.g. c:\...) 
         */
        tmp = strchr(path, ':');
        if (tmp > path + 1) {
            path = tmp + 1;
        }

        PNETCDF_PRE_RECORD();
        PNETCDF_RECORD_OPEN(ncidp, path, comm, tm1, tm2);
        PNETCDF_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(ncmpi_close)(int ncid)
{
    struct pnetcdf_file_record_ref *rec_ref;
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(ncmpi_close);

    tm1 = darshan_core_wtime();
    ret = __real_ncmpi_close(ncid);
    tm2 = darshan_core_wtime();

    PNETCDF_PRE_RECORD();
    rec_ref = darshan_lookup_record_ref(pnetcdf_runtime->ncid_hash,
        &ncid, sizeof(int));
    if(rec_ref)
    {
        if(rec_ref->file_rec->fcounters[PNETCDF_F_CLOSE_START_TIMESTAMP] == 0 ||
         rec_ref->file_rec->fcounters[PNETCDF_F_CLOSE_START_TIMESTAMP] > tm1)
           rec_ref->file_rec->fcounters[PNETCDF_F_CLOSE_START_TIMESTAMP] = tm1;
        rec_ref->file_rec->fcounters[PNETCDF_F_CLOSE_END_TIMESTAMP] = tm2;
        darshan_delete_record_ref(&(pnetcdf_runtime->ncid_hash),
            &ncid, sizeof(int));
    }
    PNETCDF_POST_RECORD();

    return(ret);
}

/************************************************************
 * Internal functions for manipulating PNETCDF module state *
 ************************************************************/

/* initialize internal PNETCDF module data strucutres and register with darshan-core */
static void pnetcdf_runtime_initialize()
{
    size_t pnetcdf_buf_size;
    darshan_module_funcs mod_funcs = {
#ifdef HAVE_MPI
    .mod_redux_func = &pnetcdf_mpi_redux,
#endif
    .mod_output_func = &pnetcdf_output,
    .mod_cleanup_func = &pnetcdf_cleanup
    };

    /* try and store the default number of records for this module */
    pnetcdf_buf_size = DARSHAN_DEF_MOD_REC_COUNT * sizeof(struct darshan_pnetcdf_file);

    /* register pnetcdf module with darshan-core */
    darshan_core_register_module(
        DARSHAN_PNETCDF_MOD,
        mod_funcs,
        &pnetcdf_buf_size,
        &my_rank,
        NULL);

    pnetcdf_runtime = malloc(sizeof(*pnetcdf_runtime));
    if(!pnetcdf_runtime)
    {
        darshan_core_unregister_module(DARSHAN_PNETCDF_MOD);
        return;
    }
    memset(pnetcdf_runtime, 0, sizeof(*pnetcdf_runtime));

    return;
}

static struct pnetcdf_file_record_ref *pnetcdf_track_new_file_record(
    darshan_record_id rec_id, const char *path)
{
    struct darshan_pnetcdf_file *file_rec = NULL;
    struct pnetcdf_file_record_ref *rec_ref = NULL;
    int ret;

    rec_ref = malloc(sizeof(*rec_ref));
    if(!rec_ref)
        return(NULL);
    memset(rec_ref, 0, sizeof(*rec_ref));

    /* add a reference to this file record based on record id */
    ret = darshan_add_record_ref(&(pnetcdf_runtime->rec_id_hash), &rec_id,
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
        DARSHAN_PNETCDF_MOD,
        sizeof(struct darshan_pnetcdf_file),
        NULL);

    if(!file_rec)
    {
        darshan_delete_record_ref(&(pnetcdf_runtime->rec_id_hash),
            &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        return(NULL);
    }

    /* registering this file record was successful, so initialize some fields */
    file_rec->base_rec.id = rec_id;
    file_rec->base_rec.rank = my_rank;
    rec_ref->file_rec = file_rec;
    pnetcdf_runtime->file_rec_count++;

    return(rec_ref);
}

#ifdef HAVE_MPI
static void pnetcdf_record_reduction_op(void* infile_v, void* inoutfile_v,
    int *len, MPI_Datatype *datatype)
{
    struct darshan_pnetcdf_file tmp_file;
    struct darshan_pnetcdf_file *infile = infile_v;
    struct darshan_pnetcdf_file *inoutfile = inoutfile_v;
    int i, j;

    assert(pnetcdf_runtime);

    for(i=0; i<*len; i++)
    {
        memset(&tmp_file, 0, sizeof(struct darshan_pnetcdf_file));
        tmp_file.base_rec.id = infile->base_rec.id;
        tmp_file.base_rec.rank = -1;

        /* sum */
        for(j=PNETCDF_INDEP_OPENS; j<=PNETCDF_COLL_OPENS; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }

        /* min non-zero (if available) value */
        for(j=PNETCDF_F_OPEN_START_TIMESTAMP; j<=PNETCDF_F_CLOSE_START_TIMESTAMP; j++)
        {
            if((infile->fcounters[j] < inoutfile->fcounters[j] &&
               infile->fcounters[j] > 0) || inoutfile->fcounters[j] == 0) 
                tmp_file.fcounters[j] = infile->fcounters[j];
            else
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
        }

        /* max */
        for(j=PNETCDF_F_OPEN_END_TIMESTAMP; j<=PNETCDF_F_CLOSE_END_TIMESTAMP; j++)
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
#endif

/***************************************************************************
 * Functions exported by PNETCDF module for coordinating with darshan-core *
 ***************************************************************************/

#ifdef HAVE_MPI
static void pnetcdf_mpi_redux(
    void *pnetcdf_buf,
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count)
{
    int pnetcdf_rec_count;
    struct pnetcdf_file_record_ref *rec_ref;
    struct darshan_pnetcdf_file *pnetcdf_rec_buf = (struct darshan_pnetcdf_file *)pnetcdf_buf;
    struct darshan_pnetcdf_file *red_send_buf = NULL;
    struct darshan_pnetcdf_file *red_recv_buf = NULL;
    MPI_Datatype red_type;
    MPI_Op red_op;
    int i;

    PNETCDF_LOCK();
    assert(pnetcdf_runtime);

    pnetcdf_rec_count = pnetcdf_runtime->file_rec_count;

    /* necessary initialization of shared records */
    for(i = 0; i < shared_rec_count; i++)
    {
        rec_ref = darshan_lookup_record_ref(pnetcdf_runtime->rec_id_hash,
            &shared_recs[i], sizeof(darshan_record_id));
        assert(rec_ref);

        rec_ref->file_rec->base_rec.rank = -1;
    }

    /* sort the array of records so we get all of the shared records
     * (marked by rank -1) in a contiguous portion at end of the array
     */
    darshan_record_sort(pnetcdf_rec_buf, pnetcdf_rec_count,
        sizeof(struct darshan_pnetcdf_file));

    /* make *send_buf point to the shared files at the end of sorted array */
    red_send_buf = &(pnetcdf_rec_buf[pnetcdf_rec_count-shared_rec_count]);

    /* allocate memory for the reduction output on rank 0 */
    if(my_rank == 0)
    {
        red_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_pnetcdf_file));
        if(!red_recv_buf)
        {
            PNETCDF_UNLOCK();
            return;
        }
    }

    /* construct a datatype for a PNETCDF file record.  This is serving no purpose
     * except to make sure we can do a reduction on proper boundaries
     */
    PMPI_Type_contiguous(sizeof(struct darshan_pnetcdf_file),
        MPI_BYTE, &red_type);
    PMPI_Type_commit(&red_type);

    /* register a PNETCDF file record reduction operator */
    PMPI_Op_create(pnetcdf_record_reduction_op, 1, &red_op);

    /* reduce shared PNETCDF file records */
    PMPI_Reduce(red_send_buf, red_recv_buf,
        shared_rec_count, red_type, red_op, 0, mod_comm);

    /* update module state to account for shared file reduction */
    if(my_rank == 0)
    {
        /* overwrite local shared records with globally reduced records */
        int tmp_ndx = pnetcdf_rec_count - shared_rec_count;
        memcpy(&(pnetcdf_rec_buf[tmp_ndx]), red_recv_buf,
            shared_rec_count * sizeof(struct darshan_pnetcdf_file));
        free(red_recv_buf);
    }
    else
    {
        /* drop shared records on non-zero ranks */
        pnetcdf_runtime->file_rec_count -= shared_rec_count;
    }

    PMPI_Type_free(&red_type);
    PMPI_Op_free(&red_op);

    PNETCDF_UNLOCK();
    return;
}
#endif

static void pnetcdf_output(
    void **pnetcdf_buf,
    int *pnetcdf_buf_sz)
{
    int pnetcdf_rec_count;

    PNETCDF_LOCK();
    assert(pnetcdf_runtime);

    /* just pass back our updated total buffer size -- no need to update buffer */
    pnetcdf_rec_count = pnetcdf_runtime->file_rec_count;
    *pnetcdf_buf_sz = pnetcdf_rec_count * sizeof(struct darshan_pnetcdf_file);

    PNETCDF_UNLOCK();
    return;
}

static void pnetcdf_cleanup()
{
    PNETCDF_LOCK();
    assert(pnetcdf_runtime);

    /* cleanup internal structures used for instrumenting */
    darshan_clear_record_refs(&(pnetcdf_runtime->ncid_hash), 0);
    darshan_clear_record_refs(&(pnetcdf_runtime->rec_id_hash), 1);

    free(pnetcdf_runtime);
    pnetcdf_runtime = NULL;

    PNETCDF_UNLOCK();
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
