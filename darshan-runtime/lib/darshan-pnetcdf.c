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
#include "darshan-pnetcdf-log-format.h"
#include "darshan-dynamic.h"

DARSHAN_FORWARD_DECL(ncmpi_create, int, (MPI_Comm comm, const char *path, int cmode, MPI_Info info, int *ncidp));
DARSHAN_FORWARD_DECL(ncmpi_open, int, (MPI_Comm comm, const char *path, int omode, MPI_Info info, int *ncidp));
DARSHAN_FORWARD_DECL(ncmpi_close, int, (int ncid));

/* structure to track i/o stats for a given PNETCDF file at runtime */
struct pnetcdf_file_runtime
{
    struct darshan_pnetcdf_file* file_record;
    UT_hash_handle hlink;
};

/* structure to associate a PNETCDF ncid with an existing file runtime structure */
struct pnetcdf_file_runtime_ref
{
    struct pnetcdf_file_runtime* file;
    int ncid;
    UT_hash_handle hlink;
};

/* necessary state for storing PNETCDF file records and coordinating with
 * darshan-core at shutdown time
 */
struct pnetcdf_runtime
{
    struct pnetcdf_file_runtime* file_runtime_array;
    struct darshan_pnetcdf_file* file_record_array;
    int file_array_size;
    int file_array_ndx;
    struct pnetcdf_file_runtime *file_hash;
    struct pnetcdf_file_runtime_ref* ncid_hash;
};

static struct pnetcdf_runtime *pnetcdf_runtime = NULL;
static pthread_mutex_t pnetcdf_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int instrumentation_disabled = 0;
static int my_rank = -1;

static void pnetcdf_runtime_initialize(void);
static struct pnetcdf_file_runtime* pnetcdf_file_by_name(const char *name);
static struct pnetcdf_file_runtime* pnetcdf_file_by_name_setncid(const char* name, int ncid);
static struct pnetcdf_file_runtime* pnetcdf_file_by_ncid(int ncid);
static void pnetcdf_file_close_ncid(int ncid);
static int pnetcdf_record_compare(const void* a, const void* b);
static void pnetcdf_record_reduction_op(void* infile_v, void* inoutfile_v,
    int *len, MPI_Datatype *datatype);

static void pnetcdf_begin_shutdown(void);
static void pnetcdf_get_output_data(MPI_Comm mod_comm, darshan_record_id *shared_recs,
    int shared_rec_count, void **pnetcdf_buf, int *pnetcdf_buf_sz);
static void pnetcdf_shutdown(void);

#define PNETCDF_LOCK() pthread_mutex_lock(&pnetcdf_runtime_mutex)
#define PNETCDF_UNLOCK() pthread_mutex_unlock(&pnetcdf_runtime_mutex)

/*********************************************************
 *      Wrappers for PNETCDF functions of interest       * 
 *********************************************************/

int DARSHAN_DECL(ncmpi_create)(MPI_Comm comm, const char *path,
    int cmode, MPI_Info info, int *ncidp)
{
    int ret;
    struct pnetcdf_file_runtime* file;
    char* tmp;
    int comm_size;
    double tm1;

    MAP_OR_FAIL(ncmpi_create);

    tm1 = darshan_core_wtime();
    ret = __real_ncmpi_create(comm, path, cmode, info, ncidp);
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

        PNETCDF_LOCK();
        pnetcdf_runtime_initialize();
        file = pnetcdf_file_by_name_setncid(path, (*ncidp));
        if(file)
        {
            if(file->file_record->fcounters[PNETCDF_F_OPEN_TIMESTAMP] == 0)
                file->file_record->fcounters[PNETCDF_F_OPEN_TIMESTAMP] = tm1;
            DARSHAN_MPI_CALL(PMPI_Comm_size)(comm, &comm_size);
            if(comm_size == 1)
            {
                file->file_record->counters[PNETCDF_INDEP_OPENS] += 1;
            }
            else
            {
                file->file_record->counters[PNETCDF_COLL_OPENS] += 1;
            }
        }
        PNETCDF_UNLOCK();
    }

    return(ret);
}

int DARSHAN_DECL(ncmpi_open)(MPI_Comm comm, const char *path,
    int omode, MPI_Info info, int *ncidp)
{
    int ret;
    struct pnetcdf_file_runtime* file;
    char* tmp;
    int comm_size;
    double tm1;

    MAP_OR_FAIL(ncmpi_open);

    tm1 = darshan_core_wtime();
    ret = __real_ncmpi_open(comm, path, omode, info, ncidp);
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

        PNETCDF_LOCK();
        pnetcdf_runtime_initialize();
        file = pnetcdf_file_by_name_setncid(path, (*ncidp));
        if(file)
        {
            if(file->file_record->fcounters[PNETCDF_F_OPEN_TIMESTAMP] == 0)
                file->file_record->fcounters[PNETCDF_F_OPEN_TIMESTAMP] = tm1;
            DARSHAN_MPI_CALL(PMPI_Comm_size)(comm, &comm_size);
            if(comm_size == 1)
            {
                file->file_record->counters[PNETCDF_INDEP_OPENS] += 1;
            }
            else
            {
                file->file_record->counters[PNETCDF_COLL_OPENS] += 1;
            }
        }
        PNETCDF_UNLOCK();
    }

    return(ret);
}

int DARSHAN_DECL(ncmpi_close)(int ncid)
{
    struct pnetcdf_file_runtime* file;
    int ret;

    MAP_OR_FAIL(ncmpi_close);

    ret = __real_ncmpi_close(ncid);

    PNETCDF_LOCK();
    pnetcdf_runtime_initialize();
    file = pnetcdf_file_by_ncid(ncid);
    if(file)
    {
        file->file_record->fcounters[PNETCDF_F_CLOSE_TIMESTAMP] =
            darshan_core_wtime();
        pnetcdf_file_close_ncid(ncid);
    }
    PNETCDF_UNLOCK();

    return(ret);
}

/************************************************************
 * Internal functions for manipulating PNETCDF module state *
 ************************************************************/

/* initialize internal PNETCDF module data strucutres and register with darshan-core */
static void pnetcdf_runtime_initialize()
{
    int mem_limit;
    struct darshan_module_funcs pnetcdf_mod_fns =
    {
        .begin_shutdown = &pnetcdf_begin_shutdown,
        .get_output_data = &pnetcdf_get_output_data,
        .shutdown = &pnetcdf_shutdown
    };

    /* don't do anything if already initialized or instrumenation is disabled */
    if(pnetcdf_runtime || instrumentation_disabled)
        return;

    /* register pnetcdf module with darshan-core */
    darshan_core_register_module(
        DARSHAN_PNETCDF_MOD,
        &pnetcdf_mod_fns,
        &my_rank,
        &mem_limit,
        NULL);

    /* return if no memory assigned by darshan-core */
    if(mem_limit == 0)
        return;

    pnetcdf_runtime = malloc(sizeof(*pnetcdf_runtime));
    if(!pnetcdf_runtime)
        return;
    memset(pnetcdf_runtime, 0, sizeof(*pnetcdf_runtime));

    /* set maximum number of file records according to max memory limit */
    /* NOTE: maximum number of records is based on the size of a pnetcdf file record */
    /* TODO: should we base memory usage off file record or total runtime structure sizes? */
    pnetcdf_runtime->file_array_size = mem_limit / sizeof(struct darshan_pnetcdf_file);
    pnetcdf_runtime->file_array_ndx = 0;

    /* allocate array of runtime file records */
    pnetcdf_runtime->file_runtime_array = malloc(pnetcdf_runtime->file_array_size *
                                                 sizeof(struct pnetcdf_file_runtime));
    pnetcdf_runtime->file_record_array = malloc(pnetcdf_runtime->file_array_size *
                                                sizeof(struct darshan_pnetcdf_file));
    if(!pnetcdf_runtime->file_runtime_array || !pnetcdf_runtime->file_record_array)
    {
        pnetcdf_runtime->file_array_size = 0;
        return;
    }
    memset(pnetcdf_runtime->file_runtime_array, 0, pnetcdf_runtime->file_array_size *
           sizeof(struct pnetcdf_file_runtime));
    memset(pnetcdf_runtime->file_record_array, 0, pnetcdf_runtime->file_array_size *
           sizeof(struct darshan_pnetcdf_file));

    return;
}

/* get a PNETCDF file record for the given file path */
static struct pnetcdf_file_runtime* pnetcdf_file_by_name(const char *name)
{
    struct pnetcdf_file_runtime *file = NULL;
    char *newname = NULL;
    darshan_record_id file_id;

    if(!pnetcdf_runtime || instrumentation_disabled)
        return(NULL);

    newname = darshan_clean_file_path(name);
    if(!newname)
        newname = (char*)name;

    /* get a unique id for this file from darshan core */
    darshan_core_register_record(
        (void*)newname,
        strlen(newname),
        1,
        DARSHAN_PNETCDF_MOD,
        &file_id,
        NULL);

    /* search the hash table for this file record, and return if found */
    HASH_FIND(hlink, pnetcdf_runtime->file_hash, &file_id, sizeof(darshan_record_id), file);
    if(file)
    {
        if(newname != name)
            free(newname);
        return(file);
    }

    if(pnetcdf_runtime->file_array_ndx < pnetcdf_runtime->file_array_size);
    {
        /* no existing record, assign a new file record from the global array */
        file = &(pnetcdf_runtime->file_runtime_array[pnetcdf_runtime->file_array_ndx]);
        file->file_record = &(pnetcdf_runtime->file_record_array[pnetcdf_runtime->file_array_ndx]);
        file->file_record->f_id = file_id;
        file->file_record->rank = my_rank;

        /* add new record to file hash table */
        HASH_ADD(hlink, pnetcdf_runtime->file_hash, file_record->f_id, sizeof(darshan_record_id), file);

        pnetcdf_runtime->file_array_ndx++;
    }

    if(newname != name)
        free(newname);
    return(file);
}

/* get a PNETCDF file record for the given file path, and also create a
 * reference structure using the returned ncid
 */
static struct pnetcdf_file_runtime* pnetcdf_file_by_name_setncid(const char* name, int ncid)
{
    struct pnetcdf_file_runtime* file;
    struct pnetcdf_file_runtime_ref* ref;

    if(!pnetcdf_runtime || instrumentation_disabled)
        return(NULL);

    /* find file record by name first */
    file = pnetcdf_file_by_name(name);

    if(!file)
        return(NULL);

    /* search hash table for existing file ref for this ncid */
    HASH_FIND(hlink, pnetcdf_runtime->ncid_hash, &ncid, sizeof(int), ref);
    if(ref)
    {
        /* we have a reference.  Make sure it points to the correct file
         * and return it
         */
        ref->file = file;
        return(file);
    }

    /* if we hit this point, then we don't have a reference for this ncid
     * in the table yet.  Add it.
     */
    ref = malloc(sizeof(*ref));
    if(!ref)
        return(NULL);
    memset(ref, 0, sizeof(*ref));

    ref->file = file;
    ref->ncid = ncid;
    HASH_ADD(hlink, pnetcdf_runtime->ncid_hash, ncid, sizeof(int), ref);

    return(file);
}

/* get a PNETCDF file record for the given ncid */
static struct pnetcdf_file_runtime* pnetcdf_file_by_ncid(int ncid)
{
    struct pnetcdf_file_runtime_ref* ref;

    if(!pnetcdf_runtime || instrumentation_disabled)
        return(NULL);

    /* search hash table for existing file ref for this ncid */
    HASH_FIND(hlink, pnetcdf_runtime->ncid_hash, &ncid, sizeof(int), ref);
    if(ref)
        return(ref->file);

    return(NULL);
}

/* free up PNETCDF reference data structures for the given ncid */
static void pnetcdf_file_close_ncid(int ncid)
{
    struct pnetcdf_file_runtime_ref* ref;

    if(!pnetcdf_runtime || instrumentation_disabled)
        return;

    /* search hash table for this ncid */
    HASH_FIND(hlink, pnetcdf_runtime->ncid_hash, &ncid, sizeof(int), ref);
    if(ref)
    {
        /* we have a reference, delete it */
        HASH_DELETE(hlink, pnetcdf_runtime->ncid_hash, ref);
        free(ref);
    }

    return;
}

/* compare function for sorting file records by descending rank */
static int pnetcdf_record_compare(const void* a_p, const void* b_p)
{
    const struct darshan_pnetcdf_file* a = a_p;
    const struct darshan_pnetcdf_file* b = b_p;

    if(a->rank < b->rank)
        return 1;
    if(a->rank > b->rank)
        return -1;

    return 0;
}

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
        tmp_file.f_id = infile->f_id;
        tmp_file.rank = -1;

        /* sum */
        for(j=PNETCDF_INDEP_OPENS; j<=PNETCDF_COLL_OPENS; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }

        /* min non-zero (if available) value */
        for(j=PNETCDF_F_OPEN_TIMESTAMP; j<=PNETCDF_F_OPEN_TIMESTAMP; j++)
        {
            if(infile->fcounters[j] > inoutfile->fcounters[j] && inoutfile->fcounters[j] > 0)
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
            else
                tmp_file.fcounters[j] = infile->fcounters[j];
        }

        /* max */
        for(j=PNETCDF_F_CLOSE_TIMESTAMP; j<=PNETCDF_F_CLOSE_TIMESTAMP; j++)
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

/***************************************************************************
 * Functions exported by PNETCDF module for coordinating with darshan-core *
 ***************************************************************************/

static void pnetcdf_begin_shutdown()
{
    assert(pnetcdf_runtime);

    PNETCDF_LOCK();
    /* disable further instrumentation while Darshan shuts down */
    instrumentation_disabled = 1;
    PNETCDF_UNLOCK();

    return;
}

static void pnetcdf_get_output_data(
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count,
    void **pnetcdf_buf,
    int *pnetcdf_buf_sz)
{
    struct pnetcdf_file_runtime *file;
    int i;
    struct darshan_pnetcdf_file *red_send_buf = NULL;
    struct darshan_pnetcdf_file *red_recv_buf = NULL;
    MPI_Datatype red_type;
    MPI_Op red_op;

    assert(pnetcdf_runtime);

    /* if there are globally shared files, do a shared file reduction */
    /* NOTE: the shared file reduction is also skipped if the 
     * DARSHAN_DISABLE_SHARED_REDUCTION environment variable is set.
     */
    if(shared_rec_count && !getenv("DARSHAN_DISABLE_SHARED_REDUCTION"))
    {
        /* necessary initialization of shared records */
        for(i = 0; i < shared_rec_count; i++)
        {
            HASH_FIND(hlink, pnetcdf_runtime->file_hash, &shared_recs[i],
                sizeof(darshan_record_id), file);
            assert(file);

            file->file_record->rank = -1;
        }

        /* sort the array of files descending by rank so that we get all of the 
         * shared files (marked by rank -1) in a contiguous portion at end 
         * of the array
         */
        qsort(pnetcdf_runtime->file_record_array, pnetcdf_runtime->file_array_ndx,
            sizeof(struct darshan_pnetcdf_file), pnetcdf_record_compare);

        /* make *send_buf point to the shared files at the end of sorted array */
        red_send_buf =
            &(pnetcdf_runtime->file_record_array[pnetcdf_runtime->file_array_ndx-shared_rec_count]);

        /* allocate memory for the reduction output on rank 0 */
        if(my_rank == 0)
        {
            red_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_pnetcdf_file));
            if(!red_recv_buf)
                return;
        }

        /* construct a datatype for a PNETCDF file record.  This is serving no purpose
         * except to make sure we can do a reduction on proper boundaries
         */
        DARSHAN_MPI_CALL(PMPI_Type_contiguous)(sizeof(struct darshan_pnetcdf_file),
            MPI_BYTE, &red_type);
        DARSHAN_MPI_CALL(PMPI_Type_commit)(&red_type);

        /* register a PNETCDF file record reduction operator */
        DARSHAN_MPI_CALL(PMPI_Op_create)(pnetcdf_record_reduction_op, 1, &red_op);

        /* reduce shared PNETCDF file records */
        DARSHAN_MPI_CALL(PMPI_Reduce)(red_send_buf, red_recv_buf,
            shared_rec_count, red_type, red_op, 0, mod_comm);

        /* clean up reduction state */
        if(my_rank == 0)
        {
            int tmp_ndx = pnetcdf_runtime->file_array_ndx - shared_rec_count;
            memcpy(&(pnetcdf_runtime->file_record_array[tmp_ndx]), red_recv_buf,
                shared_rec_count * sizeof(struct darshan_pnetcdf_file));
            free(red_recv_buf);
        }
        else
        {
            pnetcdf_runtime->file_array_ndx -= shared_rec_count;
        }

        DARSHAN_MPI_CALL(PMPI_Type_free)(&red_type);
        DARSHAN_MPI_CALL(PMPI_Op_free)(&red_op);
    }

    *pnetcdf_buf = (void *)(pnetcdf_runtime->file_record_array);
    *pnetcdf_buf_sz = pnetcdf_runtime->file_array_ndx * sizeof(struct darshan_pnetcdf_file);

    return;
}

static void pnetcdf_shutdown()
{
    struct pnetcdf_file_runtime_ref *ref, *tmp;

    assert(pnetcdf_runtime);

    HASH_ITER(hlink, pnetcdf_runtime->ncid_hash, ref, tmp)
    {
        HASH_DELETE(hlink, pnetcdf_runtime->ncid_hash, ref);
        free(ref);
    }

    HASH_CLEAR(hlink, pnetcdf_runtime->file_hash); /* these entries are freed all at once below */

    free(pnetcdf_runtime->file_runtime_array);
    free(pnetcdf_runtime->file_record_array);
    free(pnetcdf_runtime);
    pnetcdf_runtime = NULL;

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
