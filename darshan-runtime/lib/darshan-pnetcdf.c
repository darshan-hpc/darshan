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

#include <pnetcdf.h>

/* structure that can track i/o stats for a given PnetCDF file record at runtime */
struct pnetcdf_file_record_ref
{
    struct darshan_pnetcdf_file* file_rec;
    double last_meta_end;
    double last_wait_end;
};

/* structure that can track i/o stats for a given PnetCDF variable record at runtime */
struct pnetcdf_var_record_ref
{
    struct darshan_pnetcdf_var* var_rec;
    enum darshan_io_type last_io_type;
    double last_read_end;
    double last_write_end;
    double last_meta_end;
    void *access_root;
    int access_count;
    int unlimdimid;
};

/* struct to encapsulate runtime state for the PnetCDF file module */
struct pnetcdf_file_runtime
{
    void *rec_id_hash;
    void *ncid_hash;
    int rec_count;
    int frozen; /* flag to indicate that the counters should no longer be modified */
};

/* struct to encapsulate runtime state for the PnetCDF var module */
struct pnetcdf_var_runtime
{
    void *rec_id_hash;
    void *varid_hash;
    int rec_count;
    int frozen; /* flag to indicate that the counters should no longer be modified */
};

static void pnetcdf_file_runtime_initialize(void);
static void pnetcdf_var_runtime_initialize(void);
static struct pnetcdf_file_record_ref *pnetcdf_file_track_new_record(
    darshan_record_id rec_id, const char *path);
static struct pnetcdf_var_record_ref *pnetcdf_var_track_new_record(
    darshan_record_id rec_id, const char *path);
static void pnetcdf_var_finalize_records(
    void *rec_ref_p, void *user_ptr);
static void pnetcdf_file_record_reduction_op(
    void* infile_v, void* inoutfile_v, int *len, MPI_Datatype *datatype);
static void pnetcdf_var_record_reduction_op(
    void* inrec_v, void* inoutrec_v, int *len, MPI_Datatype *datatype);
static void pnetcdf_var_shared_record_variance(
    MPI_Comm mod_comm, struct darshan_pnetcdf_var *inrec_array,
    struct darshan_pnetcdf_var *outrec_array, int shared_rec_count);
static void pnetcdf_file_mpi_redux(
    void *pnetcdf_buf, MPI_Comm mod_comm,
    darshan_record_id *shared_recs, int shared_rec_count);
static void pnetcdf_var_mpi_redux(
    void *pnetcdf_buf, MPI_Comm mod_comm,
    darshan_record_id *shared_recs, int shared_rec_count);
static void pnetcdf_file_output(
    void **pnetcdf_buf, int *pnetcdf_buf_sz);
static void pnetcdf_var_output(
    void **pnetcdf_buf, int *pnetcdf_buf_sz);
static void pnetcdf_file_cleanup(void);
static void pnetcdf_var_cleanup(void);

static struct pnetcdf_file_runtime *pnetcdf_file_runtime = NULL;
static int pnetcdf_file_runtime_init_attempted = 0;
static struct pnetcdf_var_runtime *pnetcdf_var_runtime = NULL;
static int pnetcdf_var_runtime_init_attempted = 0;

static pthread_mutex_t pnetcdf_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int my_rank = -1;

#define PNETCDF_LOCK() pthread_mutex_lock(&pnetcdf_runtime_mutex)
#define PNETCDF_UNLOCK() pthread_mutex_unlock(&pnetcdf_runtime_mutex)

#define PNETCDF_WTIME() \
    __darshan_disabled ? 0 : darshan_core_wtime();

/* note that if the break condition is triggered in this macro, then it
 * will exit the do/while loop holding a lock that will be released in
 * POST_RECORD().  Otherwise it will release the lock here (if held) and
 * return immediately without reaching the POST_RECORD() macro.
 */
#define PNETCDF_FILE_PRE_RECORD() do { \
    if(!__darshan_disabled) { \
        PNETCDF_LOCK(); \
        if(!pnetcdf_file_runtime && !pnetcdf_file_runtime_init_attempted) \
            pnetcdf_file_runtime_initialize(); \
        if(pnetcdf_file_runtime && !pnetcdf_file_runtime->frozen) break; \
        PNETCDF_UNLOCK(); \
    } \
    return(ret); \
} while(0)

#define PNETCDF_FILE_POST_RECORD() do { \
    PNETCDF_UNLOCK(); \
} while(0)

#define DARSHAN_PNETCDF_VAR_DELIM ":"

/* note that if the break condition is triggered in this macro, then it
 * will exit the do/while loop holding a lock that will be released in
 * POST_RECORD().  Otherwise it will release the lock here (if held) and
 * return immediately without reaching the POST_RECORD() macro.
 */
#define PNETCDF_VAR_PRE_RECORD() do { \
    if(!__darshan_disabled) { \
        PNETCDF_LOCK(); \
        if(!pnetcdf_var_runtime && !pnetcdf_var_runtime_init_attempted) \
            pnetcdf_var_runtime_initialize(); \
        if(pnetcdf_var_runtime && !pnetcdf_var_runtime->frozen) break; \
        PNETCDF_UNLOCK(); \
    } \
    return(ret); \
} while(0)

#define PNETCDF_VAR_POST_RECORD() do { \
    PNETCDF_UNLOCK(); \
} while(0)

/*********************************************************
 *      Wrappers for PnetCDF functions of interest       *
 *********************************************************/
#include "darshan-pnetcdf-api.c"

/************************************************************
 * Internal functions for manipulating PnetCDF module state *
 ************************************************************/

/* initialize internal PnetCDF module data structures and register with darshan-core */
static void pnetcdf_file_runtime_initialize()
{
    int ret;
    size_t pnetcdf_rec_count;
    darshan_module_funcs mod_funcs = {
    .mod_redux_func = &pnetcdf_file_mpi_redux,
    .mod_output_func = &pnetcdf_file_output,
    .mod_cleanup_func = &pnetcdf_file_cleanup
    };

    /* if this attempt at initializing fails, we won't try again */
    pnetcdf_file_runtime_init_attempted = 1;

    /* try and store a default number of records for this module */
    pnetcdf_rec_count = DARSHAN_DEF_MOD_REC_COUNT;

    /* register PnetCDF module with darshan-core */
    ret = darshan_core_register_module(
        DARSHAN_PNETCDF_FILE_MOD,
        mod_funcs,
        sizeof(struct darshan_pnetcdf_file),
        &pnetcdf_rec_count,
        &my_rank,
        NULL);
    if(ret < 0)
        return;

    pnetcdf_file_runtime = (struct pnetcdf_file_runtime*) calloc(1, sizeof(struct pnetcdf_file_runtime));
    if(!pnetcdf_file_runtime)
    {
        darshan_core_unregister_module(DARSHAN_PNETCDF_FILE_MOD);
        return;
    }

    return;
}

/* initialize internal PnetCDF module data structures and register with darshan-core */
static void pnetcdf_var_runtime_initialize()
{
    int ret;
    size_t pnetcdf_rec_count;
    darshan_module_funcs mod_funcs = {
    .mod_redux_func = &pnetcdf_var_mpi_redux,
    .mod_output_func = &pnetcdf_var_output,
    .mod_cleanup_func = &pnetcdf_var_cleanup
    };

    /* if this attempt at initializing fails, we won't try again */
    pnetcdf_var_runtime_init_attempted = 1;

    /* try and store the default number of records for this module */
    pnetcdf_rec_count = DARSHAN_DEF_MOD_REC_COUNT;

    /* register PnetCDF module with darshan-core */
    ret = darshan_core_register_module(
        DARSHAN_PNETCDF_VAR_MOD,
        mod_funcs,
        sizeof(struct darshan_pnetcdf_var),
        &pnetcdf_rec_count,
        &my_rank,
        NULL);
    if(ret < 0)
        return;

    pnetcdf_var_runtime = (struct pnetcdf_var_runtime*) calloc(1, sizeof(struct pnetcdf_var_runtime));
    if(!pnetcdf_var_runtime)
    {
        darshan_core_unregister_module(DARSHAN_PNETCDF_VAR_MOD);
        return;
    }

    return;
}

static struct pnetcdf_file_record_ref *pnetcdf_file_track_new_record(
    darshan_record_id rec_id, const char *path)
{
    struct darshan_pnetcdf_file *file_rec = NULL;
    struct pnetcdf_file_record_ref *rec_ref = NULL;
    int ret;

    rec_ref = (struct pnetcdf_file_record_ref*) calloc(1, sizeof(struct pnetcdf_file_record_ref));
    if(!rec_ref)
        return(NULL);

    /* add a reference to this file record based on record id */
    ret = darshan_add_record_ref(&(pnetcdf_file_runtime->rec_id_hash), &rec_id,
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
        DARSHAN_PNETCDF_FILE_MOD,
        sizeof(struct darshan_pnetcdf_file),
        NULL);

    if(!file_rec)
    {
        darshan_delete_record_ref(&(pnetcdf_file_runtime->rec_id_hash),
            &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        return(NULL);
    }

    /* registering this file record was successful, so initialize some fields */
    file_rec->base_rec.id = rec_id;
    file_rec->base_rec.rank = my_rank;
    rec_ref->file_rec = file_rec;
    pnetcdf_file_runtime->rec_count++;

    return(rec_ref);
}

static struct pnetcdf_var_record_ref *pnetcdf_var_track_new_record(
    darshan_record_id rec_id, const char *path)
{
    struct darshan_pnetcdf_var *var_rec = NULL;
    struct pnetcdf_var_record_ref *rec_ref = NULL;
    int ret;

    rec_ref = (struct pnetcdf_var_record_ref*) calloc(1, sizeof(struct pnetcdf_var_record_ref));
    if(!rec_ref)
        return(NULL);

    /* add a reference to this variable record based on record id */
    ret = darshan_add_record_ref(&(pnetcdf_var_runtime->rec_id_hash), &rec_id,
        sizeof(darshan_record_id), rec_ref);
    if(ret == 0)
    {
        free(rec_ref);
        return(NULL);
    }

    /* register the actual variable record with darshan-core so it is persisted
     * in the log file
     */
    var_rec = darshan_core_register_record(
        rec_id,
        path,
        DARSHAN_PNETCDF_VAR_MOD,
        sizeof(struct darshan_pnetcdf_var),
        NULL);

    if(!var_rec)
    {
        darshan_delete_record_ref(&(pnetcdf_var_runtime->rec_id_hash),
            &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        return(NULL);
    }

    /* registering this variable record was successful, so initialize some fields */
    var_rec->base_rec.id = rec_id;
    var_rec->base_rec.rank = my_rank;
    rec_ref->var_rec = var_rec;
    pnetcdf_var_runtime->rec_count++;

    return(rec_ref);
}

static void pnetcdf_var_finalize_records(void *rec_ref_p, void *user_ptr)
{
    struct pnetcdf_var_record_ref *rec_ref =
        (struct pnetcdf_var_record_ref *)rec_ref_p;

    tdestroy(rec_ref->access_root, free);
    return;
}

static void pnetcdf_file_record_reduction_op(void* infile_v, void* inoutfile_v,
    int *len, MPI_Datatype *datatype)
{
    struct darshan_pnetcdf_file tmp_file;
    struct darshan_pnetcdf_file *infile = infile_v;
    struct darshan_pnetcdf_file *inoutfile = inoutfile_v;
    int i, j;

    assert(pnetcdf_file_runtime);

    for(i=0; i<*len; i++)
    {
        memset(&tmp_file, 0, sizeof(struct darshan_pnetcdf_file));
        tmp_file.base_rec.id = infile->base_rec.id;
        tmp_file.base_rec.rank = -1;

        /* sum */
        for(j=PNETCDF_FILE_CREATES; j<PNETCDF_FILE_NUM_INDICES; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }

        /* min non-zero (if available) value */
        for(j=PNETCDF_FILE_F_OPEN_START_TIMESTAMP; j<=PNETCDF_FILE_F_WAIT_START_TIMESTAMP; j++)
        {
            if((infile->fcounters[j] < inoutfile->fcounters[j] &&
               infile->fcounters[j] > 0) || inoutfile->fcounters[j] == 0)
                tmp_file.fcounters[j] = infile->fcounters[j];
            else
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
        }

        /* max */
        for(j=PNETCDF_FILE_F_OPEN_END_TIMESTAMP; j<=PNETCDF_FILE_F_WAIT_END_TIMESTAMP; j++)
        {
            if(infile->fcounters[j] > inoutfile->fcounters[j])
                tmp_file.fcounters[j] = infile->fcounters[j];
            else
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
        }

        /* sum */
        for(j=PNETCDF_FILE_F_META_TIME; j<PNETCDF_FILE_F_NUM_INDICES; j++)
        {
            tmp_file.fcounters[j] = infile->fcounters[j] + inoutfile->fcounters[j];
        }

        /* update pointers */
        *inoutfile = tmp_file;
        inoutfile++;
        infile++;
    }

    return;
}

static void pnetcdf_var_record_reduction_op(void* inrec_v, void* inoutrec_v,
    int *len, MPI_Datatype *datatype)
{
    struct darshan_pnetcdf_var tmp_var;
    struct darshan_pnetcdf_var *inrec = inrec_v;
    struct darshan_pnetcdf_var *inoutrec = inoutrec_v;
    int i, j, j2, k, k2;

    for(i=0; i<*len; i++)
    {
        memset(&tmp_var, 0, sizeof(struct darshan_pnetcdf_var));
        tmp_var.base_rec.id = inrec->base_rec.id;
        tmp_var.base_rec.rank = -1;
        tmp_var.file_rec_id = inrec->file_rec_id;

        /* sum */
        for(j=PNETCDF_VAR_OPENS; j<=PNETCDF_VAR_BPUT_VARN; j++)
        {
            tmp_var.counters[j] = inrec->counters[j] + inoutrec->counters[j];
        }

        /* skip PNETCDF_VAR_MAX_*_TIME_SIZE; handled in floating point section */

        for(j=PNETCDF_VAR_SIZE_READ_AGG_0_100; j<=PNETCDF_VAR_SIZE_WRITE_AGG_1G_PLUS; j++)
        {
            tmp_var.counters[j] = inrec->counters[j] + inoutrec->counters[j];
        }

        /* first collapse any duplicates */
        for(j=PNETCDF_VAR_ACCESS1_ACCESS, j2=PNETCDF_VAR_ACCESS1_COUNT; j<=PNETCDF_VAR_ACCESS4_ACCESS;
            j+=(PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1), j2++)
        {
            for(k=PNETCDF_VAR_ACCESS1_ACCESS, k2=PNETCDF_VAR_ACCESS1_COUNT; k<=PNETCDF_VAR_ACCESS4_ACCESS;
                k+=(PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1), k2++)
            {
                if(!memcmp(&inrec->counters[j], &inoutrec->counters[k],
                    sizeof(int64_t) * (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1)))
                {
                    memset(&inoutrec->counters[k], 0, sizeof(int64_t) *
                        (PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1));
                    inrec->counters[j2] += inoutrec->counters[k2];
                    inoutrec->counters[k2] = 0;
                }
            }
        }

        /* first set */
        for(j=PNETCDF_VAR_ACCESS1_ACCESS, j2=PNETCDF_VAR_ACCESS1_COUNT; j<=PNETCDF_VAR_ACCESS4_ACCESS;
            j+=(PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1), j2++)
        {
            DARSHAN_UPDATE_COMMON_VAL_COUNTERS(
                &(tmp_var.counters[PNETCDF_VAR_ACCESS1_ACCESS]),
                &(tmp_var.counters[PNETCDF_VAR_ACCESS1_COUNT]),
                &inrec->counters[j], PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1,
                inrec->counters[j2], 0);
        }

        /* second set */
        for(j=PNETCDF_VAR_ACCESS1_ACCESS, j2=PNETCDF_VAR_ACCESS1_COUNT; j<=PNETCDF_VAR_ACCESS4_ACCESS;
            j+=(PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1), j2++)
        {
            DARSHAN_UPDATE_COMMON_VAL_COUNTERS(
                &(tmp_var.counters[PNETCDF_VAR_ACCESS1_ACCESS]),
                &(tmp_var.counters[PNETCDF_VAR_ACCESS1_COUNT]),
                &inoutrec->counters[j], PNETCDF_VAR_MAX_NDIMS+PNETCDF_VAR_MAX_NDIMS+1,
                inoutrec->counters[j2], 0);
        }

        tmp_var.counters[PNETCDF_VAR_NDIMS] = inrec->counters[PNETCDF_VAR_NDIMS];
        tmp_var.counters[PNETCDF_VAR_NPOINTS] = inrec->counters[PNETCDF_VAR_NPOINTS];
        tmp_var.counters[PNETCDF_VAR_DATATYPE_SIZE] = inrec->counters[PNETCDF_VAR_DATATYPE_SIZE];

        if(inoutrec->counters[PNETCDF_VAR_IS_RECORD_VAR] == 1 ||
                inrec->counters[PNETCDF_VAR_IS_RECORD_VAR] == 1)
            tmp_var.counters[PNETCDF_VAR_IS_RECORD_VAR] = 1;

        /* min non-zero (if available) value */
        for(j=PNETCDF_VAR_F_OPEN_START_TIMESTAMP; j<=PNETCDF_VAR_F_CLOSE_START_TIMESTAMP; j++)
        {
            if((inrec->fcounters[j] < inoutrec->fcounters[j] &&
               inrec->fcounters[j] > 0) || inoutrec->fcounters[j] == 0)
                tmp_var.fcounters[j] = inrec->fcounters[j];
            else
                tmp_var.fcounters[j] = inoutrec->fcounters[j];
        }

        /* max */
        for(j=PNETCDF_VAR_F_OPEN_END_TIMESTAMP; j<=PNETCDF_VAR_F_CLOSE_END_TIMESTAMP; j++)
        {
            if(inrec->fcounters[j] > inoutrec->fcounters[j])
                tmp_var.fcounters[j] = inrec->fcounters[j];
            else
                tmp_var.fcounters[j] = inoutrec->fcounters[j];
        }

        /* sum */
        for(j=PNETCDF_VAR_F_READ_TIME; j<=PNETCDF_VAR_F_META_TIME; j++)
        {
            tmp_var.fcounters[j] = inrec->fcounters[j] + inoutrec->fcounters[j];
        }

        /* max (special case) */
        if(inrec->fcounters[PNETCDF_VAR_F_MAX_READ_TIME] >
            inoutrec->fcounters[PNETCDF_VAR_F_MAX_READ_TIME])
        {
            tmp_var.fcounters[PNETCDF_VAR_F_MAX_READ_TIME] =
                inrec->fcounters[PNETCDF_VAR_F_MAX_READ_TIME];
            tmp_var.counters[PNETCDF_VAR_MAX_READ_TIME_SIZE] =
                inrec->counters[PNETCDF_VAR_MAX_READ_TIME_SIZE];
        }
        else
        {
            tmp_var.fcounters[PNETCDF_VAR_F_MAX_READ_TIME] =
                inoutrec->fcounters[PNETCDF_VAR_F_MAX_READ_TIME];
            tmp_var.counters[PNETCDF_VAR_MAX_READ_TIME_SIZE] =
                inoutrec->counters[PNETCDF_VAR_MAX_READ_TIME_SIZE];
        }

        /* max (special case) */
        if(inrec->fcounters[PNETCDF_VAR_F_MAX_WRITE_TIME] >
            inoutrec->fcounters[PNETCDF_VAR_F_MAX_WRITE_TIME])
        {
            tmp_var.fcounters[PNETCDF_VAR_F_MAX_WRITE_TIME] =
                inrec->fcounters[PNETCDF_VAR_F_MAX_WRITE_TIME];
            tmp_var.counters[PNETCDF_VAR_MAX_WRITE_TIME_SIZE] =
                inrec->counters[PNETCDF_VAR_MAX_WRITE_TIME_SIZE];
        }
        else
        {
            tmp_var.fcounters[PNETCDF_VAR_F_MAX_WRITE_TIME] =
                inoutrec->fcounters[PNETCDF_VAR_F_MAX_WRITE_TIME];
            tmp_var.counters[PNETCDF_VAR_MAX_WRITE_TIME_SIZE] =
                inoutrec->counters[PNETCDF_VAR_MAX_WRITE_TIME_SIZE];
        }

        /* min (zeroes are ok here; some procs don't do I/O) */
        if(inrec->fcounters[PNETCDF_VAR_F_FASTEST_RANK_TIME] <
            inoutrec->fcounters[PNETCDF_VAR_F_FASTEST_RANK_TIME])
        {
            tmp_var.counters[PNETCDF_VAR_FASTEST_RANK] =
                inrec->counters[PNETCDF_VAR_FASTEST_RANK];
            tmp_var.counters[PNETCDF_VAR_FASTEST_RANK_BYTES] =
                inrec->counters[PNETCDF_VAR_FASTEST_RANK_BYTES];
            tmp_var.fcounters[PNETCDF_VAR_F_FASTEST_RANK_TIME] =
                inrec->fcounters[PNETCDF_VAR_F_FASTEST_RANK_TIME];
        }
        else
        {
            tmp_var.counters[PNETCDF_VAR_FASTEST_RANK] =
                inoutrec->counters[PNETCDF_VAR_FASTEST_RANK];
            tmp_var.counters[PNETCDF_VAR_FASTEST_RANK_BYTES] =
                inoutrec->counters[PNETCDF_VAR_FASTEST_RANK_BYTES];
            tmp_var.fcounters[PNETCDF_VAR_F_FASTEST_RANK_TIME] =
                inoutrec->fcounters[PNETCDF_VAR_F_FASTEST_RANK_TIME];
        }

        /* max */
        if(inrec->fcounters[PNETCDF_VAR_F_SLOWEST_RANK_TIME] >
           inoutrec->fcounters[PNETCDF_VAR_F_SLOWEST_RANK_TIME])
        {
            tmp_var.counters[PNETCDF_VAR_SLOWEST_RANK] =
                inrec->counters[PNETCDF_VAR_SLOWEST_RANK];
            tmp_var.counters[PNETCDF_VAR_SLOWEST_RANK_BYTES] =
                inrec->counters[PNETCDF_VAR_SLOWEST_RANK_BYTES];
            tmp_var.fcounters[PNETCDF_VAR_F_SLOWEST_RANK_TIME] =
                inrec->fcounters[PNETCDF_VAR_F_SLOWEST_RANK_TIME];
        }
        else
        {
            tmp_var.counters[PNETCDF_VAR_SLOWEST_RANK] =
                inoutrec->counters[PNETCDF_VAR_SLOWEST_RANK];
            tmp_var.counters[PNETCDF_VAR_SLOWEST_RANK_BYTES] =
                inoutrec->counters[PNETCDF_VAR_SLOWEST_RANK_BYTES];
            tmp_var.fcounters[PNETCDF_VAR_F_SLOWEST_RANK_TIME] =
                inoutrec->fcounters[PNETCDF_VAR_F_SLOWEST_RANK_TIME];
        }

        /* update pointers */
        *inoutrec = tmp_var;
        inoutrec++;
        inrec++;
    }

    return;
}

static void pnetcdf_var_shared_record_variance(
    MPI_Comm mod_comm, struct darshan_pnetcdf_var *inrec_array,
    struct darshan_pnetcdf_var *outrec_array, int shared_rec_count)
{
    MPI_Datatype var_dt;
    MPI_Op var_op;
    int i;
    struct darshan_variance_dt *var_send_buf = NULL;
    struct darshan_variance_dt *var_recv_buf = NULL;

    PMPI_Type_contiguous(sizeof(struct darshan_variance_dt),
        MPI_BYTE, &var_dt);
    PMPI_Type_commit(&var_dt);

    PMPI_Op_create(darshan_variance_reduce, 1, &var_op);

    var_send_buf = malloc(shared_rec_count * sizeof(struct darshan_variance_dt));
    if(!var_send_buf)
        return;

    if(my_rank == 0)
    {
        var_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_variance_dt));

        if(!var_recv_buf)
            return;
    }

    /* get total i/o time variances for shared records */

    for(i=0; i<shared_rec_count; i++)
    {
        var_send_buf[i].n = 1;
        var_send_buf[i].S = 0;
        var_send_buf[i].T = inrec_array[i].fcounters[PNETCDF_VAR_F_READ_TIME] +
                            inrec_array[i].fcounters[PNETCDF_VAR_F_WRITE_TIME] +
                            inrec_array[i].fcounters[PNETCDF_VAR_F_META_TIME];
    }

    PMPI_Reduce(var_send_buf, var_recv_buf, shared_rec_count,
        var_dt, var_op, 0, mod_comm);

    if(my_rank == 0)
    {
        for(i=0; i<shared_rec_count; i++)
        {
            outrec_array[i].fcounters[PNETCDF_VAR_F_VARIANCE_RANK_TIME] =
                (var_recv_buf[i].S / var_recv_buf[i].n);
        }
    }

    /* get total bytes moved variances for shared records */

    for(i=0; i<shared_rec_count; i++)
    {
        var_send_buf[i].n = 1;
        var_send_buf[i].S = 0;
        var_send_buf[i].T = (double)
                            inrec_array[i].counters[PNETCDF_VAR_BYTES_READ] +
                            inrec_array[i].counters[PNETCDF_VAR_BYTES_WRITTEN];
    }

    PMPI_Reduce(var_send_buf, var_recv_buf, shared_rec_count,
        var_dt, var_op, 0, mod_comm);

    if(my_rank == 0)
    {
        for(i=0; i<shared_rec_count; i++)
        {
            outrec_array[i].fcounters[PNETCDF_VAR_F_VARIANCE_RANK_BYTES] =
                (var_recv_buf[i].S / var_recv_buf[i].n);
        }
    }

    PMPI_Type_free(&var_dt);
    PMPI_Op_free(&var_op);
    free(var_send_buf);
    free(var_recv_buf);

    return;
}

/***************************************************************************
 * Functions exported by PnetCDF module for coordinating with darshan-core *
 ***************************************************************************/

static void pnetcdf_file_mpi_redux(
    void *pnetcdf_buf,
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count)
{
    int rec_count;
    struct pnetcdf_file_record_ref *rec_ref;
    struct darshan_pnetcdf_file *pnetcdf_rec_buf = (struct darshan_pnetcdf_file *)pnetcdf_buf;
    struct darshan_pnetcdf_file *red_send_buf = NULL;
    struct darshan_pnetcdf_file *red_recv_buf = NULL;
    MPI_Datatype red_type;
    MPI_Op red_op;
    int i;

    PNETCDF_LOCK();
    assert(pnetcdf_file_runtime);

    rec_count = pnetcdf_file_runtime->rec_count;

    /* necessary initialization of shared records */
    for(i = 0; i < shared_rec_count; i++)
    {
        rec_ref = darshan_lookup_record_ref(pnetcdf_file_runtime->rec_id_hash,
            &shared_recs[i], sizeof(darshan_record_id));
        assert(rec_ref);

        rec_ref->file_rec->base_rec.rank = -1;
    }

    /* sort the array of records so we get all of the shared records
     * (marked by rank -1) in a contiguous portion at end of the array
     */
    darshan_record_sort(pnetcdf_rec_buf, rec_count,
        sizeof(struct darshan_pnetcdf_file));

    /* make *send_buf point to the shared files at the end of sorted array */
    red_send_buf = &(pnetcdf_rec_buf[rec_count-shared_rec_count]);

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

    /* construct a datatype for a PnetCDF file record.  This is serving no
     * purpose except to make sure we can do a reduction on proper boundaries
     */
    PMPI_Type_contiguous(sizeof(struct darshan_pnetcdf_file), MPI_BYTE, &red_type);
    PMPI_Type_commit(&red_type);

    /* register a PnetCDF file record reduction operator */
    PMPI_Op_create(pnetcdf_file_record_reduction_op, 1, &red_op);

    /* reduce shared PnetCDF file records */
    PMPI_Reduce(red_send_buf, red_recv_buf,
        shared_rec_count, red_type, red_op, 0, mod_comm);

    /* update module state to account for shared file reduction */
    if(my_rank == 0)
    {
        /* overwrite local shared records with globally reduced records */
        int tmp_ndx = rec_count - shared_rec_count;
        memcpy(&(pnetcdf_rec_buf[tmp_ndx]), red_recv_buf,
            shared_rec_count * sizeof(struct darshan_pnetcdf_file));
        free(red_recv_buf);
    }
    else
    {
        /* drop shared records on non-zero ranks */
        pnetcdf_file_runtime->rec_count -= shared_rec_count;
    }

    PMPI_Type_free(&red_type);
    PMPI_Op_free(&red_op);

    PNETCDF_UNLOCK();
    return;
}

static void pnetcdf_var_mpi_redux(
    void *pnetcdf_buf,
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count)
{
    int rec_count;
    struct pnetcdf_var_record_ref *rec_ref;
    struct darshan_pnetcdf_var *pnetcdf_rec_buf = (struct darshan_pnetcdf_var *)pnetcdf_buf;
    double pnetcdf_time;
    struct darshan_pnetcdf_var *red_send_buf = NULL;
    struct darshan_pnetcdf_var *red_recv_buf = NULL;
    MPI_Datatype red_type;
    MPI_Op red_op;
    int i;

    PNETCDF_LOCK();
    assert(pnetcdf_var_runtime);

    rec_count = pnetcdf_var_runtime->rec_count;

    /* necessary initialization of shared records */
    for(i = 0; i < shared_rec_count; i++)
    {
        rec_ref = darshan_lookup_record_ref(pnetcdf_var_runtime->rec_id_hash,
            &shared_recs[i], sizeof(darshan_record_id));
        assert(rec_ref);

        pnetcdf_time =
            rec_ref->var_rec->fcounters[PNETCDF_VAR_F_READ_TIME] +
            rec_ref->var_rec->fcounters[PNETCDF_VAR_F_WRITE_TIME] +
            rec_ref->var_rec->fcounters[PNETCDF_VAR_F_META_TIME];

        /* until reduction occurs, we assume that this rank is both
         * the fastest and slowest. It is up to the reduction operator
         * to find the true min and max.
         */
        rec_ref->var_rec->counters[PNETCDF_VAR_FASTEST_RANK] =
            rec_ref->var_rec->base_rec.rank;
        rec_ref->var_rec->counters[PNETCDF_VAR_FASTEST_RANK_BYTES] =
            rec_ref->var_rec->counters[PNETCDF_VAR_BYTES_READ] +
            rec_ref->var_rec->counters[PNETCDF_VAR_BYTES_WRITTEN];
        rec_ref->var_rec->fcounters[PNETCDF_VAR_F_FASTEST_RANK_TIME] =
            pnetcdf_time;

        rec_ref->var_rec->counters[PNETCDF_VAR_SLOWEST_RANK] =
            rec_ref->var_rec->counters[PNETCDF_VAR_FASTEST_RANK];
        rec_ref->var_rec->counters[PNETCDF_VAR_SLOWEST_RANK_BYTES] =
            rec_ref->var_rec->counters[PNETCDF_VAR_FASTEST_RANK_BYTES];
        rec_ref->var_rec->fcounters[PNETCDF_VAR_F_SLOWEST_RANK_TIME] =
            rec_ref->var_rec->fcounters[PNETCDF_VAR_F_FASTEST_RANK_TIME];

        rec_ref->var_rec->base_rec.rank = -1;
    }

    /* sort the array of records so we get all of the shared records
     * (marked by rank -1) in a contiguous portion at end of the array
     */
    darshan_record_sort(pnetcdf_rec_buf, rec_count,
        sizeof(struct darshan_pnetcdf_var));

    /* make *send_buf point to the shared variable records at the end of sorted
     * array */
    red_send_buf = &(pnetcdf_rec_buf[rec_count-shared_rec_count]);

    /* allocate memory for the reduction output on rank 0 */
    if(my_rank == 0)
    {
        red_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_pnetcdf_var));
        if(!red_recv_buf)
        {
            PNETCDF_UNLOCK();
            return;
        }
    }

    /* construct a datatype for a PNETCDF variable record.  This is serving no
     * purpose except to make sure we can do a reduction on proper boundaries
     */
    PMPI_Type_contiguous(sizeof(struct darshan_pnetcdf_var),
        MPI_BYTE, &red_type);
    PMPI_Type_commit(&red_type);

    /* register a PNETCDF variable record reduction operator */
    PMPI_Op_create(pnetcdf_var_record_reduction_op, 1, &red_op);

    /* reduce shared PNETCDF variable records */
    PMPI_Reduce(red_send_buf, red_recv_buf,
        shared_rec_count, red_type, red_op, 0, mod_comm);

    /* get the time and byte variances for shared files */
    pnetcdf_var_shared_record_variance(mod_comm, red_send_buf, red_recv_buf,
        shared_rec_count);

    /* update module state to account for shared file reduction */
    if(my_rank == 0)
    {
        /* overwrite local shared records with globally reduced records */
        int tmp_ndx = rec_count - shared_rec_count;
        memcpy(&(pnetcdf_rec_buf[tmp_ndx]), red_recv_buf,
            shared_rec_count * sizeof(struct darshan_pnetcdf_var));
        free(red_recv_buf);
    }
    else
    {
        /* drop shared records on non-zero ranks */
        pnetcdf_var_runtime->rec_count -= shared_rec_count;
    }

    PMPI_Type_free(&red_type);
    PMPI_Op_free(&red_op);

    PNETCDF_UNLOCK();
    return;
}

static void pnetcdf_file_output(
    void **pnetcdf_buf,
    int *pnetcdf_buf_sz)
{
    int rec_count;

    PNETCDF_LOCK();
    assert(pnetcdf_file_runtime);

    /* just pass back our updated total buffer size -- no need to update buffer */
    rec_count = pnetcdf_file_runtime->rec_count;
    *pnetcdf_buf_sz = rec_count * sizeof(struct darshan_pnetcdf_file);

    pnetcdf_file_runtime->frozen = 1;

    PNETCDF_UNLOCK();
    return;
}

static void pnetcdf_var_output(
    void **pnetcdf_buf,
    int *pnetcdf_buf_sz)
{
    int rec_count;

    PNETCDF_LOCK();
    assert(pnetcdf_var_runtime);

    /* just pass back our updated total buffer size -- no need to update buffer */
    rec_count = pnetcdf_var_runtime->rec_count;
    *pnetcdf_buf_sz = rec_count * sizeof(struct darshan_pnetcdf_var);

    pnetcdf_var_runtime->frozen = 1;

    PNETCDF_UNLOCK();
    return;
}

static void pnetcdf_file_cleanup(void)
{
    PNETCDF_LOCK();
    assert(pnetcdf_file_runtime);

    /* cleanup internal structures used for instrumenting */
    darshan_clear_record_refs(&(pnetcdf_file_runtime->ncid_hash), 0);
    darshan_clear_record_refs(&(pnetcdf_file_runtime->rec_id_hash), 1);

    free(pnetcdf_file_runtime);
    pnetcdf_file_runtime = NULL;
    pnetcdf_file_runtime_init_attempted = 0;

    PNETCDF_UNLOCK();
    return;
}

static void pnetcdf_var_cleanup(void)
{
    PNETCDF_LOCK();
    assert(pnetcdf_var_runtime);

    /* perform any final transformations on PnetCDF file records before
     * writing them out to log file
     */
    darshan_iter_record_refs(pnetcdf_var_runtime->rec_id_hash,
        &pnetcdf_var_finalize_records, NULL);

    /* cleanup internal structures used for instrumenting */
    darshan_clear_record_refs(&(pnetcdf_var_runtime->varid_hash), 0);
    darshan_clear_record_refs(&(pnetcdf_var_runtime->rec_id_hash), 1);

    free(pnetcdf_var_runtime);
    pnetcdf_var_runtime = NULL;
    pnetcdf_var_runtime_init_attempted = 0;

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
