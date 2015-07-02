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
#include <fcntl.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/uio.h>
#include <sys/mman.h>
#include <search.h>
#include <assert.h>
#include <libgen.h>
#include <aio.h>
#define __USE_GNU
#include <pthread.h>

#include "uthash.h"

#include "darshan.h"
#include "darshan-mpiio-log-format.h"
#include "darshan-dynamic.h"

/* The mpiio_file_runtime structure maintains necessary runtime metadata
 * for the MPIIO file record (darshan_mpiio_file structure, defined in
 * darshan-mpiio-log-format.h) pointed to by 'file_record'. This metadata
 * assists with the instrumenting of specific statistics in the file record.
 * 'hlink' is a hash table link structure used to add/remove this record
 * from the hash table of MPIIO file records for this process. 
 *
 * RATIONALE: the MPIIO module needs to track some stateful, volatile 
 * information about each open file (like the current file offset, most recent 
 * access time, etc.) to aid in instrumentation, but this information can't be
 * stored in the darshan_mpiio_file struct because we don't want it to appear in
 * the final darshan log file.  We therefore associate a mpiio_file_runtime
 * struct with each darshan_mpiio_file struct in order to track this information.
  *
 * NOTE: There is a one-to-one mapping of mpiio_file_runtime structs to
 * darshan_mpiio_file structs.
 *
 * NOTE: The mpiio_file_runtime struct contains a pointer to a darshan_mpiio_file
 * struct (see the *file_record member) rather than simply embedding an entire
 * darshan_mpiio_file struct.  This is done so that all of the darshan_mpiio_file
 * structs can be kept contiguous in memory as a single array to simplify
 * reduction, compression, and storage.
 */
struct mpiio_file_runtime
{
    struct darshan_mpiio_file* file_record;
    double last_meta_end;
    double last_read_end;
    double last_write_end;
    UT_hash_handle hlink;
};

/* The mpiio_file_runtime_ref structure is used to associate a MPIIO
 * file handle with an already existing MPIIO file record. This is
 * necessary as many MPIIO I/O functions take only a file handle as input,
 * but MPIIO file records are indexed by their full file paths (i.e., darshan
 * record identifiers for MPIIO files are created by hashing the file path).
 * In other words, this structure is necessary as it allows us to look up a
 * file record either by a pathname (mpiio_file_runtime) or by MPIIO file
 * descriptor (mpiio_file_runtime_ref), depending on which parameters are
 * available. This structure includes another hash table link, since separate
 * hashes are maintained for mpiio_file_runtime structures and mpiio_file_runtime_ref
 * structures.
 *
 * RATIONALE: In theory the file handle information could be included in the
 * mpiio_file_runtime struct rather than in a separate structure here.  The
 * reason we don't do that is to handle the potential for an MPI implementation
 * to produce a new file handle instance each time MPI_File_open() is called on a
 * file.  Thus there might be multiple file handles referring to the same
 * underlying record.
 *
 * NOTE: there are potentially multiple mpiio_file_runtime_ref structures
 * referring to a single mpiio_file_runtime structure.  Most of the time there is
 * only one, however.
 */
struct mpiio_file_runtime_ref
{
    struct mpiio_file_runtime* file;
    MPI_File fh;
    UT_hash_handle hlink;
};

/* The mpiio_runtime structure maintains necessary state for storing
 * MPI-IO file records and for coordinating with darshan-core at 
 * shutdown time.
 */
struct mpiio_runtime
{
    struct mpiio_file_runtime* file_runtime_array;
    struct darshan_mpiio_file* file_record_array;
    int file_array_size;
    int file_array_ndx;
    struct mpiio_file_runtime* file_hash;
    struct mpiio_file_runtime_ref* fh_hash;
    void *red_buf;
    int shared_rec_count;
};

static struct mpiio_runtime *mpiio_runtime = NULL;
static pthread_mutex_t mpiio_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int instrumentation_disabled = 0;
static int my_rank = -1;

static void mpiio_runtime_initialize(void);
static struct mpiio_file_runtime* mpiio_file_by_name(const char *name);
static struct mpiio_file_runtime* mpiio_file_by_name_setfh(const char* name, MPI_File fh);
static struct mpiio_file_runtime* mpiio_file_by_fh(MPI_File fh);
static void mpiio_file_close_fh(MPI_File fh);
static int mpiio_file_compare(const void* a, const void* b);

static void mpiio_begin_shutdown(void);
static void mpiio_setup_reduction(darshan_record_id *shared_recs, int *shared_rec_count,
    void **send_buf, void **recv_buf, int *rec_size);
static void mpiio_record_reduction_op(void* infile_v, void* inoutfile_v,
    int *len, MPI_Datatype *datatype);
static void mpiio_get_output_data(void **buffer, int *size);
static void mpiio_shutdown(void);

/* TODO: maybe use a counter to track cases in which a derived datatype is used? */

#define MPIIO_LOCK() pthread_mutex_lock(&mpiio_runtime_mutex)
#define MPIIO_UNLOCK() pthread_mutex_unlock(&mpiio_runtime_mutex)

#define MPIIO_RECORD_READ(__ret, __fh, __count, __datatype, __counter, __tm1, __tm2) do { \
    struct mpiio_file_runtime* file; \
    int size = 0; \
    MPI_Aint extent = 0; \
    if(__ret != MPI_SUCCESS) break; \
    file = mpiio_file_by_fh(__fh); \
    if(!file) break; \
    DARSHAN_MPI_CALL(PMPI_Type_size)(__datatype, &size);  \
    size = size * __count; \
    DARSHAN_MPI_CALL(PMPI_Type_extent)(__datatype, &extent); \
    DARSHAN_BUCKET_INC(file->file_record, MPIIO_SIZE_READ_AGG_0_100, size); \
    DARSHAN_BUCKET_INC(file->file_record, MPIIO_EXTENT_READ_0_100, extent); \
    DARSHAN_COUNTER_INC(file->file_record, __counter, 1); \
    DARSHAN_COUNTER_F_INC_NO_OVERLAP(file->file_record, __tm1, __tm2, \
        file->last_read_end, MPIIO_F_READ_TIME); \
    if(DARSHAN_COUNTER_F_VALUE(file->file_record, MPIIO_F_READ_START_TIMESTAMP) == 0) \
        DARSHAN_COUNTER_F_SET(file->file_record, MPIIO_F_READ_START_TIMESTAMP, __tm1); \
    DARSHAN_COUNTER_F_SET(file->file_record, MPIIO_F_READ_END_TIMESTAMP, __tm2); \
} while(0)

#define MPIIO_RECORD_WRITE(__ret, __fh, __count, __datatype, __counter, __tm1, __tm2) do { \
    struct mpiio_file_runtime* file; \
    int size = 0; \
    MPI_Aint extent = 0; \
    if(__ret != MPI_SUCCESS) break; \
    file = mpiio_file_by_fh(__fh); \
    if(!file) break; \
    DARSHAN_MPI_CALL(PMPI_Type_size)(__datatype, &size);  \
    size = size * __count; \
    DARSHAN_MPI_CALL(PMPI_Type_extent)(__datatype, &extent); \
    DARSHAN_BUCKET_INC(file->file_record, MPIIO_SIZE_WRITE_AGG_0_100, size); \
    DARSHAN_BUCKET_INC(file->file_record, MPIIO_EXTENT_WRITE_0_100, extent); \
    DARSHAN_COUNTER_INC(file->file_record, __counter, 1); \
    DARSHAN_COUNTER_F_INC_NO_OVERLAP(file->file_record, __tm1, __tm2, \
        file->last_write_end, MPIIO_F_WRITE_TIME); \
    if(DARSHAN_COUNTER_F_VALUE(file->file_record, MPIIO_F_WRITE_START_TIMESTAMP) == 0) \
        DARSHAN_COUNTER_F_SET(file->file_record, MPIIO_F_WRITE_START_TIMESTAMP, __tm1); \
    DARSHAN_COUNTER_F_SET(file->file_record, MPIIO_F_WRITE_END_TIMESTAMP, __tm2); \
} while(0)

/**********************************************************
 *        Wrappers for MPI-IO functions of interest       * 
 **********************************************************/

#ifdef HAVE_MPIIO_CONST
int MPI_File_open(MPI_Comm comm, const char *filename, int amode, MPI_Info info, MPI_File *fh) 
#else
int MPI_File_open(MPI_Comm comm, char *filename, int amode, MPI_Info info, MPI_File *fh) 
#endif
{
    int ret;
    struct mpiio_file_runtime* file;
    char* tmp;
    int comm_size;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_open)(comm, filename, amode, info, fh);
    tm2 = darshan_core_wtime();

    if(ret == MPI_SUCCESS)
    {
        MPIIO_LOCK();
        mpiio_runtime_initialize();

        /* use ROMIO approach to strip prefix if present */
        /* strip off prefix if there is one, but only skip prefixes
         * if they are greater than length one to allow for windows
         * drive specifications (e.g. c:\...) 
         */
        tmp = strchr(filename, ':');
        if (tmp > filename + 1) {
            filename = tmp + 1;
        }

        file = mpiio_file_by_name_setfh(filename, (*fh));
        if(file)
        {
            file->file_record->rank = my_rank;
            DARSHAN_MPI_CALL(PMPI_Comm_size)(comm, &comm_size);
            if(comm_size == 1)
            {
                DARSHAN_COUNTER_INC(file->file_record, MPIIO_INDEP_OPENS, 1);
            }
            else
            {
                DARSHAN_COUNTER_INC(file->file_record, MPIIO_COLL_OPENS, 1);
            }
            if(info != MPI_INFO_NULL)
            {
                DARSHAN_COUNTER_INC(file->file_record, MPIIO_HINTS, 1);
            }
            if(DARSHAN_COUNTER_F_VALUE(file->file_record, MPIIO_F_OPEN_TIMESTAMP) == 0)
                DARSHAN_COUNTER_F_SET(file->file_record, MPIIO_F_OPEN_TIMESTAMP, tm1);
            DARSHAN_COUNTER_F_INC_NO_OVERLAP(file->file_record, tm1, tm2,
                file->last_meta_end, MPIIO_F_META_TIME);
        }

        MPIIO_UNLOCK();
    }

    return(ret);
}

int MPI_File_read(MPI_File fh, void *buf, int count,
    MPI_Datatype datatype, MPI_Status *status)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_read)(fh, buf, count, datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_INDEP_READS, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

#ifdef HAVE_MPIIO_CONST
int MPI_File_write(MPI_File fh, const void *buf, int count,
    MPI_Datatype datatype, MPI_Status *status)
#else
int MPI_File_write(MPI_File fh, void *buf, int count,
    MPI_Datatype datatype, MPI_Status *status)
#endif
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_write)(fh, buf, count, datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_INDEP_WRITES, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

int MPI_File_read_at(MPI_File fh, MPI_Offset offset, void *buf,
    int count, MPI_Datatype datatype, MPI_Status *status)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_read_at)(fh, offset, buf,
        count, datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_INDEP_READS, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

#ifdef HAVE_MPIIO_CONST
int MPI_File_write_at(MPI_File fh, MPI_Offset offset, const void *buf,
    int count, MPI_Datatype datatype, MPI_Status *status)
#else
int MPI_File_write_at(MPI_File fh, MPI_Offset offset, void *buf,
    int count, MPI_Datatype datatype, MPI_Status *status)
#endif
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_write_at)(fh, offset, buf,
        count, datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_INDEP_WRITES, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

int MPI_File_read_all(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status *status)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_read_all)(fh, buf, count,
        datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_COLL_READS, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

#ifdef HAVE_MPIIO_CONST
int MPI_File_write_all(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Status *status)
#else
int MPI_File_write_all(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status *status)
#endif
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_write_all)(fh, buf, count,
        datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_COLL_WRITES, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

int MPI_File_read_at_all(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype, MPI_Status * status)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_read_at_all)(fh, offset, buf,
        count, datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_COLL_READS, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

#ifdef HAVE_MPIIO_CONST
int MPI_File_write_at_all(MPI_File fh, MPI_Offset offset, const void * buf,
    int count, MPI_Datatype datatype, MPI_Status * status)
#else
int MPI_File_write_at_all(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype, MPI_Status * status)
#endif
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_write_at_all)(fh, offset, buf,
        count, datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_COLL_WRITES, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

/* TODO: reads and writes */

int MPI_File_sync(MPI_File fh)
{
    int ret;
    struct mpiio_file_runtime* file;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_sync)(fh);
    tm2 = darshan_core_wtime();

    if(ret == MPI_SUCCESS)
    {
        MPIIO_LOCK();
        mpiio_runtime_initialize();
        file = mpiio_file_by_fh(fh);
        if(file)
        {
            DARSHAN_COUNTER_INC(file->file_record, MPIIO_SYNCS, 1);
            DARSHAN_COUNTER_F_INC_NO_OVERLAP(file->file_record, tm1, tm2,
                file->last_write_end, MPIIO_F_WRITE_TIME);
        }
        MPIIO_UNLOCK();
    }

    return(ret);
}

/* TODO: test */
#ifdef HAVE_MPIIO_CONST
int MPI_File_set_view(MPI_File fh, MPI_Offset disp, MPI_Datatype etype,
    MPI_Datatype filetype, const char *datarep, MPI_Info info)
#else
int MPI_File_set_view(MPI_File fh, MPI_Offset disp, MPI_Datatype etype,
    MPI_Datatype filetype, char *datarep, MPI_Info info)
#endif
{
    int ret;
    struct mpiio_file_runtime* file;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_set_view)(fh, disp, etype, filetype,
        datarep, info);
    tm2 = darshan_core_wtime();

    if(ret == MPI_SUCCESS)
    {
        MPIIO_LOCK();
        mpiio_runtime_initialize();
        file = mpiio_file_by_fh(fh);
        if(file)
        {
            DARSHAN_COUNTER_INC(file->file_record, MPIIO_VIEWS, 1);
            if(info != MPI_INFO_NULL)
            {
                DARSHAN_COUNTER_INC(file->file_record, MPIIO_HINTS, 1);
                DARSHAN_COUNTER_F_INC_NO_OVERLAP(file->file_record, tm1, tm2,
                    file->last_meta_end, MPIIO_F_META_TIME);
           }
        }
        MPIIO_UNLOCK();
    }

    return(ret);
}

int MPI_File_close(MPI_File *fh)
{
    int ret;
    struct mpiio_file_runtime* file;
    MPI_File tmp_fh = *fh;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_close)(fh);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    file = mpiio_file_by_fh(tmp_fh);
    if(file)
    {
        DARSHAN_COUNTER_F_SET(file->file_record, MPIIO_F_CLOSE_TIMESTAMP,
            darshan_core_wtime());
        DARSHAN_COUNTER_F_INC_NO_OVERLAP(file->file_record, tm1, tm2,
            file->last_meta_end, MPIIO_F_META_TIME);
        mpiio_file_close_fh(tmp_fh);
    }
    MPIIO_UNLOCK();

    return(ret);
}

/***********************************************************
 * Internal functions for manipulating MPI-IO module state *
 ***********************************************************/

/* initialize data structures and register with darshan-core component */
static void mpiio_runtime_initialize()
{
    int mem_limit;
    struct darshan_module_funcs mpiio_mod_fns =
    {
        .begin_shutdown = &mpiio_begin_shutdown,
        .setup_reduction = &mpiio_setup_reduction,
        .record_reduction_op = &mpiio_record_reduction_op,
        .get_output_data = &mpiio_get_output_data,
        .shutdown = &mpiio_shutdown
    };

    /* don't do anything if already initialized or instrumenation is disabled */
    if(mpiio_runtime || instrumentation_disabled)
        return;

    /* register the mpiio module with darshan core */
    darshan_core_register_module(
        DARSHAN_MPIIO_MOD,
        &mpiio_mod_fns,
        &mem_limit,
        NULL);

    /* return if no memory assigned by darshan core */
    if(mem_limit == 0)
        return;

    mpiio_runtime = malloc(sizeof(*mpiio_runtime));
    if(!mpiio_runtime)
        return;
    memset(mpiio_runtime, 0, sizeof(*mpiio_runtime));

    /* set maximum number of file records according to max memory limit */
    /* NOTE: maximum number of records is based on the size of a mpiio file record */
    mpiio_runtime->file_array_size = mem_limit / sizeof(struct darshan_mpiio_file);
    mpiio_runtime->file_array_ndx = 0;

    /* allocate array of runtime file records */
    mpiio_runtime->file_runtime_array = malloc(mpiio_runtime->file_array_size *
                                               sizeof(struct mpiio_file_runtime));
    mpiio_runtime->file_record_array = malloc(mpiio_runtime->file_array_size *
                                              sizeof(struct darshan_mpiio_file));
    if(!mpiio_runtime->file_runtime_array || !mpiio_runtime->file_record_array)
    {
        mpiio_runtime->file_array_size = 0;
        return;
    }
    memset(mpiio_runtime->file_runtime_array, 0, mpiio_runtime->file_array_size *
           sizeof(struct mpiio_file_runtime));
    memset(mpiio_runtime->file_record_array, 0, mpiio_runtime->file_array_size *
           sizeof(struct darshan_mpiio_file));

    /* TODO: can we move this out of here? perhaps register_module returns rank? */
    DARSHAN_MPI_CALL(PMPI_Comm_rank)(MPI_COMM_WORLD, &my_rank);

    return;
}

/* get a MPIIO file record for the given file path */
static struct mpiio_file_runtime* mpiio_file_by_name(const char *name)
{
    struct mpiio_file_runtime *file = NULL;
    char *newname = NULL;
    darshan_record_id file_id;

    if(!mpiio_runtime || instrumentation_disabled)
        return(NULL);

    newname = darshan_clean_file_path(name);
    if(!newname)
        newname = (char*)name;

    /* get a unique id for this file from darshan core */
    darshan_core_register_record(
        (void*)newname,
        strlen(newname),
        1,
        DARSHAN_MPIIO_MOD,
        &file_id,
        NULL);

    /* search the hash table for this file record, and return if found */
    HASH_FIND(hlink, mpiio_runtime->file_hash, &file_id, sizeof(darshan_record_id), file);
    if(file)
    {
        if(newname != name)
            free(newname);
        return(file);
    }

    if(mpiio_runtime->file_array_ndx < mpiio_runtime->file_array_size);
    {
        /* no existing record, assign a new file record from the global array */
        file = &(mpiio_runtime->file_runtime_array[mpiio_runtime->file_array_ndx]);
        file->file_record = &(mpiio_runtime->file_record_array[mpiio_runtime->file_array_ndx]);
        file->file_record->f_id = file_id;

        /* add new record to file hash table */
        HASH_ADD(hlink, mpiio_runtime->file_hash, file_record->f_id, sizeof(darshan_record_id), file);

        mpiio_runtime->file_array_ndx++;
    }

    if(newname != name)
        free(newname);
    return(file);
}

/* get an MPIIO file record for the given file path, and also create a
 * reference structure using the corresponding file handle
 */
static struct mpiio_file_runtime* mpiio_file_by_name_setfh(const char* name, MPI_File fh)
{
    struct mpiio_file_runtime* file;
    struct mpiio_file_runtime_ref* ref;

    if(!mpiio_runtime || instrumentation_disabled)
        return(NULL);

    /* find file record by name first */
    file = mpiio_file_by_name(name);

    if(!file)
        return(NULL);

    /* search hash table for existing file ref for this fh */
    HASH_FIND(hlink, mpiio_runtime->fh_hash, &fh, sizeof(fh), ref);
    if(ref)
    {
        /* we have a reference.  Make sure it points to the correct file
         * and return it
         */
        ref->file = file;
        return(file);
    }

    /* if we hit this point, then we don't have a reference for this fh
     * in the table yet.  Add it.
     */
    ref = malloc(sizeof(*ref));
    if(!ref)
        return(NULL);
    memset(ref, 0, sizeof(*ref));

    ref->file = file;
    ref->fh = fh;    
    HASH_ADD(hlink, mpiio_runtime->fh_hash, fh, sizeof(fh), ref);

    return(file);
}

/* get an MPIIO file record for the given file handle */
static struct mpiio_file_runtime* mpiio_file_by_fh(MPI_File fh)
{
    struct mpiio_file_runtime_ref* ref;

    if(!mpiio_runtime || instrumentation_disabled)
        return(NULL);

    /* search hash table for existing file ref for this file handle */
    HASH_FIND(hlink, mpiio_runtime->fh_hash, &fh, sizeof(fh), ref);
    if(ref)
        return(ref->file);

    return(NULL);
}

/* free up reference data structures for the given file handle */
static void mpiio_file_close_fh(MPI_File fh)
{
    struct mpiio_file_runtime_ref* ref;

    if(!mpiio_runtime || instrumentation_disabled)
        return;

    /* search hash table for this fd */
    HASH_FIND(hlink, mpiio_runtime->fh_hash, &fh, sizeof(fh), ref);
    if(ref)
    {
        /* we have a reference, delete it */
        HASH_DELETE(hlink, mpiio_runtime->fh_hash, ref);
        free(ref);
    }

    return;
}

/* compare function for sorting file records by descending rank */
static int mpiio_file_compare(const void* a_p, const void* b_p)
{
    const struct darshan_mpiio_file* a = a_p;
    const struct darshan_mpiio_file* b = b_p;

    if(a->rank < b->rank)
        return 1;
    if(a->rank > b->rank)
        return -1;

    return 0;
}

/**************************************************************************
 * Functions exported by MPI-IO module for coordinating with darshan-core *
 **************************************************************************/

static void mpiio_begin_shutdown()
{
    assert(mpiio_runtime);

    MPIIO_LOCK();
    instrumentation_disabled = 1;
    MPIIO_UNLOCK();

    return;
}

static void mpiio_setup_reduction(
    darshan_record_id *shared_recs,
    int *shared_rec_count,
    void **send_buf,
    void **recv_buf,
    int *rec_size)
{
    struct mpiio_file_runtime *file;
    int i;

    assert(mpiio_runtime);

    /* necessary initialization of shared records (e.g., change rank to -1) */
    for(i = 0; i < *shared_rec_count; i++)
    {
        HASH_FIND(hlink, mpiio_runtime->file_hash, &shared_recs[i],
            sizeof(darshan_record_id), file);
        assert(file);

        file->file_record->rank = -1;
    }

    /* sort the array of files descending by rank so that we get all of the 
     * shared files (marked by rank -1) in a contiguous portion at end 
     * of the array
     */
    qsort(mpiio_runtime->file_record_array, mpiio_runtime->file_array_ndx,
        sizeof(struct darshan_mpiio_file), mpiio_file_compare);

    /* make *send_buf point to the shared files at the end of sorted array */
    *send_buf =
        &(mpiio_runtime->file_record_array[mpiio_runtime->file_array_ndx-(*shared_rec_count)]);

    /* allocate memory for the reduction output on rank 0 */
    if(my_rank == 0)
    {
        *recv_buf = malloc(*shared_rec_count * sizeof(struct darshan_mpiio_file));
        if(!(*recv_buf))
            return;
    }

    *rec_size = sizeof(struct darshan_mpiio_file);

    /* TODO: cleaner way to do this? */
    if(my_rank == 0)
        mpiio_runtime->red_buf = *recv_buf;
    mpiio_runtime->shared_rec_count = *shared_rec_count;

    return;
}

static void mpiio_record_reduction_op(
    void* infile_v,
    void* inoutfile_v,
    int *len,
    MPI_Datatype *datatype)
{
    struct darshan_mpiio_file tmp_file;
    struct darshan_mpiio_file *infile = infile_v;
    struct darshan_mpiio_file *inoutfile = inoutfile_v;
    int i, j;

    assert(mpiio_runtime);

    for(i=0; i<*len; i++)
    {
        memset(&tmp_file, 0, sizeof(struct darshan_mpiio_file));

        tmp_file.f_id = infile->f_id;
        tmp_file.rank = -1;

        /* sum */
        for(j=MPIIO_INDEP_OPENS; j<=MPIIO_HINTS; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }

        /* sum (floating point) */
        for(j=MPIIO_F_META_TIME; j<=MPIIO_F_META_TIME; j++)
        {
            tmp_file.fcounters[j] = infile->fcounters[j] + inoutfile->fcounters[j];
        }

        /* min non-zero (if available) value */
        for(j=MPIIO_F_OPEN_TIMESTAMP; j<=MPIIO_F_OPEN_TIMESTAMP; j++)
        {
            if(infile->fcounters[j] > inoutfile->fcounters[j] && inoutfile->fcounters[j] > 0)
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
            else
                tmp_file.fcounters[j] = infile->fcounters[j];
        }

        /* update pointers */
        *inoutfile = tmp_file;
        inoutfile++;
        infile++;
    }

    return;
}

static void mpiio_get_output_data(
    void **buffer,
    int *size)
{
    assert(mpiio_runtime);

    /* TODO: clean up reduction stuff */
    if(my_rank == 0)
    {
        int tmp_ndx = mpiio_runtime->file_array_ndx - mpiio_runtime->shared_rec_count;
        memcpy(&(mpiio_runtime->file_record_array[tmp_ndx]), mpiio_runtime->red_buf,
            mpiio_runtime->shared_rec_count * sizeof(struct darshan_mpiio_file));
        free(mpiio_runtime->red_buf);
    }
    else
    {
        mpiio_runtime->file_array_ndx -= mpiio_runtime->shared_rec_count;
    }

    *buffer = (void *)(mpiio_runtime->file_record_array);
    *size = mpiio_runtime->file_array_ndx * sizeof(struct darshan_mpiio_file);

    return;
}

static void mpiio_shutdown()
{
    struct mpiio_file_runtime_ref *ref, *tmp;

    HASH_ITER(hlink, mpiio_runtime->fh_hash, ref, tmp)
    {
        HASH_DELETE(hlink, mpiio_runtime->fh_hash, ref);
        free(ref);
    }

    HASH_CLEAR(hlink, mpiio_runtime->file_hash); /* these entries are freed all at once below */

    free(mpiio_runtime->file_runtime_array);
    free(mpiio_runtime->file_record_array);
    free(mpiio_runtime);
    mpiio_runtime = NULL;

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
