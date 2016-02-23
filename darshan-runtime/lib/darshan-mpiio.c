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
#include <fcntl.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <errno.h>
#include <search.h>
#include <assert.h>
#include <pthread.h>

#include "uthash.h"

#include "darshan.h"
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
    enum darshan_io_type last_io_type;
    double last_meta_end;
    double last_read_end;
    double last_write_end;
    void *access_root;
    int access_count;
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
static int mpiio_record_compare(const void* a, const void* b);
static void mpiio_record_reduction_op(void* infile_v, void* inoutfile_v,
    int *len, MPI_Datatype *datatype);
static void mpiio_shared_record_variance(MPI_Comm mod_comm,
    struct darshan_mpiio_file *inrec_array, struct darshan_mpiio_file *outrec_array,
    int shared_rec_count);

static void mpiio_begin_shutdown(void);
static void mpiio_get_output_data(MPI_Comm mod_comm, darshan_record_id *shared_recs,
    int shared_rec_count, void **mpiio_buf, int *mpiio_buf_sz);
static void mpiio_shutdown(void);

#define MPIIO_LOCK() pthread_mutex_lock(&mpiio_runtime_mutex)
#define MPIIO_UNLOCK() pthread_mutex_unlock(&mpiio_runtime_mutex)

#define MPIIO_RECORD_OPEN(__ret, __path, __fh, __comm, __mode, __info, __tm1, __tm2) do { \
    struct mpiio_file_runtime* file; \
    char *exclude; \
    int tmp_index = 0; \
    int comm_size; \
    if(__ret != MPI_SUCCESS) break; \
    while((exclude=darshan_path_exclusions[tmp_index])) { \
        if(!(strncmp(exclude, __path, strlen(exclude)))) \
            break; \
        tmp_index++; \
    } \
    if(exclude) break; \
    file = mpiio_file_by_name_setfh(__path, __fh); \
    if(!file) break; \
    file->file_record->counters[MPIIO_MODE] = __mode; \
    DARSHAN_MPI_CALL(PMPI_Comm_size)(__comm, &comm_size); \
    if(comm_size == 1) \
        file->file_record->counters[MPIIO_INDEP_OPENS] += 1; \
    else \
        file->file_record->counters[MPIIO_COLL_OPENS] += 1; \
    if(__info != MPI_INFO_NULL) \
        file->file_record->counters[MPIIO_HINTS] += 1; \
    if(file->file_record->fcounters[MPIIO_F_OPEN_TIMESTAMP] == 0) \
        file->file_record->fcounters[MPIIO_F_OPEN_TIMESTAMP] = __tm1; \
    DARSHAN_TIMER_INC_NO_OVERLAP(file->file_record->fcounters[MPIIO_F_META_TIME], __tm1, __tm2, file->last_meta_end); \
} while(0)

#define MPIIO_RECORD_READ(__ret, __fh, __count, __datatype, __counter, __tm1, __tm2) do { \
    struct mpiio_file_runtime* file; \
    int size = 0; \
    double __elapsed = __tm2-__tm1; \
    if(__ret != MPI_SUCCESS) break; \
    file = mpiio_file_by_fh(__fh); \
    if(!file) break; \
    DARSHAN_MPI_CALL(PMPI_Type_size)(__datatype, &size);  \
    size = size * __count; \
    DARSHAN_BUCKET_INC(&(file->file_record->counters[MPIIO_SIZE_READ_AGG_0_100]), size); \
    darshan_common_val_counter(&file->access_root, &file->access_count, size); \
    file->file_record->counters[MPIIO_BYTES_READ] += size; \
    file->file_record->counters[__counter] += 1; \
    if(file->last_io_type == DARSHAN_IO_WRITE) \
        file->file_record->counters[MPIIO_RW_SWITCHES] += 1; \
    file->last_io_type = DARSHAN_IO_READ; \
    if(file->file_record->fcounters[MPIIO_F_READ_START_TIMESTAMP] == 0) \
        file->file_record->fcounters[MPIIO_F_READ_START_TIMESTAMP] = __tm1; \
    file->file_record->fcounters[MPIIO_F_READ_END_TIMESTAMP] = __tm2; \
    if(file->file_record->fcounters[MPIIO_F_MAX_READ_TIME] < __elapsed) { \
        file->file_record->fcounters[MPIIO_F_MAX_READ_TIME] = __elapsed; \
        file->file_record->counters[MPIIO_MAX_READ_TIME_SIZE] = size; } \
    DARSHAN_TIMER_INC_NO_OVERLAP(file->file_record->fcounters[MPIIO_F_READ_TIME], __tm1, __tm2, file->last_read_end); \
} while(0)

#define MPIIO_RECORD_WRITE(__ret, __fh, __count, __datatype, __counter, __tm1, __tm2) do { \
    struct mpiio_file_runtime* file; \
    int size = 0; \
    double __elapsed = __tm2-__tm1; \
    if(__ret != MPI_SUCCESS) break; \
    file = mpiio_file_by_fh(__fh); \
    if(!file) break; \
    DARSHAN_MPI_CALL(PMPI_Type_size)(__datatype, &size);  \
    size = size * __count; \
    DARSHAN_BUCKET_INC(&(file->file_record->counters[MPIIO_SIZE_WRITE_AGG_0_100]), size); \
    darshan_common_val_counter(&file->access_root, &file->access_count, size); \
    file->file_record->counters[MPIIO_BYTES_WRITTEN] += size; \
    file->file_record->counters[__counter] += 1; \
    if(file->last_io_type == DARSHAN_IO_READ) \
        file->file_record->counters[MPIIO_RW_SWITCHES] += 1; \
    file->last_io_type = DARSHAN_IO_WRITE; \
    if(file->file_record->fcounters[MPIIO_F_WRITE_START_TIMESTAMP] == 0) \
        file->file_record->fcounters[MPIIO_F_WRITE_START_TIMESTAMP] = __tm1; \
    file->file_record->fcounters[MPIIO_F_WRITE_END_TIMESTAMP] = __tm2; \
    if(file->file_record->fcounters[MPIIO_F_MAX_WRITE_TIME] < __elapsed) { \
        file->file_record->fcounters[MPIIO_F_MAX_WRITE_TIME] = __elapsed; \
        file->file_record->counters[MPIIO_MAX_WRITE_TIME_SIZE] = size; } \
    DARSHAN_TIMER_INC_NO_OVERLAP(file->file_record->fcounters[MPIIO_F_WRITE_TIME], __tm1, __tm2, file->last_write_end); \
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
    char* tmp;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_open)(comm, filename, amode, info, fh);
    tm2 = darshan_core_wtime();

    /* use ROMIO approach to strip prefix if present */
    /* strip off prefix if there is one, but only skip prefixes
     * if they are greater than length one to allow for windows
     * drive specifications (e.g. c:\...) 
     */
    tmp = strchr(filename, ':');
    if (tmp > filename + 1) {
        filename = tmp + 1;
    }

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_OPEN(ret, filename, (*fh), comm, amode, info, tm1, tm2);
    MPIIO_UNLOCK();
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

int MPI_File_read_shared(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status *status)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_read_shared)(fh, buf, count,
        datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_INDEP_READS, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

#ifdef HAVE_MPIIO_CONST
int MPI_File_write_shared(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Status *status)
#else
int MPI_File_write_shared(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status *status)
#endif
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_write_shared)(fh, buf, count,
        datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_INDEP_WRITES, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

int MPI_File_read_ordered(MPI_File fh, void * buf, int count,
    MPI_Datatype datatype, MPI_Status * status)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_read_ordered)(fh, buf, count,
        datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_COLL_READS, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

#ifdef HAVE_MPIIO_CONST
int MPI_File_write_ordered(MPI_File fh, const void * buf, int count,
    MPI_Datatype datatype, MPI_Status * status)
#else
int MPI_File_write_ordered(MPI_File fh, void * buf, int count,
    MPI_Datatype datatype, MPI_Status * status)
#endif
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_write_ordered)(fh, buf, count,
         datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_COLL_WRITES, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

int MPI_File_read_all_begin(MPI_File fh, void * buf, int count, MPI_Datatype datatype)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_read_all_begin)(fh, buf, count, datatype);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_SPLIT_READS, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

#ifdef HAVE_MPIIO_CONST
int MPI_File_write_all_begin(MPI_File fh, const void * buf, int count, MPI_Datatype datatype)
#else
int MPI_File_write_all_begin(MPI_File fh, void * buf, int count, MPI_Datatype datatype)
#endif
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_write_all_begin)(fh, buf, count, datatype);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_SPLIT_WRITES, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

int MPI_File_read_at_all_begin(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_read_at_all_begin)(fh, offset, buf,
        count, datatype);
    tm2 = darshan_core_wtime();
    
    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_SPLIT_READS, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

#ifdef HAVE_MPIIO_CONST
int MPI_File_write_at_all_begin(MPI_File fh, MPI_Offset offset, const void * buf,
    int count, MPI_Datatype datatype)
#else
int MPI_File_write_at_all_begin(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype)
#endif
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_write_at_all_begin)(fh, offset,
        buf, count, datatype);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_SPLIT_WRITES, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

int MPI_File_read_ordered_begin(MPI_File fh, void * buf, int count, MPI_Datatype datatype)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_read_ordered_begin)(fh, buf, count,
        datatype);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_SPLIT_READS, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

#ifdef HAVE_MPIIO_CONST
int MPI_File_write_ordered_begin(MPI_File fh, const void * buf, int count, MPI_Datatype datatype)
#else
int MPI_File_write_ordered_begin(MPI_File fh, void * buf, int count, MPI_Datatype datatype)
#endif
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_write_ordered_begin)(fh, buf, count,
        datatype);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_SPLIT_WRITES, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

int MPI_File_iread(MPI_File fh, void * buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST * request)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_iread)(fh, buf, count, datatype, request);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_NB_READS, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

#ifdef HAVE_MPIIO_CONST
int MPI_File_iwrite(MPI_File fh, const void * buf, int count,
    MPI_Datatype datatype, __D_MPI_REQUEST * request)
#else
int MPI_File_iwrite(MPI_File fh, void * buf, int count,
    MPI_Datatype datatype, __D_MPI_REQUEST * request)
#endif
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_iwrite)(fh, buf, count, datatype, request);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_NB_WRITES, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

int MPI_File_iread_at(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype, __D_MPI_REQUEST *request)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_iread_at)(fh, offset, buf, count,
        datatype, request);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_NB_READS, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

#ifdef HAVE_MPIIO_CONST
int MPI_File_iwrite_at(MPI_File fh, MPI_Offset offset, const void * buf,
    int count, MPI_Datatype datatype, __D_MPI_REQUEST *request)
#else
int MPI_File_iwrite_at(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype, __D_MPI_REQUEST *request)
#endif
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_iwrite_at)(fh, offset, buf,
        count, datatype, request);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_NB_WRITES, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

int MPI_File_iread_shared(MPI_File fh, void * buf, int count,
    MPI_Datatype datatype, __D_MPI_REQUEST * request)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_iread_shared)(fh, buf, count,
        datatype, request);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_NB_READS, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

#ifdef HAVE_MPIIO_CONST
int MPI_File_iwrite_shared(MPI_File fh, const void * buf, int count,
    MPI_Datatype datatype, __D_MPI_REQUEST * request)
#else
int MPI_File_iwrite_shared(MPI_File fh, void * buf, int count,
    MPI_Datatype datatype, __D_MPI_REQUEST * request)
#endif
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_core_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_iwrite_shared)(fh, buf, count,
        datatype, request);
    tm2 = darshan_core_wtime();

    MPIIO_LOCK();
    mpiio_runtime_initialize();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_NB_WRITES, tm1, tm2);
    MPIIO_UNLOCK();
    return(ret);
}

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
            file->file_record->counters[MPIIO_SYNCS] += 1;
            DARSHAN_TIMER_INC_NO_OVERLAP(
                file->file_record->fcounters[MPIIO_F_WRITE_TIME],
                tm1, tm2, file->last_write_end);
        }
        MPIIO_UNLOCK();
    }

    return(ret);
}

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
            file->file_record->counters[MPIIO_VIEWS] += 1;
            if(info != MPI_INFO_NULL)
            {
                file->file_record->counters[MPIIO_HINTS] += 1;
                DARSHAN_TIMER_INC_NO_OVERLAP(
                    file->file_record->fcounters[MPIIO_F_META_TIME],
                    tm1, tm2, file->last_meta_end);
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
        file->file_record->fcounters[MPIIO_F_CLOSE_TIMESTAMP] =
            darshan_core_wtime();
        DARSHAN_TIMER_INC_NO_OVERLAP(
            file->file_record->fcounters[MPIIO_F_META_TIME],
            tm1, tm2, file->last_meta_end);
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
        &my_rank,
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

    return;
}

/* get a MPIIO file record for the given file path */
static struct mpiio_file_runtime* mpiio_file_by_name(const char *name)
{
    struct mpiio_file_runtime *file = NULL;
    char *newname = NULL;
    darshan_record_id file_id;
    int limit_flag;

    if(!mpiio_runtime || instrumentation_disabled)
        return(NULL);

    newname = darshan_clean_file_path(name);
    if(!newname)
        newname = (char*)name;

    limit_flag = (mpiio_runtime->file_array_ndx >= mpiio_runtime->file_array_size);

    /* get a unique id for this file from darshan core */
    darshan_core_register_record(
        (void*)newname,
        strlen(newname),
        DARSHAN_MPIIO_MOD,
        1,
        limit_flag,
        &file_id,
        NULL);

    /* the file record id is set to 0 if no memory is available for tracking
     * new records -- just fall through and ignore this record
     */
    if(file_id == 0)
    {
        if(newname != name)
            free(newname);
        return(NULL);
    }

    /* search the hash table for this file record, and return if found */
    HASH_FIND(hlink, mpiio_runtime->file_hash, &file_id, sizeof(darshan_record_id), file);
    if(file)
    {
        if(newname != name)
            free(newname);
        return(file);
    }

    /* no existing record, assign a new file record from the global array */
    file = &(mpiio_runtime->file_runtime_array[mpiio_runtime->file_array_ndx]);
    file->file_record = &(mpiio_runtime->file_record_array[mpiio_runtime->file_array_ndx]);
    file->file_record->f_id = file_id;
    file->file_record->rank = my_rank;

    /* add new record to file hash table */
    HASH_ADD(hlink, mpiio_runtime->file_hash, file_record->f_id, sizeof(darshan_record_id), file);
    mpiio_runtime->file_array_ndx++;

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
static int mpiio_record_compare(const void* a_p, const void* b_p)
{
    const struct darshan_mpiio_file* a = a_p;
    const struct darshan_mpiio_file* b = b_p;

    if(a->rank < b->rank)
        return 1;
    if(a->rank > b->rank)
        return -1;

    return 0;
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
    int i, j, k;

    assert(mpiio_runtime);

    for(i=0; i<*len; i++)
    {
        memset(&tmp_file, 0, sizeof(struct darshan_mpiio_file));

        tmp_file.f_id = infile->f_id;
        tmp_file.rank = -1;

        /* sum */
        for(j=MPIIO_INDEP_OPENS; j<=MPIIO_VIEWS; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }

        tmp_file.counters[MPIIO_MODE] = infile->counters[MPIIO_MODE];

        /* sum */
        for(j=MPIIO_BYTES_READ; j<=MPIIO_RW_SWITCHES; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }

        /* skip MPIIO_MAX_*_TIME_SIZE; handled in floating point section */

        for(j=MPIIO_SIZE_READ_AGG_0_100; j<=MPIIO_SIZE_WRITE_AGG_1G_PLUS; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }

        /* first collapse any duplicates */
        for(j=MPIIO_ACCESS1_ACCESS; j<=MPIIO_ACCESS4_ACCESS; j++)
        {
            for(k=MPIIO_ACCESS1_ACCESS; k<=MPIIO_ACCESS4_ACCESS; k++)
            {
                if(infile->counters[j] == inoutfile->counters[k])
                {
                    infile->counters[j+4] += inoutfile->counters[k+4];
                    inoutfile->counters[k] = 0;
                    inoutfile->counters[k+4] = 0;
                }
            }
        }

        /* first set */
        for(j=MPIIO_ACCESS1_ACCESS; j<=MPIIO_ACCESS4_ACCESS; j++)
        {
            DARSHAN_COMMON_VAL_COUNTER_INC(&(tmp_file.counters[MPIIO_ACCESS1_ACCESS]),
                &(tmp_file.counters[MPIIO_ACCESS1_COUNT]), infile->counters[j],
                infile->counters[j+4]);
        }

        /* second set */
        for(j=MPIIO_ACCESS1_ACCESS; j<=MPIIO_ACCESS4_ACCESS; j++)
        {
            DARSHAN_COMMON_VAL_COUNTER_INC(&(tmp_file.counters[MPIIO_ACCESS1_ACCESS]),
                &(tmp_file.counters[MPIIO_ACCESS1_COUNT]), inoutfile->counters[j],
                inoutfile->counters[j+4]);
        }

        /* min non-zero (if available) value */
        for(j=MPIIO_F_OPEN_TIMESTAMP; j<=MPIIO_F_WRITE_START_TIMESTAMP; j++)
        {
            if((infile->fcounters[j] < inoutfile->fcounters[j] &&
               infile->fcounters[j] > 0) || inoutfile->fcounters[j] == 0)
                tmp_file.fcounters[j] = infile->fcounters[j];
            else
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
        }

        /* max */
        for(j=MPIIO_F_READ_END_TIMESTAMP; j<= MPIIO_F_CLOSE_TIMESTAMP; j++)
        {
            if(infile->fcounters[j] > inoutfile->fcounters[j])
                tmp_file.fcounters[j] = infile->fcounters[j];
            else
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
        }

        /* sum */
        for(j=MPIIO_F_READ_TIME; j<=MPIIO_F_META_TIME; j++)
        {
            tmp_file.fcounters[j] = infile->fcounters[j] + inoutfile->fcounters[j];
        }

        /* max (special case) */
        if(infile->fcounters[MPIIO_F_MAX_READ_TIME] >
            inoutfile->fcounters[MPIIO_F_MAX_READ_TIME])
        {
            tmp_file.fcounters[MPIIO_F_MAX_READ_TIME] =
                infile->fcounters[MPIIO_F_MAX_READ_TIME];
            tmp_file.counters[MPIIO_MAX_READ_TIME_SIZE] =
                infile->counters[MPIIO_MAX_READ_TIME_SIZE];
        }
        else
        {
            tmp_file.fcounters[MPIIO_F_MAX_READ_TIME] =
                inoutfile->fcounters[MPIIO_F_MAX_READ_TIME];
            tmp_file.counters[MPIIO_MAX_READ_TIME_SIZE] =
                inoutfile->counters[MPIIO_MAX_READ_TIME_SIZE];
        }

        if(infile->fcounters[MPIIO_F_MAX_WRITE_TIME] >
            inoutfile->fcounters[MPIIO_F_MAX_WRITE_TIME])
        {
            tmp_file.fcounters[MPIIO_F_MAX_WRITE_TIME] =
                infile->fcounters[MPIIO_F_MAX_WRITE_TIME];
            tmp_file.counters[MPIIO_MAX_WRITE_TIME_SIZE] =
                infile->counters[MPIIO_MAX_WRITE_TIME_SIZE];
        }
        else
        {
            tmp_file.fcounters[MPIIO_F_MAX_WRITE_TIME] =
                inoutfile->fcounters[MPIIO_F_MAX_WRITE_TIME];
            tmp_file.counters[MPIIO_MAX_WRITE_TIME_SIZE] =
                inoutfile->counters[MPIIO_MAX_WRITE_TIME_SIZE];
        }

        /* min (zeroes are ok here; some procs don't do I/O) */
        if(infile->fcounters[MPIIO_F_FASTEST_RANK_TIME] <
            inoutfile->fcounters[MPIIO_F_FASTEST_RANK_TIME])
        {
            tmp_file.counters[MPIIO_FASTEST_RANK] =
                infile->counters[MPIIO_FASTEST_RANK];
            tmp_file.counters[MPIIO_FASTEST_RANK_BYTES] =
                infile->counters[MPIIO_FASTEST_RANK_BYTES];
            tmp_file.fcounters[MPIIO_F_FASTEST_RANK_TIME] =
                infile->fcounters[MPIIO_F_FASTEST_RANK_TIME];
        }
        else
        {
            tmp_file.counters[MPIIO_FASTEST_RANK] =
                inoutfile->counters[MPIIO_FASTEST_RANK];
            tmp_file.counters[MPIIO_FASTEST_RANK_BYTES] =
                inoutfile->counters[MPIIO_FASTEST_RANK_BYTES];
            tmp_file.fcounters[MPIIO_F_FASTEST_RANK_TIME] =
                inoutfile->fcounters[MPIIO_F_FASTEST_RANK_TIME];
        }

        /* max */
        if(infile->fcounters[MPIIO_F_SLOWEST_RANK_TIME] >
           inoutfile->fcounters[MPIIO_F_SLOWEST_RANK_TIME])
        {
            tmp_file.counters[MPIIO_SLOWEST_RANK] =
                infile->counters[MPIIO_SLOWEST_RANK];
            tmp_file.counters[MPIIO_SLOWEST_RANK_BYTES] =
                infile->counters[MPIIO_SLOWEST_RANK_BYTES];
            tmp_file.fcounters[MPIIO_F_SLOWEST_RANK_TIME] =
                infile->fcounters[MPIIO_F_SLOWEST_RANK_TIME];
        }
        else
        {
            tmp_file.counters[MPIIO_SLOWEST_RANK] =
                inoutfile->counters[MPIIO_SLOWEST_RANK];
            tmp_file.counters[MPIIO_SLOWEST_RANK_BYTES] =
                inoutfile->counters[MPIIO_SLOWEST_RANK_BYTES];
            tmp_file.fcounters[MPIIO_F_SLOWEST_RANK_TIME] =
                inoutfile->fcounters[MPIIO_F_SLOWEST_RANK_TIME];
        }

        /* update pointers */
        *inoutfile = tmp_file;
        inoutfile++;
        infile++;
    }

    return;
}

static void mpiio_shared_record_variance(MPI_Comm mod_comm,
    struct darshan_mpiio_file *inrec_array, struct darshan_mpiio_file *outrec_array,
    int shared_rec_count)
{
    MPI_Datatype var_dt;
    MPI_Op var_op;
    int i;
    struct darshan_variance_dt *var_send_buf = NULL;
    struct darshan_variance_dt *var_recv_buf = NULL;

    DARSHAN_MPI_CALL(PMPI_Type_contiguous)(sizeof(struct darshan_variance_dt),
        MPI_BYTE, &var_dt);
    DARSHAN_MPI_CALL(PMPI_Type_commit)(&var_dt);

    DARSHAN_MPI_CALL(PMPI_Op_create)(darshan_variance_reduce, 1, &var_op);

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
        var_send_buf[i].T = inrec_array[i].fcounters[MPIIO_F_READ_TIME] +
                            inrec_array[i].fcounters[MPIIO_F_WRITE_TIME] +
                            inrec_array[i].fcounters[MPIIO_F_META_TIME];
    }

    DARSHAN_MPI_CALL(PMPI_Reduce)(var_send_buf, var_recv_buf, shared_rec_count,
        var_dt, var_op, 0, mod_comm);

    if(my_rank == 0)
    {
        for(i=0; i<shared_rec_count; i++)
        {
            outrec_array[i].fcounters[MPIIO_F_VARIANCE_RANK_TIME] =
                (var_recv_buf[i].S / var_recv_buf[i].n);
        }
    }

    /* get total bytes moved variances for shared records */

    for(i=0; i<shared_rec_count; i++)
    {
        var_send_buf[i].n = 1;
        var_send_buf[i].S = 0;
        var_send_buf[i].T = (double)
                            inrec_array[i].counters[MPIIO_BYTES_READ] +
                            inrec_array[i].counters[MPIIO_BYTES_WRITTEN];
    }

    DARSHAN_MPI_CALL(PMPI_Reduce)(var_send_buf, var_recv_buf, shared_rec_count,
        var_dt, var_op, 0, mod_comm);

    if(my_rank == 0)
    {
        for(i=0; i<shared_rec_count; i++)
        {
            outrec_array[i].fcounters[MPIIO_F_VARIANCE_RANK_BYTES] =
                (var_recv_buf[i].S / var_recv_buf[i].n);
        }
    }

    DARSHAN_MPI_CALL(PMPI_Type_free)(&var_dt);
    DARSHAN_MPI_CALL(PMPI_Op_free)(&var_op);
    free(var_send_buf);
    free(var_recv_buf);

    return;
}

/* mpiio module shutdown benchmark routine */
void darshan_mpiio_shutdown_bench_setup(int test_case)
{
    char filepath[256];
    MPI_File *fh_array;
    int64_t *size_array;
    int i;
    intptr_t j;

    if(mpiio_runtime)
        mpiio_shutdown();

    mpiio_runtime_initialize();

    srand(my_rank);
    fh_array = malloc(1024 * sizeof(MPI_File));
    size_array = malloc(DARSHAN_COMMON_VAL_MAX_RUNTIME_COUNT * sizeof(int64_t));
    assert(fh_array && size_array);

    for(j = 0; j < 1024; j++)
        fh_array[j] = (MPI_File)j;
    for(i = 0; i < DARSHAN_COMMON_VAL_MAX_RUNTIME_COUNT; i++)
        size_array[i] = rand();

    switch(test_case)
    {
        case 1: /* single file-per-process */
            snprintf(filepath, 256, "fpp-0_rank-%d", my_rank);

            MPIIO_RECORD_OPEN(MPI_SUCCESS, filepath, fh_array[0], MPI_COMM_SELF,
                2, MPI_INFO_NULL, 0, 1);
            MPIIO_RECORD_WRITE(MPI_SUCCESS, fh_array[0], size_array[0], MPI_BYTE,
                MPIIO_INDEP_WRITES, 1, 2);

            break;
        case 2: /* single shared file */
            snprintf(filepath, 256, "shared-0");

            MPIIO_RECORD_OPEN(MPI_SUCCESS, filepath, fh_array[0], MPI_COMM_WORLD,
                2, MPI_INFO_NULL, 0, 1);
            MPIIO_RECORD_WRITE(MPI_SUCCESS, fh_array[0], size_array[0], MPI_BYTE,
                MPIIO_COLL_WRITES, 1, 2);

            break;
        case 3: /* 1024 unique files per proc */
            for(i = 0; i < 1024; i++)
            {
                snprintf(filepath, 256, "fpp-%d_rank-%d", i , my_rank);

                MPIIO_RECORD_OPEN(MPI_SUCCESS, filepath, fh_array[i], MPI_COMM_SELF,
                    2, MPI_INFO_NULL, 0, 1);
                MPIIO_RECORD_WRITE(MPI_SUCCESS, fh_array[i],
                    size_array[i % DARSHAN_COMMON_VAL_MAX_RUNTIME_COUNT],
                    MPI_BYTE, MPIIO_INDEP_WRITES, 1, 2);
            }

            break;
        case 4: /* 1024 shared files per proc */
            for(i = 0; i < 1024; i++)
            {
                snprintf(filepath, 256, "shared-%d", i);

                MPIIO_RECORD_OPEN(MPI_SUCCESS, filepath, fh_array[i], MPI_COMM_WORLD,
                    2, MPI_INFO_NULL, 0, 1);
                MPIIO_RECORD_WRITE(MPI_SUCCESS, fh_array[i],
                    size_array[i % DARSHAN_COMMON_VAL_MAX_RUNTIME_COUNT],
                    MPI_BYTE, MPIIO_COLL_WRITES, 1, 2);
            }
            break;
        default:
            fprintf(stderr, "Error: invalid Darshan benchmark test case.\n");
            return;
    }

    return;
}

/**************************************************************************
 * Functions exported by MPI-IO module for coordinating with darshan-core *
 **************************************************************************/

static void mpiio_begin_shutdown()
{
    assert(mpiio_runtime);

    MPIIO_LOCK();
    /* disable further instrumentation while Darshan shuts down */
    instrumentation_disabled = 1;
    MPIIO_UNLOCK();

    return;
}

static void mpiio_get_output_data(
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count,
    void **mpiio_buf,
    int *mpiio_buf_sz)
{
    struct mpiio_file_runtime *file;
    struct mpiio_file_runtime* tmp;
    int i;
    double mpiio_time;
    void *red_send_buf = NULL;
    void *red_recv_buf = NULL;
    MPI_Datatype red_type;
    MPI_Op red_op;

    assert(mpiio_runtime);

    /* go through and set the 4 most common access sizes for MPI-IO */
    for(i = 0; i < mpiio_runtime->file_array_ndx; i++)
    {
        tmp = &(mpiio_runtime->file_runtime_array[i]);

        /* common access sizes */
        darshan_walk_common_vals(tmp->access_root,
            &(tmp->file_record->counters[MPIIO_ACCESS1_ACCESS]),
            &(tmp->file_record->counters[MPIIO_ACCESS1_COUNT]));
    }

    /* if there are globally shared files, do a shared file reduction */
    /* NOTE: the shared file reduction is also skipped if the 
     * DARSHAN_DISABLE_SHARED_REDUCTION environment variable is set.
     */
    if(shared_rec_count && !getenv("DARSHAN_DISABLE_SHARED_REDUCTION"))
    {
        /* necessary initialization of shared records */
        for(i = 0; i < shared_rec_count; i++)
        {
            HASH_FIND(hlink, mpiio_runtime->file_hash, &shared_recs[i],
                sizeof(darshan_record_id), file);
            assert(file);

            mpiio_time =
                file->file_record->fcounters[MPIIO_F_READ_TIME] +
                file->file_record->fcounters[MPIIO_F_WRITE_TIME] +
                file->file_record->fcounters[MPIIO_F_META_TIME];

            /* initialize fastest/slowest info prior to the reduction */
            file->file_record->counters[MPIIO_FASTEST_RANK] =
                file->file_record->rank;
            file->file_record->counters[MPIIO_FASTEST_RANK_BYTES] =
                file->file_record->counters[MPIIO_BYTES_READ] +
                file->file_record->counters[MPIIO_BYTES_WRITTEN];
            file->file_record->fcounters[MPIIO_F_FASTEST_RANK_TIME] =
                mpiio_time;

            /* until reduction occurs, we assume that this rank is both
             * the fastest and slowest. It is up to the reduction operator
             * to find the true min and max.
             */
            file->file_record->counters[MPIIO_SLOWEST_RANK] =
                file->file_record->counters[MPIIO_FASTEST_RANK];
            file->file_record->counters[MPIIO_SLOWEST_RANK_BYTES] =
                file->file_record->counters[MPIIO_FASTEST_RANK_BYTES];
            file->file_record->fcounters[MPIIO_F_SLOWEST_RANK_TIME] =
                file->file_record->fcounters[MPIIO_F_FASTEST_RANK_TIME];

            file->file_record->rank = -1;
        }

        /* sort the array of files descending by rank so that we get all of the 
         * shared files (marked by rank -1) in a contiguous portion at end 
         * of the array
         */
        qsort(mpiio_runtime->file_record_array, mpiio_runtime->file_array_ndx,
            sizeof(struct darshan_mpiio_file), mpiio_record_compare);

        /* make *send_buf point to the shared files at the end of sorted array */
        red_send_buf =
            &(mpiio_runtime->file_record_array[mpiio_runtime->file_array_ndx-shared_rec_count]);

        /* allocate memory for the reduction output on rank 0 */
        if(my_rank == 0)
        {
            red_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_mpiio_file));
            if(!red_recv_buf)
            {
                return;
            }
        }

        /* construct a datatype for a MPIIO file record.  This is serving no purpose
         * except to make sure we can do a reduction on proper boundaries
         */
        DARSHAN_MPI_CALL(PMPI_Type_contiguous)(sizeof(struct darshan_mpiio_file),
            MPI_BYTE, &red_type);
        DARSHAN_MPI_CALL(PMPI_Type_commit)(&red_type);

        /* register a MPIIO file record reduction operator */
        DARSHAN_MPI_CALL(PMPI_Op_create)(mpiio_record_reduction_op, 1, &red_op);

        /* reduce shared MPIIO file records */
        DARSHAN_MPI_CALL(PMPI_Reduce)(red_send_buf, red_recv_buf,
            shared_rec_count, red_type, red_op, 0, mod_comm);

        /* get the time and byte variances for shared files */
        mpiio_shared_record_variance(mod_comm, red_send_buf, red_recv_buf,
            shared_rec_count);

        /* clean up reduction state */
        if(my_rank == 0)
        {
            int tmp_ndx = mpiio_runtime->file_array_ndx - shared_rec_count;
            memcpy(&(mpiio_runtime->file_record_array[tmp_ndx]), red_recv_buf,
                shared_rec_count * sizeof(struct darshan_mpiio_file));
            free(red_recv_buf);
        }
        else
        {
            mpiio_runtime->file_array_ndx -= shared_rec_count;
        }

        DARSHAN_MPI_CALL(PMPI_Type_free)(&red_type);
        DARSHAN_MPI_CALL(PMPI_Op_free)(&red_op);
    }

    *mpiio_buf = (void *)(mpiio_runtime->file_record_array);
    *mpiio_buf_sz = mpiio_runtime->file_array_ndx * sizeof(struct darshan_mpiio_file);

    return;
}

static void mpiio_shutdown()
{
    struct mpiio_file_runtime_ref *ref, *tmp;

    assert(mpiio_runtime);

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
    instrumentation_disabled = 0;

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
