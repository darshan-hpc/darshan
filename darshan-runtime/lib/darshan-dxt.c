/*
 * Copyright (C) 2016 Intel Corporation.
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
#include <sys/uio.h>
#include <sys/mman.h>
#include <search.h>
#include <assert.h>
#include <libgen.h>
#include <pthread.h>

#include "utlist.h"
#include "uthash.h"
#include "darshan.h"
#include "darshan-dynamic.h"

#ifndef HAVE_OFF64_T
typedef int64_t off64_t;
#endif

/* maximum amount of memory to use for storing DXT records */
#define DXT_IO_TRACE_MEM_MAX (4 * 1024 * 1024) /* 4 MiB */

/* initial size of read/write trace buffer (in number of segments) */
/* NOTE: when this size is exceeded, the buffer size is doubled */
#define IO_TRACE_BUF_SIZE       64

/* The dxt_file_record_ref structure maintains necessary runtime metadata
 * for the DXT file record (dxt_file_record structure, defined in
 * darshan-dxt-log-format.h) pointed to by 'file_rec'. This metadata
 * assists with the instrumenting of specific statistics in the file record.
 *
 * NOTE: we use the 'darshan_record_ref' interface (in darshan-common) to
 * associate different types of handles with this dxt_file_record_ref struct.
 * This allows us to index this struct (and the underlying file record) by using
 * either the corresponding Darshan record identifier (derived from the filename)
 * or by a generated file descriptor, for instance. Note that, while there should
 * only be a single Darshan record identifier that indexes a dxt_file_record_ref,
 * there could be multiple open file descriptors that index it.
 */
struct dxt_file_record_ref
{
    struct dxt_file_record *file_rec;

    int64_t write_available_buf;
    int64_t read_available_buf;

    segment_info *write_traces;
    segment_info *read_traces;
};

/* The dxt_runtime structure maintains necessary state for storing
 * DXT file records and for coordinating with darshan-core at
 * shutdown time.
 */
struct dxt_posix_runtime
{
    void *rec_id_hash;
    int file_rec_count;
    char *record_buf;
    int record_buf_size;
};

struct dxt_mpiio_runtime
{
    void *rec_id_hash;
    int file_rec_count;
    char *record_buf;
    int record_buf_size;
};

/* dxt read/write instrumentation wrappers for POSIX and MPI-IO */
void dxt_posix_write(darshan_record_id rec_id, int64_t offset,
        int64_t length, double start_time, double end_time);
void dxt_posix_read(darshan_record_id rec_id, int64_t offset,
        int64_t length, double start_time, double end_time);
void dxt_mpiio_write(darshan_record_id rec_id, int64_t length,
        double start_time, double end_time);
void dxt_mpiio_read(darshan_record_id rec_id, int64_t length,
        double start_time, double end_time);

static void check_wr_trace_buf(
    struct dxt_file_record_ref *rec_ref);
static void check_rd_trace_buf(
    struct dxt_file_record_ref *rec_ref);
static void dxt_posix_runtime_initialize(
    void);
static void dxt_mpiio_runtime_initialize(
    void);
static struct dxt_file_record_ref *dxt_posix_track_new_file_record(
    darshan_record_id rec_id);
static struct dxt_file_record_ref *dxt_mpiio_track_new_file_record(
    darshan_record_id rec_id);
static void dxt_posix_cleanup_runtime(
    void);
static void dxt_mpiio_cleanup_runtime(
    void);

static void dxt_posix_shutdown(
    MPI_Comm mod_comm, darshan_record_id *shared_recs,
    int shared_rec_count, void **dxt_buf, int *dxt_buf_sz);
static void dxt_mpiio_shutdown(
    MPI_Comm mod_comm, darshan_record_id *shared_recs,
    int shared_rec_count, void **dxt_buf, int *dxt_buf_sz);

static struct dxt_posix_runtime *dxt_posix_runtime = NULL;
static struct dxt_mpiio_runtime *dxt_mpiio_runtime = NULL;
static pthread_mutex_t dxt_runtime_mutex =
            PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

static int posix_my_rank = -1;
static int mpiio_my_rank = -1;
static int instrumentation_disabled = 0;
static int darshan_mem_alignment = 1;
static int dxt_mem_remaining = DXT_IO_TRACE_MEM_MAX;

#define DXT_LOCK() pthread_mutex_lock(&dxt_runtime_mutex)
#define DXT_UNLOCK() pthread_mutex_unlock(&dxt_runtime_mutex)


/**********************************************************
 *      Wrappers for DXT I/O functions of interest      *
 **********************************************************/

static void check_wr_trace_buf(struct dxt_file_record_ref *rec_ref)
{
    struct dxt_file_record *file_rec = rec_ref->file_rec;

    int write_count = file_rec->write_count;
    int write_available_buf = rec_ref->write_available_buf;

    if (write_count >= write_available_buf) {
        int write_count_inc;
        if(write_available_buf == 0)
            write_count_inc = IO_TRACE_BUF_SIZE;
        else
            write_count_inc = write_available_buf;

        DXT_LOCK();
        if((write_count_inc * sizeof(segment_info)) > dxt_mem_remaining)
            write_count_inc = dxt_mem_remaining / sizeof(segment_info);

        dxt_mem_remaining -= (write_count_inc * sizeof(segment_info));
        DXT_UNLOCK();

        if(write_count_inc > 0)
        {
            write_available_buf += write_count_inc;
            rec_ref->write_traces =
                (segment_info *)realloc(rec_ref->write_traces,
                        write_available_buf * sizeof(segment_info));

            rec_ref->write_available_buf = write_available_buf;
        }
    }
}

static void check_rd_trace_buf(struct dxt_file_record_ref *rec_ref)
{
    struct dxt_file_record *file_rec = rec_ref->file_rec;

    int read_count = file_rec->read_count;
    int read_available_buf = rec_ref->read_available_buf;

    if (read_count >= read_available_buf) {
        int read_count_inc;
        if(read_available_buf == 0)
            read_count_inc = IO_TRACE_BUF_SIZE;
        else
            read_count_inc = read_available_buf;

        DXT_LOCK();
        if((read_count_inc * sizeof(segment_info)) > dxt_mem_remaining)
            read_count_inc = dxt_mem_remaining / sizeof(segment_info);

        dxt_mem_remaining -= (read_count_inc * sizeof(segment_info));
        DXT_UNLOCK();

        if(read_count_inc > 0)
        {
            read_available_buf += read_count_inc;
            rec_ref->read_traces =
                (segment_info *)realloc(rec_ref->read_traces,
                        read_available_buf * sizeof(segment_info));
            
            rec_ref->read_available_buf = read_available_buf;
        }
    }
}

void dxt_posix_write(darshan_record_id rec_id, int64_t offset,
        int64_t length, double start_time, double end_time)
{
    struct dxt_file_record_ref* rec_ref = NULL;
    struct dxt_file_record *file_rec;

    /* make sure dxt posix runtime is initialized properly */
    if(instrumentation_disabled) return;
    if(!dxt_posix_runtime)
    {
        dxt_posix_runtime_initialize();
        if(!dxt_posix_runtime) return;
    }

    rec_ref = darshan_lookup_record_ref(dxt_posix_runtime->rec_id_hash,
        &rec_id, sizeof(darshan_record_id));
    if(!rec_ref)
    {
        /* track new dxt file record */
        rec_ref = dxt_posix_track_new_file_record(rec_id);
        if(!rec_ref) return;
    }

    file_rec = rec_ref->file_rec;
    check_wr_trace_buf(rec_ref);
    if(file_rec->write_count == rec_ref->write_available_buf)
        return; /* no more memory for i/o segments ... back out */

    rec_ref->write_traces[file_rec->write_count].offset = offset;
    rec_ref->write_traces[file_rec->write_count].length = length;
    rec_ref->write_traces[file_rec->write_count].start_time = start_time;
    rec_ref->write_traces[file_rec->write_count].end_time = end_time;
    file_rec->write_count += 1;
}

void dxt_posix_read(darshan_record_id rec_id, int64_t offset,
        int64_t length, double start_time, double end_time)
{
    struct dxt_file_record_ref* rec_ref = NULL;
    struct dxt_file_record *file_rec;

    /* make sure dxt posix runtime is initialized properly */
    if(instrumentation_disabled) return;
    if(!dxt_posix_runtime)
    {
        dxt_posix_runtime_initialize();
        if(!dxt_posix_runtime) return;
    }

    rec_ref = darshan_lookup_record_ref(dxt_posix_runtime->rec_id_hash,
                &rec_id, sizeof(darshan_record_id));
    if (!rec_ref) {
        /* track new dxt file record */
        rec_ref = dxt_posix_track_new_file_record(rec_id);
        if(!rec_ref) return;
    }

    file_rec = rec_ref->file_rec;
    check_rd_trace_buf(rec_ref);
    if(file_rec->read_count == rec_ref->read_available_buf)
        return; /* no more memory for i/o segments ... back out */

    rec_ref->read_traces[file_rec->read_count].offset = offset;
    rec_ref->read_traces[file_rec->read_count].length = length;
    rec_ref->read_traces[file_rec->read_count].start_time = start_time;
    rec_ref->read_traces[file_rec->read_count].end_time = end_time;
    file_rec->read_count += 1;
}

void dxt_mpiio_write(darshan_record_id rec_id, int64_t length,
        double start_time, double end_time)
{
    struct dxt_file_record_ref* rec_ref = NULL;
    struct dxt_file_record *file_rec;

    /* make sure dxt mpiio runtime is initialized properly */
    if(instrumentation_disabled) return;
    if(!dxt_mpiio_runtime)
    {
        dxt_mpiio_runtime_initialize();
        if(!dxt_mpiio_runtime) return;
    }

    rec_ref = darshan_lookup_record_ref(dxt_mpiio_runtime->rec_id_hash,
                &rec_id, sizeof(darshan_record_id));
    if(!rec_ref)
    {
        /* track new dxt file record */
        rec_ref = dxt_mpiio_track_new_file_record(rec_id);
        if(!rec_ref) return;
    }

    file_rec = rec_ref->file_rec;
    check_wr_trace_buf(rec_ref);
    if(file_rec->write_count == rec_ref->write_available_buf)
        return; /* no more memory for i/o segments ... back out */

    rec_ref->write_traces[file_rec->write_count].length = length;
    rec_ref->write_traces[file_rec->write_count].start_time = start_time;
    rec_ref->write_traces[file_rec->write_count].end_time = end_time;
    file_rec->write_count += 1;
}

void dxt_mpiio_read(darshan_record_id rec_id, int64_t length,
        double start_time, double end_time)
{
    struct dxt_file_record_ref* rec_ref = NULL;
    struct dxt_file_record *file_rec;

    /* make sure dxt mpiio runtime is initialized properly */
    if(instrumentation_disabled) return;
    if(!dxt_mpiio_runtime)
    {
        dxt_mpiio_runtime_initialize();
        if(!dxt_mpiio_runtime) return;
    }

    rec_ref = darshan_lookup_record_ref(dxt_mpiio_runtime->rec_id_hash,
                &rec_id, sizeof(darshan_record_id));
    if(!rec_ref)
    {
        /* track new dxt file record */
        rec_ref = dxt_mpiio_track_new_file_record(rec_id);
        if(!rec_ref) return;
    }

    file_rec = rec_ref->file_rec;
    check_rd_trace_buf(rec_ref);
    if(file_rec->read_count == rec_ref->read_available_buf)
        return; /* no more memory for i/o segments ... back out */

    rec_ref->read_traces[file_rec->read_count].length = length;
    rec_ref->read_traces[file_rec->read_count].start_time = start_time;
    rec_ref->read_traces[file_rec->read_count].end_time = end_time;
    file_rec->read_count += 1;
}


/**********************************************************
 * Internal functions for manipulating DXT module state *
 **********************************************************/

/* initialize internal DXT module data structures and register with darshan-core */
static void dxt_posix_runtime_initialize()
{
    /* DXT modules request 0 memory -- buffers will be managed internally by DXT
     * and passed back to darshan-core at shutdown time to allow DXT more control
     * over realloc'ing module memory as needed.
     */
    int dxt_psx_buf_size = 0;

    /* register the DXT module with darshan core */
    darshan_core_register_module(
        DXT_POSIX_MOD,
        &dxt_posix_shutdown,
        &dxt_psx_buf_size,
        &posix_my_rank,
        &darshan_mem_alignment);

    /* return if darshan-core allocates an unexpected amount of memory */
    if(dxt_psx_buf_size != 0)
    {
        darshan_core_unregister_module(DXT_POSIX_MOD);
        return;
    }

    dxt_posix_runtime = malloc(sizeof(*dxt_posix_runtime));
    if(!dxt_posix_runtime)
    {
        darshan_core_unregister_module(DXT_POSIX_MOD);
        return;
    }
    memset(dxt_posix_runtime, 0, sizeof(*dxt_posix_runtime));

    return;
}

void dxt_mpiio_runtime_initialize()
{
    /* DXT modules request 0 memory -- buffers will be managed internally by DXT
     * and passed back to darshan-core at shutdown time to allow DXT more control
     * over realloc'ing module memory as needed.
     */
    int dxt_mpiio_buf_size = 0;

    /* register the DXT module with darshan core */
    darshan_core_register_module(
        DXT_MPIIO_MOD,
        &dxt_mpiio_shutdown,
        &dxt_mpiio_buf_size,
        &mpiio_my_rank,
        &darshan_mem_alignment);

    /* return if darshan-core allocates an unexpected amount of memory */
    if(dxt_mpiio_buf_size != 0)
    {
        darshan_core_unregister_module(DXT_MPIIO_MOD);
        return;
    }

    dxt_mpiio_runtime = malloc(sizeof(*dxt_mpiio_runtime));
    if(!dxt_mpiio_runtime)
    {
        darshan_core_unregister_module(DXT_MPIIO_MOD);
        return;
    }
    memset(dxt_mpiio_runtime, 0, sizeof(*dxt_mpiio_runtime));

    return;
}

static struct dxt_file_record_ref *dxt_posix_track_new_file_record(
    darshan_record_id rec_id)
{
    struct dxt_file_record_ref *rec_ref = NULL;
    struct dxt_file_record *file_rec = NULL;
    int ret;

    /* check if we have enough room for a new DXT record */
    DXT_LOCK();
    if(dxt_mem_remaining < sizeof(struct dxt_file_record))
    {
        DXT_UNLOCK();
        return(NULL);
    }

    rec_ref = malloc(sizeof(*rec_ref));
    if(!rec_ref)
    {
        DXT_UNLOCK();
        return(NULL);
    }
    memset(rec_ref, 0, sizeof(*rec_ref));

    file_rec = malloc(sizeof(*file_rec));
    if(!file_rec)
    {
        free(rec_ref);
        DXT_UNLOCK();
        return(NULL);
    }
    memset(file_rec, 0, sizeof(*file_rec));

    /* add a reference to this file record based on record id */
    ret = darshan_add_record_ref(&(dxt_posix_runtime->rec_id_hash), &rec_id,
            sizeof(darshan_record_id), rec_ref);
    if(ret == 0)
    {
        free(file_rec);
        free(rec_ref);
        DXT_UNLOCK();
        return(NULL);
    }

    dxt_mem_remaining -= sizeof(struct dxt_file_record);
    DXT_UNLOCK();

    /* initialize record and record reference fields */
    file_rec->base_rec.id = rec_id;
    file_rec->base_rec.rank = posix_my_rank;
    gethostname(file_rec->hostname, HOSTNAME_SIZE);

    rec_ref->file_rec = file_rec;
    dxt_posix_runtime->file_rec_count++;

    return(rec_ref);
}

static struct dxt_file_record_ref *dxt_mpiio_track_new_file_record(
    darshan_record_id rec_id)
{
    struct dxt_file_record *file_rec = NULL;
    struct dxt_file_record_ref *rec_ref = NULL;
    int ret;

    /* check if we have enough room for a new DXT record */
    DXT_LOCK();
    if(dxt_mem_remaining < sizeof(struct dxt_file_record))
    {
        DXT_UNLOCK();
        return(NULL);
    }

    rec_ref = malloc(sizeof(*rec_ref));
    if(!rec_ref)
    {
        DXT_UNLOCK();
        return(NULL);
    }
    memset(rec_ref, 0, sizeof(*rec_ref));

    file_rec = malloc(sizeof(*file_rec));
    if(!file_rec)
    {
        free(rec_ref);
        DXT_UNLOCK();
        return(NULL);
    }
    memset(file_rec, 0, sizeof(*file_rec));

    /* add a reference to this file record based on record id */
    ret = darshan_add_record_ref(&(dxt_mpiio_runtime->rec_id_hash), &rec_id,
            sizeof(darshan_record_id), rec_ref);
    if(ret == 0)
    {
        free(file_rec);
        free(rec_ref);
        DXT_UNLOCK();
        return(NULL);
    }

    dxt_mem_remaining -= sizeof(struct dxt_file_record);
    DXT_UNLOCK();

    /* initialize record and record reference fields */
    file_rec->base_rec.id = rec_id;
    file_rec->base_rec.rank = mpiio_my_rank;
    gethostname(file_rec->hostname, HOSTNAME_SIZE);

    rec_ref->file_rec = file_rec;
    dxt_mpiio_runtime->file_rec_count++;

    return(rec_ref);
}

static void dxt_free_record_data(void *rec_ref_p)
{
    struct dxt_file_record_ref *dxt_rec_ref = (struct dxt_file_record_ref *)rec_ref_p;

    /* TODO: update these pointer addresses once {write/read}_traces are moved to rec_ref structure */
    free(dxt_rec_ref->write_traces);
    free(dxt_rec_ref->read_traces);
    free(dxt_rec_ref->file_rec);
}

static void dxt_posix_cleanup_runtime()
{
    darshan_iter_record_refs(dxt_posix_runtime->rec_id_hash, dxt_free_record_data);
    darshan_clear_record_refs(&(dxt_posix_runtime->rec_id_hash), 1);

    free(dxt_posix_runtime);
    dxt_posix_runtime = NULL;

    return;
}

static void dxt_mpiio_cleanup_runtime()
{
    darshan_iter_record_refs(dxt_mpiio_runtime->rec_id_hash, dxt_free_record_data);
    darshan_clear_record_refs(&(dxt_mpiio_runtime->rec_id_hash), 1);

    free(dxt_mpiio_runtime);
    dxt_mpiio_runtime = NULL;

    return;
}


/********************************************************************************
 * shutdown function exported by this module for coordinating with darshan-core *
 ********************************************************************************/

static void dxt_serialize_posix_records(void *rec_ref_p)
{
    struct dxt_file_record_ref *rec_ref = (struct dxt_file_record_ref *)rec_ref_p;
    struct dxt_file_record *file_rec;
    int64_t record_size = 0;
    int64_t record_write_count = 0;
    int64_t record_read_count = 0;
    void *tmp_buf_ptr;

    assert(rec_ref);
    file_rec = rec_ref->file_rec;
    assert(file_rec);

    record_write_count = file_rec->write_count;
    record_read_count = file_rec->read_count;
    if (record_write_count == 0 && record_read_count == 0)
        return;

    /*
     * Buffer format:
     * dxt_file_record + write_traces + read_traces
     */
    record_size = sizeof(struct dxt_file_record) +
            (record_write_count + record_read_count) * sizeof(segment_info);

    tmp_buf_ptr = (void *)(dxt_posix_runtime->record_buf +
        dxt_posix_runtime->record_buf_size);

    /*Copy struct dxt_file_record */
    memcpy(tmp_buf_ptr, (void *)file_rec, sizeof(struct dxt_file_record));
    tmp_buf_ptr = (void *)(tmp_buf_ptr + sizeof(struct dxt_file_record));

    /*Copy write record */
    memcpy(tmp_buf_ptr, (void *)(rec_ref->write_traces),
            record_write_count * sizeof(segment_info));
    tmp_buf_ptr = (void *)(tmp_buf_ptr +
                record_write_count * sizeof(segment_info));

    /*Copy read record */
    memcpy(tmp_buf_ptr, (void *)(rec_ref->read_traces),
            record_read_count * sizeof(segment_info));
    tmp_buf_ptr = (void *)(tmp_buf_ptr +
                record_read_count * sizeof(segment_info));

    dxt_posix_runtime->record_buf_size += record_size;

#if 0
    int i;
    int64_t rank;
    char *hostname;
    int64_t offset;
    int64_t length;
    double start_time;
    double end_time;

    rank = file_rec->base_rec.rank;
    hostname = file_rec->hostname;

    printf("X_POSIX, record_id: %" PRIu64 "\n", rec_ref->file_rec->base_rec.id);
    printf("X_POSIX, write_count is: %d read_count is: %d\n",
                file_rec->write_count, file_rec->read_count);
    printf("X_POSIX, rank: %d hostname: %s\n", rank, hostname);

    for (i = 0; i < file_rec->write_count; i++) {
        offset = rec_ref->write_traces[i].offset;
        length = rec_ref->write_traces[i].length;
        start_time = rec_ref->write_traces[i].start_time;
        end_time = rec_ref->write_traces[i].end_time;

        printf("X_POSIX, rank %d writes segment %lld [offset: %lld length: %lld start_time: %fs end_time: %fs]\n", rank, i, offset, length, start_time, end_time);
    }

    for (i = 0; i < file_rec->read_count; i++) {
        offset = rec_ref->read_traces[i].offset;
        length = rec_ref->read_traces[i].length;
        start_time = rec_ref->read_traces[i].start_time;
        end_time = rec_ref->read_traces[i].end_time;

        printf("X_POSIX, rank %d reads segment %lld [offset: %lld length: %lld start_time: %fs end_time: %fs]\n", rank, i, offset, length, start_time, end_time);
    }
#endif
}

static void dxt_posix_shutdown(
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count,
    void **dxt_posix_buf,
    int *dxt_posix_buf_sz)
{
    assert(dxt_posix_runtime);

    *dxt_posix_buf_sz = 0;

    dxt_posix_runtime->record_buf = malloc(DXT_IO_TRACE_MEM_MAX);
    if(!(dxt_posix_runtime->record_buf))
        return;
    memset(dxt_posix_runtime->record_buf, 0, DXT_IO_TRACE_MEM_MAX);
    dxt_posix_runtime->record_buf_size = 0;

    /* iterate all dxt posix records and serialize them to the output buffer */
    darshan_iter_record_refs(dxt_posix_runtime->rec_id_hash, dxt_serialize_posix_records);

    /* set output */
    *dxt_posix_buf = dxt_posix_runtime->record_buf;
    *dxt_posix_buf_sz = dxt_posix_runtime->record_buf_size;

    /* shutdown internal structures used for instrumenting */
    dxt_posix_cleanup_runtime();

    /* disable further instrumentation */
    instrumentation_disabled = 1;

    return;
}

static void dxt_serialize_mpiio_records(void *rec_ref_p)
{
    struct dxt_file_record_ref *rec_ref = (struct dxt_file_record_ref *)rec_ref_p;
    struct dxt_file_record *file_rec;
    int64_t record_size = 0;
    int64_t record_write_count = 0;
    int64_t record_read_count = 0;
    void *tmp_buf_ptr;

    assert(rec_ref);
    file_rec = rec_ref->file_rec;
    assert(file_rec);

    record_write_count = file_rec->write_count;
    record_read_count = file_rec->read_count;
    if (record_write_count == 0 && record_read_count == 0)
        return;

    /*
     * Buffer format:
     * dxt_file_record + write_traces + read_traces
     */
    record_size = sizeof(struct dxt_file_record) +
            (record_write_count + record_read_count) * sizeof(segment_info);

    tmp_buf_ptr = (void *)(dxt_mpiio_runtime->record_buf +
        dxt_mpiio_runtime->record_buf_size);

    /*Copy struct dxt_file_record */
    memcpy(tmp_buf_ptr, (void *)file_rec, sizeof(struct dxt_file_record));
    tmp_buf_ptr = (void *)(tmp_buf_ptr + sizeof(struct dxt_file_record));

    /*Copy write record */
    memcpy(tmp_buf_ptr, (void *)(rec_ref->write_traces),
            record_write_count * sizeof(segment_info));
    tmp_buf_ptr = (void *)(tmp_buf_ptr +
                record_write_count * sizeof(segment_info));

    /*Copy read record */
    memcpy(tmp_buf_ptr, (void *)(rec_ref->read_traces),
            record_read_count * sizeof(segment_info));
    tmp_buf_ptr = (void *)(tmp_buf_ptr +
                record_read_count * sizeof(segment_info));

    dxt_mpiio_runtime->record_buf_size += record_size;

#if 0
    int i;
    int64_t rank;
    char *hostname;
    int64_t length;
    double start_time;
    double end_time;

    rank = file_rec->base_rec.rank;
    hostname = file_rec->hostname;

    printf("X_MPIIO, record_id: %" PRIu64 "\n", rec_ref->file_rec->base_rec.id);
    printf("X_MPIIO, write_count is: %d read_count is: %d\n",
                file_rec->write_count, file_rec->read_count);
    printf("X_MPIIO, rank: %d hostname: %s\n", rank, hostname);

    for (i = 0; i < file_rec->write_count; i++) {
        length = rec_ref->write_traces[i].length;
        start_time = rec_ref->write_traces[i].start_time;
        end_time = rec_ref->write_traces[i].end_time;

        printf("X_MPIIO, rank %d writes segment %lld [length: %lld start_time: %fs end_time: %fs]\n", rank, i, length, start_time, end_time);
    }

    for (i = 0; i < file_rec->read_count; i++) {
        length = rec_ref->read_traces[i].length;
        start_time = rec_ref->read_traces[i].start_time;
        end_time = rec_ref->read_traces[i].end_time;

        printf("X_MPIIO, rank %d reads segment %lld [length: %lld start_time: %fs end_time: %fs]\n", rank, i, length, start_time, end_time);
    }
#endif
}

static void dxt_mpiio_shutdown(
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count,
    void **dxt_mpiio_buf,
    int *dxt_mpiio_buf_sz)
{
    assert(dxt_mpiio_runtime);

    *dxt_mpiio_buf_sz = 0;

    dxt_mpiio_runtime->record_buf = malloc(DXT_IO_TRACE_MEM_MAX);
    if(!(dxt_mpiio_runtime->record_buf))
        return;
    memset(dxt_mpiio_runtime->record_buf, 0, DXT_IO_TRACE_MEM_MAX);
    dxt_mpiio_runtime->record_buf_size = 0;

    /* iterate all dxt posix records and serialize them to the output buffer */
    darshan_iter_record_refs(dxt_mpiio_runtime->rec_id_hash, dxt_serialize_mpiio_records);

    /* set output */ 
    *dxt_mpiio_buf = dxt_mpiio_runtime->record_buf;
    *dxt_mpiio_buf_sz = dxt_mpiio_runtime->record_buf_size;

    /* shutdown internal structures used for instrumenting */
    dxt_mpiio_cleanup_runtime();

    /* disable further instrumentation */
    instrumentation_disabled = 1;

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
