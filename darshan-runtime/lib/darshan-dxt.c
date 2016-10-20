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

#ifdef DARSHAN_LUSTRE
#include <lustre/lustre_user.h>
#endif

#define IO_TRACE_BUF_SIZE       1024

struct dxt_record_ref_tracker
{
    void *rec_ref_p;
    UT_hash_handle hlink;
};

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
    int fs_type; /* same as darshan_fs_info->fs_type */
};

/* The dxt_runtime structure maintains necessary state for storing
 * DXT file records and for coordinating with darshan-core at
 * shutdown time.
 */
struct dxt_posix_runtime
{
    void *rec_id_hash;
    void *fd_hash;
    int file_rec_count;
};

struct dxt_mpiio_runtime
{
    void *rec_id_hash;
    void *fh_hash;
    int file_rec_count;
};

void dxt_posix_runtime_initialize(
    void);
void dxt_mpiio_runtime_initialize(
    void);
void dxt_posix_track_new_file_record(
    darshan_record_id rec_id, const char *path);
void dxt_mpiio_track_new_file_record(
    darshan_record_id rec_id, const char *path);
void dxt_posix_add_record_ref(darshan_record_id rec_id, int fd);
void dxt_mpiio_add_record_ref(darshan_record_id rec_id, MPI_File fh);
#if 0
static void dxt_instrument_fs_data(
    darshan_record_id rec_id, int fs_type, struct dxt_file_record *file_rec);
#endif
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


#ifdef DARSHAN_LUSTRE
/* XXX modules don't expose an API for other modules, so use extern to get
 * Lustre instrumentation function
 */
extern void dxt_get_lustre_stripe_info(
    darshan_record_id rec_id, struct dxt_file_record *file_rec);
#endif

static struct dxt_posix_runtime *dxt_posix_runtime = NULL;
static struct dxt_mpiio_runtime *dxt_mpiio_runtime = NULL;
static pthread_mutex_t dxt_posix_runtime_mutex =
            PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static pthread_mutex_t dxt_mpiio_runtime_mutex =
            PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

static int posix_my_rank = -1;
static int mpiio_my_rank = -1;
static int instrumentation_disabled = 0;
static int darshan_mem_alignment = 1;

#define DXT_POSIX_LOCK() pthread_mutex_lock(&dxt_posix_runtime_mutex)
#define DXT_POSIX_UNLOCK() pthread_mutex_unlock(&dxt_posix_runtime_mutex)

#define DXT_MPIIO_LOCK() pthread_mutex_lock(&dxt_mpiio_runtime_mutex)
#define DXT_MPIIO_UNLOCK() pthread_mutex_unlock(&dxt_mpiio_runtime_mutex)


/**********************************************************
 *      Wrappers for DXT I/O functions of interest      *
 **********************************************************/

void check_io_trace_buf(struct dxt_file_record_ref *rec_ref)
{
    struct dxt_file_record *file_rec = rec_ref->file_rec;

    int write_count = file_rec->write_count;
    int write_available_buf = rec_ref->write_available_buf;

    if (write_count >= write_available_buf) {
        write_available_buf += IO_TRACE_BUF_SIZE;

        file_rec->write_traces =
            (segment_info *)realloc(file_rec->write_traces,
                    write_available_buf * sizeof(segment_info));

        rec_ref->write_available_buf = write_available_buf;
    }

    int read_count = file_rec->read_count;
    int read_available_buf = rec_ref->read_available_buf;

    if (read_count >= read_available_buf) {
        read_available_buf += IO_TRACE_BUF_SIZE;

        file_rec->read_traces =
            (segment_info *)realloc(file_rec->read_traces,
                    read_available_buf * sizeof(segment_info));

        rec_ref->read_available_buf = read_available_buf;
    }
}

void dxt_posix_write(int fd, int64_t offset, int64_t length,
        double start_time, double end_time)
{
    struct dxt_file_record_ref* rec_ref = NULL;
    struct dxt_file_record *file_rec;

    rec_ref = darshan_lookup_record_ref(dxt_posix_runtime->fd_hash,
                &fd, sizeof(int));
    if (!rec_ref) {
        fprintf(stderr, "Error: dxt_posix_write unable to find rec_ref.\n");
        return;
    }

    file_rec = rec_ref->file_rec;
    if (dxt_posix_runtime) {
        check_io_trace_buf(rec_ref);
    }

    file_rec->write_traces[file_rec->write_count].offset = offset;
    file_rec->write_traces[file_rec->write_count].length = length;
    file_rec->write_traces[file_rec->write_count].start_time = start_time;
    file_rec->write_traces[file_rec->write_count].end_time = end_time;
    file_rec->write_count += 1;
}

void dxt_posix_read(int fd, int64_t offset, int64_t length,
        double start_time, double end_time)
{
    struct dxt_file_record_ref* rec_ref = NULL;
    struct dxt_file_record *file_rec;

    rec_ref = darshan_lookup_record_ref(dxt_posix_runtime->fd_hash,
                &fd, sizeof(int));
    if (!rec_ref) {
        fprintf(stderr, "Error: dxt_posix_read unable to find rec_ref.\n");
        return;
    }

    file_rec = rec_ref->file_rec;
    if (dxt_posix_runtime) {
        check_io_trace_buf(rec_ref);
    }

    file_rec->read_traces[file_rec->read_count].offset = offset;
    file_rec->read_traces[file_rec->read_count].length = length;
    file_rec->read_traces[file_rec->read_count].start_time = start_time;
    file_rec->read_traces[file_rec->read_count].end_time = end_time;
    file_rec->read_count += 1;
}

void dxt_mpiio_write(MPI_File fh, int64_t length,
        double start_time, double end_time)
{
    struct dxt_file_record_ref* rec_ref = NULL;
    struct dxt_file_record *file_rec;

    rec_ref = darshan_lookup_record_ref(dxt_mpiio_runtime->fh_hash,
                &fh, sizeof(MPI_File));
    if (!rec_ref) {
        fprintf(stderr, "Error: dxt_mpiio_write unable to find rec_ref.\n");
        return;
    }

    file_rec = rec_ref->file_rec;
    if (dxt_mpiio_runtime) {
        check_io_trace_buf(rec_ref);
    }

    file_rec->write_traces[file_rec->write_count].length = length;
    file_rec->write_traces[file_rec->write_count].start_time = start_time;
    file_rec->write_traces[file_rec->write_count].end_time = end_time;
    file_rec->write_count += 1;
}

void dxt_mpiio_read(MPI_File fh, int64_t length,
        double start_time, double end_time)
{
    struct dxt_file_record_ref* rec_ref = NULL;
    struct dxt_file_record *file_rec;

    rec_ref = darshan_lookup_record_ref(dxt_mpiio_runtime->fh_hash,
                &fh, sizeof(MPI_File));
    if (!rec_ref) {
        fprintf(stderr, "Error: dxt_mpiio_read unable to find rec_ref.\n");
        return;
    }

    file_rec = rec_ref->file_rec;
    if (dxt_mpiio_runtime) {
        check_io_trace_buf(rec_ref);
    }

    file_rec->read_traces[file_rec->read_count].length = length;
    file_rec->read_traces[file_rec->read_count].start_time = start_time;
    file_rec->read_traces[file_rec->read_count].end_time = end_time;
    file_rec->read_count += 1;
}


/**********************************************************
 * Internal functions for manipulating DXT module state *
 **********************************************************/

/* initialize internal DXT module data structures and register with darshan-core */
void dxt_posix_runtime_initialize()
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

void dxt_posix_track_new_file_record(
    darshan_record_id rec_id, const char *path)
{
    struct dxt_file_record_ref *rec_ref = NULL;
    struct dxt_file_record *file_rec = NULL;
    struct darshan_fs_info fs_info;
    int ret;

    rec_ref = malloc(sizeof(*rec_ref));
    if(!rec_ref)
        return;
    memset(rec_ref, 0, sizeof(*rec_ref));

    /* add a reference to this file record based on record id */
    ret = darshan_add_record_ref(&(dxt_posix_runtime->rec_id_hash), &rec_id,
            sizeof(darshan_record_id), rec_ref);
    if(ret == 0)
    {
        free(rec_ref);
        return;
    }

    /* register the actual file record with darshan-core so it is persisted
     * in the log file
     */
    file_rec = darshan_core_register_record(
            rec_id,
            path,
            DXT_POSIX_MOD,
            sizeof(struct dxt_file_record),
            &fs_info);

    if(!file_rec)
    {
        darshan_delete_record_ref(&(dxt_posix_runtime->rec_id_hash),
                &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        return;
    }

    /*
     * Registering this file record was successful, so initialize
     * some fields */
    file_rec->base_rec.id = rec_id;
    file_rec->base_rec.rank = posix_my_rank;

    file_rec->write_count = 0;
    file_rec->write_traces = malloc(IO_TRACE_BUF_SIZE *
            sizeof(segment_info));

    file_rec->read_count = 0;
    file_rec->read_traces = malloc(IO_TRACE_BUF_SIZE *
            sizeof(segment_info));

    rec_ref->file_rec = file_rec;
    rec_ref->write_available_buf = IO_TRACE_BUF_SIZE;
    rec_ref->read_available_buf = IO_TRACE_BUF_SIZE;
    rec_ref->fs_type = fs_info.fs_type;

    dxt_posix_runtime->file_rec_count++;
}

void dxt_mpiio_track_new_file_record(
    darshan_record_id rec_id, const char *path)
{
    struct dxt_file_record *file_rec = NULL;
    struct dxt_file_record_ref *rec_ref = NULL;
    struct darshan_fs_info fs_info;
    int ret;

    rec_ref = malloc(sizeof(*rec_ref));
    if(!rec_ref)
        return;
    memset(rec_ref, 0, sizeof(*rec_ref));

    /* add a reference to this file record based on record id */
    ret = darshan_add_record_ref(&(dxt_mpiio_runtime->rec_id_hash), &rec_id,
            sizeof(darshan_record_id), rec_ref);
    if(ret == 0)
    {
        free(rec_ref);
        return;
    }

    /* register the actual file record with darshan-core so it is persisted
     * in the log file
     */
    file_rec = darshan_core_register_record(
            rec_id,
            path,
            DXT_MPIIO_MOD,
            sizeof(struct dxt_file_record),
            &fs_info);

    if(!file_rec)
    {
        darshan_delete_record_ref(&(dxt_mpiio_runtime->rec_id_hash),
                &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        return;
    }

    /* registering this file record was successful, so initialize
     * some fields
     */
    file_rec->base_rec.id = rec_id;
    file_rec->base_rec.rank = mpiio_my_rank;

    file_rec->write_count = 0;
    file_rec->write_traces = malloc(IO_TRACE_BUF_SIZE *
            sizeof(segment_info));

    file_rec->read_count = 0;
    file_rec->read_traces = malloc(IO_TRACE_BUF_SIZE *
            sizeof(segment_info));

    rec_ref->file_rec = file_rec;
    rec_ref->write_available_buf = IO_TRACE_BUF_SIZE;
    rec_ref->read_available_buf = IO_TRACE_BUF_SIZE;
    rec_ref->fs_type = fs_info.fs_type;

    dxt_mpiio_runtime->file_rec_count++;
}

void dxt_posix_add_record_ref(darshan_record_id rec_id, int fd)
{
    struct dxt_file_record_ref *rec_ref = NULL;
    struct dxt_file_record *file_rec;
    int i;

    rec_ref = darshan_lookup_record_ref(dxt_posix_runtime->rec_id_hash, &rec_id,
                sizeof(darshan_record_id));
    assert(rec_ref);

    darshan_add_record_ref(&(dxt_posix_runtime->fd_hash), &fd,
            sizeof(int), rec_ref);

#if 0
    /* get Lustre stripe information */
    file_rec = rec_ref->file_rec;
    dxt_instrument_fs_data(rec_id, rec_ref->fs_type, file_rec);
#endif
}

void dxt_mpiio_add_record_ref(darshan_record_id rec_id, MPI_File fh)
{
    struct dxt_file_record_ref *rec_ref = NULL;

    rec_ref = darshan_lookup_record_ref(dxt_mpiio_runtime->rec_id_hash, &rec_id,
                sizeof(darshan_record_id));
    assert(rec_ref);

    darshan_add_record_ref(&(dxt_mpiio_runtime->fh_hash), &fh,
            sizeof(MPI_File), rec_ref);
}

#if 0
static void dxt_instrument_fs_data(
        darshan_record_id rec_id, int fs_type, struct dxt_file_record *file_rec)
{
#ifdef DARSHAN_LUSTRE
    /* allow lustre to generate a record if we configured with lustre support */
    if(fs_type == LL_SUPER_MAGIC)
    {
        dxt_get_lustre_stripe_info(rec_id, file_rec);
        return;
    }
#endif
    return;
}
#endif

void dxt_clear_record_refs(void **hash_head_p, int free_flag)
{
    struct dxt_record_ref_tracker *ref_tracker, *tmp;
    struct dxt_record_ref_tracker *ref_tracker_head =
        *(struct dxt_record_ref_tracker **)hash_head_p;
    struct dxt_file_record_ref *rec_ref;
    struct dxt_file_record *file_rec;

#if 0    
    /* iterate the hash table and remove/free all reference trackers */
    HASH_ITER(hlink, ref_tracker_head, ref_tracker, tmp)
    {
        HASH_DELETE(hlink, ref_tracker_head, ref_tracker);
        if (free_flag) {
            rec_ref = (struct dxt_file_record_ref *)ref_tracker->rec_ref_p;
            file_rec = rec_ref->file_rec;

            if (file_rec->write_traces)
                free(file_rec->write_traces);

            if (file_rec->read_traces)
                free(file_rec->read_traces);

            free(rec_ref);
        }
        free(ref_tracker);
    }
    *hash_head_p = ref_tracker_head;
#endif

    return;
}

static void dxt_posix_cleanup_runtime()
{
    dxt_clear_record_refs(&(dxt_posix_runtime->fd_hash), 0);
    dxt_clear_record_refs(&(dxt_posix_runtime->rec_id_hash), 1);

    free(dxt_posix_runtime);
    dxt_posix_runtime = NULL;

    return;
}

static void dxt_mpiio_cleanup_runtime()
{
    dxt_clear_record_refs(&(dxt_mpiio_runtime->fh_hash), 0);
    dxt_clear_record_refs(&(dxt_mpiio_runtime->rec_id_hash), 1);

    free(dxt_mpiio_runtime);
    dxt_mpiio_runtime = NULL;

    return;
}


/********************************************************************************
 * shutdown function exported by this module for coordinating with darshan-core *
 ********************************************************************************/

static void dxt_posix_shutdown(
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count,
    void **dxt_posix_buf,
    int *dxt_posix_buf_sz)
{
    struct dxt_file_record_ref *rec_ref;
    struct dxt_file_record *file_rec;
    int i, idx;

    int64_t offset;
    int64_t length;
    int64_t rank;
    double start_time;
    double end_time;

    int64_t record_size = 0;
    int64_t record_write_count = 0;
    int64_t record_read_count = 0;
    void *tmp_buf_ptr = *dxt_posix_buf;

    assert(dxt_posix_runtime);

    DXT_POSIX_LOCK();
    int dxt_rec_count = dxt_posix_runtime->file_rec_count;
    struct dxt_record_ref_tracker *ref_tracker, *tmp;
    struct dxt_record_ref_tracker *ref_tracker_head =
        (struct dxt_record_ref_tracker *)(dxt_posix_runtime->rec_id_hash);

    *dxt_posix_buf_sz = 0;

    HASH_ITER(hlink, ref_tracker_head, ref_tracker, tmp) {
        rec_ref = (struct dxt_file_record_ref *)ref_tracker->rec_ref_p;
        assert(rec_ref);

        file_rec = rec_ref->file_rec;

        record_write_count = file_rec->write_count;
        record_read_count = file_rec->read_count;

        if (record_write_count == 0 && record_read_count == 0)
            continue;

        /*
         * Buffer format:
         * dxt_file_record + ost_ids + write_traces + read_traces
         */
        record_size = sizeof(struct dxt_file_record) +
                (record_write_count + record_read_count) * sizeof(segment_info);

        if (*dxt_posix_buf_sz == 0) {
            *dxt_posix_buf = (void *)malloc(record_size);
        } else {
            *dxt_posix_buf = (void *)realloc((*dxt_posix_buf),
                            *dxt_posix_buf_sz + record_size);
        }
        tmp_buf_ptr = (void *)(*dxt_posix_buf) + *dxt_posix_buf_sz;

        /*Copy struct dxt_file_record */
        memcpy(tmp_buf_ptr, (void *)file_rec, sizeof(struct dxt_file_record));
        tmp_buf_ptr = ((void *)tmp_buf_ptr) + sizeof(struct dxt_file_record);

        /*Copy write record */
        memcpy(tmp_buf_ptr, (void *)(file_rec->write_traces),
                record_write_count * sizeof(segment_info));
        tmp_buf_ptr = ((void *)tmp_buf_ptr) +
                    record_write_count * sizeof(segment_info);

        /*Copy read record */
        memcpy(tmp_buf_ptr, (void *)(file_rec->read_traces),
                record_read_count * sizeof(segment_info));
        tmp_buf_ptr = ((char *)tmp_buf_ptr) +
                    record_read_count * sizeof(segment_info);

        *dxt_posix_buf_sz += record_size;

#if 0
        printf("DXT, record_id: %" PRIu64 "\n", rec_ref->file_rec->base_rec.id);
        printf("DXT, write_count is: %d read_count is: %d\n",
                    file_rec->write_count, file_rec->read_count);

        for (i = 0; i < file_rec->write_count; i++) {
            rank = file_rec->base_rec.rank;
            offset = file_rec->write_traces[i].offset;
            length = file_rec->write_traces[i].length;
            start_time = file_rec->write_traces[i].start_time;
            end_time = file_rec->write_traces[i].end_time;

            printf("DXT, rank %d writes segment %lld [offset: %lld length: %lld start_time: %fs end_time: %fs]\n", rank, i, offset, length, start_time, end_time);
        }

        for (i = 0; i < file_rec->read_count; i++) {
            rank = file_rec->base_rec.rank;
            offset = file_rec->read_traces[i].offset;
            length = file_rec->read_traces[i].length;
            start_time = file_rec->read_traces[i].start_time;
            end_time = file_rec->read_traces[i].end_time;

            printf("DXT, rank %d reads segment %lld [offset: %lld length: %lld start_time: %fs end_time: %fs]\n", rank, i, offset, length, start_time, end_time);
        }
#endif
    }

    /* shutdown internal structures used for instrumenting */
    dxt_posix_cleanup_runtime();

    /* disable further instrumentation */
    instrumentation_disabled = 1;

    DXT_POSIX_UNLOCK();

    return;
}

static void dxt_mpiio_shutdown(
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count,
    void **dxt_mpiio_buf,
    int *dxt_mpiio_buf_sz)
{
    struct dxt_file_record_ref *rec_ref;
    struct dxt_file_record *file_rec;
    int i, idx;

    int64_t offset;
    int64_t length;
    int64_t rank;
    double start_time;
    double end_time;

    int64_t record_size = 0;
    int64_t record_write_count = 0;
    int64_t record_read_count = 0;
    void *tmp_buf_ptr = *dxt_mpiio_buf;

    assert(dxt_mpiio_runtime);

    DXT_MPIIO_LOCK();
    int dxt_rec_count = dxt_mpiio_runtime->file_rec_count;
    struct dxt_record_ref_tracker *ref_tracker, *tmp;
    struct dxt_record_ref_tracker *ref_tracker_head =
        (struct dxt_record_ref_tracker *)(dxt_mpiio_runtime->rec_id_hash);

    *dxt_mpiio_buf_sz = 0;

    HASH_ITER(hlink, ref_tracker_head, ref_tracker, tmp) {
        rec_ref = (struct dxt_file_record_ref *)ref_tracker->rec_ref_p;
        assert(rec_ref);

        file_rec = rec_ref->file_rec;

        record_write_count = file_rec->write_count;
        record_read_count = file_rec->read_count;
        if (record_write_count == 0 && record_read_count == 0)
            continue;

        /*
         * Buffer format:
         * dxt_file_record + ost_ids + write_traces + read_traces
         */
        record_size = sizeof(struct dxt_file_record) +
                (record_write_count + record_read_count) * sizeof(segment_info);

        if (*dxt_mpiio_buf_sz == 0) {
            *dxt_mpiio_buf = (void *)malloc(record_size);
        } else {
            *dxt_mpiio_buf = (void *)realloc((*dxt_mpiio_buf),
                            *dxt_mpiio_buf_sz + record_size);
        }
        tmp_buf_ptr = (void *)(*dxt_mpiio_buf) + *dxt_mpiio_buf_sz;

        /*Copy struct dxt_file_record */
        memcpy(tmp_buf_ptr, (void *)file_rec, sizeof(struct dxt_file_record));
        tmp_buf_ptr = ((void *)tmp_buf_ptr) + sizeof(struct dxt_file_record);

        /*Copy write record */
        memcpy(tmp_buf_ptr, (void *)(file_rec->write_traces),
                record_write_count * sizeof(segment_info));
        tmp_buf_ptr = ((void *)tmp_buf_ptr) +
                    record_write_count * sizeof(segment_info);

        /*Copy read record */
        memcpy(tmp_buf_ptr, (void *)(file_rec->read_traces),
                record_read_count * sizeof(segment_info));
        tmp_buf_ptr = ((char *)tmp_buf_ptr) +
                    record_read_count * sizeof(segment_info);

        *dxt_mpiio_buf_sz += record_size;

#if 0
        printf("Cong, record_id: %" PRIu64 "\n", rec_ref->file_rec->base_rec.id);
        printf("DXT, file_rec->write_count is: %d\n",
                    file_rec->write_count);

        for (i = 0; i < file_rec->write_count; i++) {
            rank = file_rec->base_rec.rank;
            offset = file_rec->write_traces[i].offset;
            length = file_rec->write_traces[i].length;
            start_time = file_rec->write_traces[i].start_time;
            end_time = file_rec->write_traces[i].end_time;

            printf("DXT, rank %d writes segment %lld [offset: %lld length: %lld start_time: %fs end_time: %fs]\n", rank, i, offset, length, start_time, end_time);
        }

        for (i = 0; i < file_rec->read_count; i++) {
            rank = file_rec->base_rec.rank;
            offset = file_rec->read_traces[i].offset;
            length = file_rec->read_traces[i].length;
            start_time = file_rec->read_traces[i].start_time;
            end_time = file_rec->read_traces[i].end_time;

            printf("DXT, rank %d reads segment %lld [offset: %lld length: %lld start_time: %fs end_time: %fs]\n", rank, i, offset, length, start_time, end_time);
        }
#endif
    }

    /* shutdown internal structures used for instrumenting */
    dxt_mpiio_cleanup_runtime();

    /* disable further instrumentation */
    instrumentation_disabled = 1;

    DXT_MPIIO_UNLOCK();

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
