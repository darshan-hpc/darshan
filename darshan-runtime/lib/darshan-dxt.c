/*
 * Copyright (C) 2016 Intel Corporation.
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
#include <regex.h>

#include "utlist.h"
#include "uthash.h"
#include "darshan.h"
#include "darshan-dynamic.h"
#include "darshan-dxt.h"

/* Check for LDMS libraries if Darshan is built --with-ldms */
#ifdef ENABLE_LDMS
#include <ldms/ldms.h>
#include <ldms/ldmsd_stream.h>
#include <ovis_util/util.h>
#include "ovis_json/ovis_json.h"

#define _GNU_SOURCE
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#endif

#ifndef HAVE_OFF64_T
typedef int64_t off64_t;
#endif

/* maximum amount of memory to use for storing DXT records */
#ifdef __DARSHAN_MOD_MEM_MAX
#define DXT_IO_TRACE_MEM_MAX (__DARSHAN_MOD_MEM_MAX * 1024L * 1024L)
#else
/* 2 MiB default */
#define DXT_IO_TRACE_MEM_MAX (2 * 1024 * 1024)
#endif

/* Darshan core expects modules to express memory requirements in terms
 * of a fixed-length record size and a total number of records. DXT records
 * are naturally variable in-length, but for simplicity we define each record
 * as being 1 KiB in size
 */
#define DXT_DEF_RECORD_SIZE 1024

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
struct dxt_runtime
{
    void *rec_id_hash;
    int file_rec_count;
    size_t mem_allocated;
    size_t mem_used;
    char *record_buf;
    int record_buf_size;
    int frozen; /* flag to indicate that the counters should no longer be modified */
};

#ifdef HAVE_LDMS
/* Initialize darshanConnector struct metrics to add to json message if LDMS is enabled. */
struct darshanConnector dC = {
    //.conn_status = ENOTCONN,
    .ldms_darsh = NULL,
    .ldms_lib = 0
};
#else
struct darshanConnector dC = {
    .ldms_lib = 1
};
#endif

/* internal helper routines */
static void check_wr_trace_buf(
    struct dxt_file_record_ref *rec_ref, darshan_module_id mod_id,
    struct dxt_runtime *runtime);
static void check_rd_trace_buf(
    struct dxt_file_record_ref *rec_ref, darshan_module_id mod_id,
    struct dxt_runtime *runtime);
static struct dxt_file_record_ref *dxt_posix_track_new_file_record(
    darshan_record_id rec_id);
static struct dxt_file_record_ref *dxt_mpiio_track_new_file_record(
    darshan_record_id rec_id);

/* DXT output/cleanup routines for darshan-core */
static void dxt_posix_output(
    void **dxt_buf, int *dxt_buf_sz);
static void dxt_mpiio_output(
    void **dxt_buf, int *dxt_buf_sz);
static void dxt_posix_cleanup(
    void);
static void dxt_mpiio_cleanup(
    void);

/* POSIX module helper for filtering DXT trace records */
extern struct darshan_posix_file *darshan_posix_rec_id_to_file(
    darshan_record_id rec_id);

static struct dxt_runtime *dxt_posix_runtime = NULL;
static struct dxt_runtime *dxt_mpiio_runtime = NULL;
static pthread_mutex_t dxt_runtime_mutex =
            PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

static int dxt_my_rank = -1;

#define DXT_LOCK() pthread_mutex_lock(&dxt_runtime_mutex)
#define DXT_UNLOCK() pthread_mutex_unlock(&dxt_runtime_mutex)

/************************************************************
 *  DXT routines exposed to Darshan core and other modules  *
 ************************************************************/

/* initialize internal DXT module data structures and register with darshan-core */
void dxt_posix_runtime_initialize()
{
    /* calculate how many "records" to request from Darshan core using DXT's
     * configured max memory consumption and the default record size
     */
    size_t dxt_psx_rec_count = DXT_IO_TRACE_MEM_MAX / DXT_DEF_RECORD_SIZE;
    darshan_module_funcs mod_funcs = {
#ifdef HAVE_MPI
    .mod_redux_func = NULL,
#endif
    .mod_output_func = &dxt_posix_output,
    .mod_cleanup_func = &dxt_posix_cleanup
    };
    int ret;

    /* register the DXT module with darshan core */
    ret = darshan_core_register_module(
        DXT_POSIX_MOD,
        mod_funcs,
        DXT_DEF_RECORD_SIZE,
        &dxt_psx_rec_count,
        &dxt_my_rank,
        NULL);
    if(ret < 0)
        return;

    DXT_LOCK();
    dxt_posix_runtime = malloc(sizeof(*dxt_posix_runtime));
    if(!dxt_posix_runtime)
    {
        darshan_core_unregister_module(DXT_POSIX_MOD);
        DXT_UNLOCK();
        return;
    }
    memset(dxt_posix_runtime, 0, sizeof(*dxt_posix_runtime));
    dxt_posix_runtime->mem_used = 0;
    dxt_posix_runtime->mem_allocated = dxt_psx_rec_count * DXT_DEF_RECORD_SIZE;
    DXT_UNLOCK();

    return;
}

void dxt_mpiio_runtime_initialize()
{
    /* calculate how many "records" to request from Darshan core using DXT's
     * configured max memory consumption and the default record size
     */
    size_t dxt_mpiio_rec_count = DXT_IO_TRACE_MEM_MAX / DXT_DEF_RECORD_SIZE;
    darshan_module_funcs mod_funcs = {
#ifdef HAVE_MPI
    .mod_redux_func = NULL,
#endif
    .mod_output_func = &dxt_mpiio_output,
    .mod_cleanup_func = &dxt_mpiio_cleanup
    };
    int ret;

    /* register the DXT module with darshan core */
    ret = darshan_core_register_module(
        DXT_MPIIO_MOD,
        mod_funcs,
        DXT_DEF_RECORD_SIZE,
        &dxt_mpiio_rec_count,
        &dxt_my_rank,
        NULL);
    if(ret < 0)
        return;

    DXT_LOCK();
    dxt_mpiio_runtime = malloc(sizeof(*dxt_mpiio_runtime));
    if(!dxt_mpiio_runtime)
    {
        darshan_core_unregister_module(DXT_MPIIO_MOD);
        DXT_UNLOCK();
        return;
    }
    memset(dxt_mpiio_runtime, 0, sizeof(*dxt_mpiio_runtime));
    dxt_mpiio_runtime->mem_used = 0;
    dxt_mpiio_runtime->mem_allocated = dxt_mpiio_rec_count * DXT_DEF_RECORD_SIZE;
    DXT_UNLOCK();

    return;
}

#ifdef HAVE_LDMS
ldms_t ldms_g;
static void event_cb(ldms_t x, ldms_xprt_event_t e, void *cb_arg)
{
	switch (e->type) {
	case LDMS_XPRT_EVENT_CONNECTED:
		sem_post(&dC.conn_sem);
		dC.conn_status = 0;
		break;
	case LDMS_XPRT_EVENT_REJECTED:
		ldms_xprt_put(x);
                dC.conn_status = ECONNREFUSED;
		break;
	case LDMS_XPRT_EVENT_DISCONNECTED:
		ldms_xprt_put(x);
		dC.conn_status = ENOTCONN;
		break;
	case LDMS_XPRT_EVENT_ERROR:
		dC.conn_status = ECONNREFUSED;
		break;
	case LDMS_XPRT_EVENT_RECV:
		sem_post(&dC.recv_sem);
		break;
	case LDMS_XPRT_EVENT_SEND_COMPLETE:
                break;
	default:
		printf("Received invalid event type %d\n", e->type);
	}
}

#define SLURM_NOTIFY_TIMEOUT 5
ldms_t setup_connection(const char *xprt, const char *host,
			const char *port, const char *auth)
{
	char hostname[PATH_MAX];
	const char *timeout = "5";
	int rc;
	struct timespec ts;

	if (!host) {
		if (0 == gethostname(hostname, sizeof(hostname)))
			host = hostname;
	}
	if (!timeout) {
		ts.tv_sec = time(NULL) + 5;
		ts.tv_nsec = 0;
	} else {
		int to = atoi(timeout);
		if (to <= 0)
			to = 5;
		ts.tv_sec = time(NULL) + to;
		ts.tv_nsec = 0;
	}

        ldms_g = ldms_xprt_new_with_auth(xprt, NULL, auth, NULL);
        if (!ldms_g) {
		printf("Error %d creating the '%s' transport\n",
		       errno, xprt);
		return NULL;
	}

	sem_init(&dC.recv_sem, 1, 0);
	sem_init(&dC.conn_sem, 1, 0);

        rc = ldms_xprt_connect_by_name(ldms_g, host, port, event_cb, NULL);
        if (rc) {
		printf("Error %d connecting to %s:%s\n",
		       rc, host, port);
		return NULL;
	}
	sem_timedwait(&dC.conn_sem, &ts);
	if (dC.conn_status)
		return NULL;
	return ldms_g;
}

void darshan_ldms_connector_initialize()
{
    if (!getenv("DARSHAN_LDMS_STREAM"))
        dC.env_ldms_stream = "darshanConnector";

    /* Set flags for various LDMS environment variables */
    if (getenv("DXT_ENABLE_LDMS"))
        dC.dxt_enable_ldms = 0;
    else
        dC.dxt_enable_ldms =1;

    if (getenv("POSIX_ENABLE_LDMS"))
        dC.posix_enable_ldms = 0;
    else
        dC.posix_enable_ldms = 1;

    if (getenv("MPIIO_ENABLE_LDMS"))
        dC.mpiio_enable_ldms = 0;
    else
        dC.mpiio_enable_ldms = 1;

    if (getenv("STDIO_ENABLE_LDMS"))
        dC.stdio_enable_ldms = 0;
    else
        dC.stdio_enable_ldms = 1;

    if (getenv("HDF5_ENABLE_LDMS"))
        dC.hdf5_enable_ldms = 0;
    else
        dC.hdf5_enable_ldms = 1;

    if (getenv("MDHIM_ENABLE_LDMS"))
        dC.mdhim_enable_ldms = 0;
    else
        dC.mdhim_enable_ldms = 1;

    const char* env_ldms_xprt    = getenv("DARSHAN_LDMS_XPRT");
    const char* env_ldms_host    = getenv("DARSHAN_LDMS_HOST");
    const char* env_ldms_port    = getenv("DARSHAN_LDMS_PORT");
    const char* env_ldms_auth    = getenv("DARSHAN_LDMS_AUTH");

    /* Check/set LDMS transport type */
    if (!env_ldms_xprt || !env_ldms_host || !env_ldms_port || !env_ldms_auth){
        printf("Either the transport, host, port or authentication is not given\n");
        return;
    }

    pthread_mutex_lock(&dC.ln_lock);
    dC.ldms_darsh = setup_connection(env_ldms_xprt, env_ldms_host, env_ldms_port, env_ldms_auth);
        if (dC.conn_status != 0) {
            printf("Error setting up connection: %i -- exiting\n", dC.conn_status);
            pthread_mutex_unlock(&dC.ln_lock);
            return;
        }
        else if (dC.ldms_darsh->disconnected){
            printf("Error setting up connection -- exiting\n");
            pthread_mutex_unlock(&dC.ln_lock);
            return;
        }
    pthread_mutex_unlock(&dC.ln_lock);
    return;
}

void darshan_ldms_set_meta(const char *filename, const char *data_set, uint64_t record_id, int64_t rank)
{
    dC.rank = rank;
    dC.filename = filename;
    dC.data_set = data_set;
    dC.record_id = record_id;
    return;

}

void darshan_ldms_connector_send(int64_t record_count, char *rwo, int64_t offset, int64_t length, int64_t max_byte, int64_t rw_switch, int64_t flushes,  double start_time, double end_time, struct timespec tspec_start, struct timespec tspec_end, double total_time, char *mod_name, char *data_type)
{
    char jb11[1024];
    int rc, ret, i, size, exists;
    uint64_t micro_s = tspec_end.tv_nsec/1.0e3;
    dC.env_ldms_stream  = getenv("DARSHAN_LDMS_STREAM");


    pthread_mutex_lock(&dC.ln_lock);
    if (dC.ldms_darsh != NULL)
        exists = 1;
    else
        exists = 0;
    pthread_mutex_unlock(&dC.ln_lock);

    if (!exists){
        return;
    }


    if (strcmp(rwo, "open") == 0)
        dC.open_count = record_count;

    /* set record count to number of opens since we are closing the same file we opened.*/
    if (strcmp(rwo, "close") == 0)
        record_count = dC.open_count;

    if (strcmp(mod_name, "H5D") != 0){
        size = sizeof(dC.hdf5_data)/sizeof(dC.hdf5_data[0]);
        dC.data_set = "N/A";
        for (i=0; i < size; i++)
            dC.hdf5_data[i] = -1;
    }

    if (strcmp(data_type, "MOD") == 0)
    {
        dC.filename = "N/A";
        dC.exename = "N/A";
    }

    sprintf(jb11,"{ \"uid\":%d, \"exe\":\"%s\",\"job_id\":%d,\"rank\":%d,\"ProducerName\":\"%s\",\"file\":\"%s\",\"record_id\":%"PRIu64",\"module\":\"%s\",\"type\":\"%s\",\"max_byte\":%lld,\"switches\":%d,\"flushes\":%d,\"cnt\":%d,\"op\":\"%s\",\"seg\":[{\"data_set\":\"%s\",\"pt_sel\":%lld,\"irreg_hslab\":%lld,\"reg_hslab\":%lld,\"ndims\":%lld,\"npoints\":%lld,\"off\":%lld,\"len\":%lld,\"start\":%0.6f,\"dur\":%0.6f,\"total\":%0.6f,\"timestamp\":%lu.%0.6lu}]}", dC.uid, dC.exename, dC.jobid, dC.rank, dC.hname, dC.filename, dC.record_id, mod_name, data_type, max_byte, rw_switch, flushes, record_count, rwo, dC.data_set, dC.hdf5_data[0], dC.hdf5_data[1], dC.hdf5_data[2], dC.hdf5_data[3], dC.hdf5_data[4], offset, length, start_time, end_time-start_time, total_time, tspec_end.tv_sec, micro_s);
    //printf("this is in jb11 %s \n", jb11);

    rc = ldmsd_stream_publish(dC.ldms_darsh, dC.env_ldms_stream, LDMSD_STREAM_JSON, jb11, strlen(jb11) + 1);
    if (rc)
        printf("Error %d publishing data.\n", rc);

 out_1:
    return;
}

#else
void darshan_ldms_connector_initialize()
{
    return;
}

void darshan_ldms_set_meta(const char *filename, const char *data_set, uint64_t record_id, int64_t rank)
{
    return;
}

void darshan_ldms_connector_send(int64_t record_count, char *rwo, int64_t offset, int64_t length, int64_t max_byte, int64_t rw_switch, int64_t flushes,  double start_time, double end_time, struct timespec tspec_start, struct timespec tspec_end, double total_time, char *mod_name, char *data_type)
{
    return;
}
#endif

void dxt_posix_write(darshan_record_id rec_id, int64_t offset,
        int64_t length, double start_time, double end_time)
{
    struct dxt_file_record_ref* rec_ref = NULL;
    struct dxt_file_record *file_rec;

    DXT_LOCK();

    if(!dxt_posix_runtime || dxt_posix_runtime->frozen)
    {
        DXT_UNLOCK();
        return;
    }

    rec_ref = darshan_lookup_record_ref(dxt_posix_runtime->rec_id_hash,
        &rec_id, sizeof(darshan_record_id));
    if(!rec_ref)
    {
        /* track new dxt file record */
        rec_ref = dxt_posix_track_new_file_record(rec_id);
        if(!rec_ref)
        {
            DXT_UNLOCK();
            return;
        }
    }

    file_rec = rec_ref->file_rec;
    check_wr_trace_buf(rec_ref, DXT_POSIX_MOD, dxt_posix_runtime);
    if(file_rec->write_count == rec_ref->write_available_buf)
    {
        /* no more memory for i/o segments ... back out */
        DXT_UNLOCK();
        return;
    }

    rec_ref->write_traces[file_rec->write_count].offset = offset;
    rec_ref->write_traces[file_rec->write_count].length = length;
    rec_ref->write_traces[file_rec->write_count].start_time = start_time;
    rec_ref->write_traces[file_rec->write_count].end_time = end_time;
    file_rec->write_count += 1;

    DXT_UNLOCK();
}

void dxt_posix_read(darshan_record_id rec_id, int64_t offset,
        int64_t length, double start_time, double end_time)
{
    struct dxt_file_record_ref* rec_ref = NULL;
    struct dxt_file_record *file_rec;

    DXT_LOCK();

    if(!dxt_posix_runtime || dxt_posix_runtime->frozen)
    {
        DXT_UNLOCK();
        return;
    }

    rec_ref = darshan_lookup_record_ref(dxt_posix_runtime->rec_id_hash,
                &rec_id, sizeof(darshan_record_id));
    if (!rec_ref)
    {
        /* track new dxt file record */
        rec_ref = dxt_posix_track_new_file_record(rec_id);
        if(!rec_ref)
        {
            DXT_UNLOCK();
            return;
        }
    }

    file_rec = rec_ref->file_rec;
    check_rd_trace_buf(rec_ref, DXT_POSIX_MOD, dxt_posix_runtime);
    if(file_rec->read_count == rec_ref->read_available_buf)
    {
        /* no more memory for i/o segments ... back out */
        DXT_UNLOCK();
        return;
    }

    rec_ref->read_traces[file_rec->read_count].offset = offset;
    rec_ref->read_traces[file_rec->read_count].length = length;
    rec_ref->read_traces[file_rec->read_count].start_time = start_time;
    rec_ref->read_traces[file_rec->read_count].end_time = end_time;
    file_rec->read_count += 1;

    DXT_UNLOCK();
}

void dxt_mpiio_write(darshan_record_id rec_id, int64_t offset,
        int64_t length, double start_time, double end_time)
{
    struct dxt_file_record_ref* rec_ref = NULL;
    struct dxt_file_record *file_rec;

    DXT_LOCK();

    if(!dxt_mpiio_runtime || dxt_mpiio_runtime->frozen)
    {
        DXT_UNLOCK();
        return;
    }

    rec_ref = darshan_lookup_record_ref(dxt_mpiio_runtime->rec_id_hash,
                &rec_id, sizeof(darshan_record_id));
    if(!rec_ref)
    {
        /* track new dxt file record */
        rec_ref = dxt_mpiio_track_new_file_record(rec_id);
        if(!rec_ref)
        {
            DXT_UNLOCK();
            return;
        }
    }

    file_rec = rec_ref->file_rec;
    check_wr_trace_buf(rec_ref, DXT_MPIIO_MOD, dxt_mpiio_runtime);
    if(file_rec->write_count == rec_ref->write_available_buf)
    {
        /* no more memory for i/o segments ... back out */
        DXT_UNLOCK();
        return;
    }

    rec_ref->write_traces[file_rec->write_count].length = length;
    rec_ref->write_traces[file_rec->write_count].offset = offset;
    rec_ref->write_traces[file_rec->write_count].start_time = start_time;
    rec_ref->write_traces[file_rec->write_count].end_time = end_time;
    file_rec->write_count += 1;

    DXT_UNLOCK();
}

void dxt_mpiio_read(darshan_record_id rec_id, int64_t offset,
        int64_t length, double start_time, double end_time)
{
    struct dxt_file_record_ref* rec_ref = NULL;
    struct dxt_file_record *file_rec;

    DXT_LOCK();

    if(!dxt_mpiio_runtime || dxt_mpiio_runtime->frozen)
    {
        DXT_UNLOCK();
        return;
    }

    rec_ref = darshan_lookup_record_ref(dxt_mpiio_runtime->rec_id_hash,
                &rec_id, sizeof(darshan_record_id));
    if(!rec_ref)
    {
        /* track new dxt file record */
        rec_ref = dxt_mpiio_track_new_file_record(rec_id);
        if(!rec_ref)
        {
            DXT_UNLOCK();
            return;
        }
    }

    file_rec = rec_ref->file_rec;
    check_rd_trace_buf(rec_ref, DXT_MPIIO_MOD, dxt_mpiio_runtime);
    if(file_rec->read_count == rec_ref->read_available_buf)
    {
        /* no more memory for i/o segments ... back out */
        DXT_UNLOCK();
        return;
    }

    rec_ref->read_traces[file_rec->read_count].length = length;
    rec_ref->read_traces[file_rec->read_count].offset = offset;
    rec_ref->read_traces[file_rec->read_count].start_time = start_time;
    rec_ref->read_traces[file_rec->read_count].end_time = end_time;
    file_rec->read_count += 1;

    DXT_UNLOCK();
}

static void dxt_posix_filter_traces_iterator(void *rec_ref_p, void *user_ptr)
{
    struct dxt_file_record_ref *psx_rec_ref, *mpiio_rec_ref;
    struct darshan_posix_file *psx_file;
    struct dxt_trigger *trigger = (struct dxt_trigger *)user_ptr;
    int should_keep = 0;

    psx_rec_ref = (struct dxt_file_record_ref *)rec_ref_p;
    psx_file = darshan_posix_rec_id_to_file(psx_rec_ref->file_rec->base_rec.id);

    /* analyze dynamic triggers to determine whether we should keep the record */
    switch(trigger->type)
    {
        case DXT_SMALL_IO_TRIGGER:
        {
            int total_ops = psx_file->counters[POSIX_WRITES] +
                psx_file->counters[POSIX_READS];
            int small_ops = psx_file->counters[POSIX_SIZE_WRITE_0_100] +
                psx_file->counters[POSIX_SIZE_WRITE_100_1K] +
                psx_file->counters[POSIX_SIZE_WRITE_1K_10K] +
                psx_file->counters[POSIX_SIZE_READ_0_100] +
                psx_file->counters[POSIX_SIZE_READ_100_1K] +
                psx_file->counters[POSIX_SIZE_READ_1K_10K];
            double small_pct = (small_ops / (double)(total_ops));
            if(small_pct >= trigger->u.small_io.thresh_pct)
                should_keep = 1;
            break;
        }
        case DXT_UNALIGNED_IO_TRIGGER:
        {
            int total_ops = psx_file->counters[POSIX_WRITES] +
                psx_file->counters[POSIX_READS];
            int unaligned_ops = psx_file->counters[POSIX_FILE_NOT_ALIGNED];
            double unaligned_pct = (unaligned_ops / (double)(total_ops));
            if(unaligned_pct >= trigger->u.unaligned_io.thresh_pct)
                should_keep = 1;
            break;
        }
    }

    /* drop the record if no dynamic trace triggers occurred */
    if(!should_keep)
    {
        if(dxt_mpiio_runtime && dxt_mpiio_runtime->rec_id_hash)
        {
            /* first check the MPI-IO traces to see if we should drop there */
            mpiio_rec_ref = darshan_delete_record_ref(&dxt_mpiio_runtime->rec_id_hash,
                &psx_file->base_rec.id, sizeof(darshan_record_id));
            if(mpiio_rec_ref)
            {
                free(mpiio_rec_ref->write_traces);
                free(mpiio_rec_ref->read_traces);
                free(mpiio_rec_ref->file_rec);
                free(mpiio_rec_ref);
            }
        }

        if(dxt_posix_runtime && dxt_posix_runtime->rec_id_hash)
        {
            /* then delete the POSIX trace records */
            psx_rec_ref = darshan_delete_record_ref(&dxt_posix_runtime->rec_id_hash,
                &psx_file->base_rec.id, sizeof(darshan_record_id));
            if(psx_rec_ref)
            {
                free(psx_rec_ref->write_traces);
                free(psx_rec_ref->read_traces);
                free(psx_rec_ref->file_rec);
                free(psx_rec_ref);
            }
        }
    }

    return;
}

void dxt_posix_apply_trace_filter(
    struct dxt_trigger *trigger)
{
    DXT_LOCK();

    if(!dxt_posix_runtime)
    {
        DXT_UNLOCK();
        return;
    }

    darshan_iter_record_refs(dxt_posix_runtime->rec_id_hash,
        dxt_posix_filter_traces_iterator, trigger);

    DXT_UNLOCK();

    return;
}

/***********************************
 *  internal DXT helper routines   *
 ***********************************/

static void check_wr_trace_buf(struct dxt_file_record_ref *rec_ref,
    darshan_module_id mod_id, struct dxt_runtime *runtime)
{
    struct dxt_file_record *file_rec = rec_ref->file_rec;

    int write_count = file_rec->write_count;
    int write_available_buf = rec_ref->write_available_buf;

    if (write_count >= write_available_buf)
    {
        int write_count_inc;
        if(write_available_buf == 0)
            write_count_inc = IO_TRACE_BUF_SIZE;
        else
            write_count_inc = write_available_buf;

        size_t mem_left = runtime->mem_allocated - runtime->mem_used;
        size_t mem_req = write_count_inc * sizeof(segment_info);
        if(mem_req > mem_left)
        {
            write_count_inc = mem_left / sizeof(segment_info);
            if(write_count_inc == 0)
            {
                /* we need to request at least one record, even if we
                 * know there is not enough memory left, so that Darshan
                 * core can mark this module as having ran out of data
                 */
                write_count_inc = 1;
            }
            mem_req = write_count_inc * sizeof(segment_info);
        }

        /* register the increased write buffer size with Darshan core */
        /* NOTE: register_record() does not handle DXT memory allocations,
         * it just checks that there is enough memory for the record -- if
         * there is not enough memory, this function will return NULL
         */
        if(darshan_core_register_record(
             file_rec->base_rec.id,
             NULL, /* no name registration needed, handled in initial record alloc */
             mod_id,
             mem_req,
             NULL))
        {
            /* there is enough memory for these additional trace segments,
             * but we have to (re)allocate them ourselves
             */
            write_available_buf += write_count_inc;
            rec_ref->write_traces =
                (segment_info *)realloc(rec_ref->write_traces,
                        write_available_buf * sizeof(segment_info));

            rec_ref->write_available_buf = write_available_buf;
        }
        runtime->mem_used += mem_req;
    }
}

static void check_rd_trace_buf(struct dxt_file_record_ref *rec_ref,
    darshan_module_id mod_id, struct dxt_runtime *runtime)
{
    struct dxt_file_record *file_rec = rec_ref->file_rec;

    int read_count = file_rec->read_count;
    int read_available_buf = rec_ref->read_available_buf;

    if (read_count >= read_available_buf)
    {
        int read_count_inc;
        if(read_available_buf == 0)
            read_count_inc = IO_TRACE_BUF_SIZE;
        else
            read_count_inc = read_available_buf;

        size_t mem_left = runtime->mem_allocated - runtime->mem_used;
        size_t mem_req = read_count_inc * sizeof(segment_info);
        if(mem_req > mem_left)
        {
            read_count_inc = mem_left / sizeof(segment_info);
            if(read_count_inc == 0)
            {
                /* we need to request at least one record, even if we
                 * know there is not enough memory left, so that Darshan
                 * core can mark this module as having ran out of data
                 */
                read_count_inc = 1;
            }
            mem_req = read_count_inc * sizeof(segment_info);
        }

        /* register the increased read buffer size with Darshan core */
        /* NOTE: register_record() does not handle DXT memory allocations,
         * it just checks that there is enough memory for the record -- if
         * there is not enough memory, this function will return NULL
         */
        if(darshan_core_register_record(
             file_rec->base_rec.id,
             NULL, /* no name registration needed, handled in initial record alloc */
             mod_id,
             mem_req,
             NULL))
        {
            /* there is enough memory for these additional trace segments,
             * but we have to (re)allocate them ourselves
             */
            read_available_buf += read_count_inc;
            rec_ref->read_traces =
                (segment_info *)realloc(rec_ref->read_traces,
                        read_available_buf * sizeof(segment_info));

            rec_ref->read_available_buf = read_available_buf;
        }
        runtime->mem_used += mem_req;
    }
}

static struct dxt_file_record_ref *dxt_posix_track_new_file_record(
    darshan_record_id rec_id)
{
    struct dxt_file_record_ref *rec_ref = NULL;
    struct dxt_file_record *file_rec = NULL;
    int ret;

    DXT_LOCK();

    rec_ref = malloc(sizeof(*rec_ref));
    if(!rec_ref)
    {
        DXT_UNLOCK();
        return(NULL);
    }
    memset(rec_ref, 0, sizeof(*rec_ref));

    /* add a reference to this file record based on record id */
    ret = darshan_add_record_ref(&(dxt_posix_runtime->rec_id_hash), &rec_id,
            sizeof(darshan_record_id), rec_ref);
    if(ret == 0)
    {
        free(rec_ref);
        DXT_UNLOCK();
        return(NULL);
    }

    /* register base DXT record with with darshan core for now */
    /* NOTE: register_record() does not handle DXT memory allocations,
     * it just checks that there is enough memory for the record -- if
     * there is not enough memory, this function will return NULL
     */
    if(darshan_core_register_record(
         rec_id,
         darshan_core_lookup_record_name(rec_id),
         DXT_POSIX_MOD,
         sizeof(*file_rec),
         NULL) == NULL)
    {
        darshan_delete_record_ref(&(dxt_posix_runtime->rec_id_hash),
            &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        DXT_UNLOCK();
        return(NULL);
    }

    /* allocate DXT record ourselves if Darshan core registration succeeded */
    file_rec = malloc(sizeof(*file_rec));
    if(!file_rec)
    {
        darshan_delete_record_ref(&(dxt_posix_runtime->rec_id_hash),
            &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        DXT_UNLOCK();
        return(NULL);
    }
    memset(file_rec, 0, sizeof(*file_rec));

    dxt_posix_runtime->file_rec_count++;
    dxt_posix_runtime->mem_used += sizeof(*file_rec);
    DXT_UNLOCK();

    /* initialize record and record reference fields */
    file_rec->base_rec.id = rec_id;
    file_rec->base_rec.rank = dxt_my_rank;
    gethostname(file_rec->hostname, HOSTNAME_SIZE);

    rec_ref->file_rec = file_rec;

    return(rec_ref);
}

static struct dxt_file_record_ref *dxt_mpiio_track_new_file_record(
    darshan_record_id rec_id)
{
    struct dxt_file_record *file_rec = NULL;
    struct dxt_file_record_ref *rec_ref = NULL;
    int ret;

    DXT_LOCK();

    rec_ref = malloc(sizeof(*rec_ref));
    if(!rec_ref)
    {
        DXT_UNLOCK();
        return(NULL);
    }
    memset(rec_ref, 0, sizeof(*rec_ref));

    /* add a reference to this file record based on record id */
    ret = darshan_add_record_ref(&(dxt_mpiio_runtime->rec_id_hash), &rec_id,
            sizeof(darshan_record_id), rec_ref);
    if(ret == 0)
    {
        free(rec_ref);
        DXT_UNLOCK();
        return(NULL);
    }

    /* register base DXT record with with darshan core for now */
    /* NOTE: register_record() does not handle DXT memory allocations,
     * it just checks that there is enough memory for the record -- if
     * there is not enough memory, this function will return NULL
     */
    if(darshan_core_register_record(
         rec_id,
         darshan_core_lookup_record_name(rec_id),
         DXT_MPIIO_MOD,
         sizeof(*file_rec),
         NULL) == NULL)
    {
        darshan_delete_record_ref(&(dxt_mpiio_runtime->rec_id_hash),
            &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        DXT_UNLOCK();
        return(NULL);
    }

    /* allocate DXT record ourselves if Darshan core registration succeeded */
    file_rec = malloc(sizeof(*file_rec));
    if(!file_rec)
    {
        darshan_delete_record_ref(&(dxt_mpiio_runtime->rec_id_hash),
            &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        DXT_UNLOCK();
        return(NULL);
    }
    memset(file_rec, 0, sizeof(*file_rec));

    dxt_mpiio_runtime->file_rec_count++;
    dxt_mpiio_runtime->mem_used += sizeof(*file_rec);
    DXT_UNLOCK();

    /* initialize record and record reference fields */
    file_rec->base_rec.id = rec_id;
    file_rec->base_rec.rank = dxt_my_rank;
    gethostname(file_rec->hostname, HOSTNAME_SIZE);

    rec_ref->file_rec = file_rec;

    return(rec_ref);
}

static void dxt_free_record_data(void *rec_ref_p, void *user_ptr)
{
    struct dxt_file_record_ref *dxt_rec_ref = (struct dxt_file_record_ref *)rec_ref_p;

    free(dxt_rec_ref->write_traces);
    free(dxt_rec_ref->read_traces);
    free(dxt_rec_ref->file_rec);
}

/********************************************************************************
 *     functions exported by this module for coordinating with darshan-core     *
 ********************************************************************************/

static void dxt_serialize_posix_records(void *rec_ref_p, void *user_ptr)
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
}

static void dxt_posix_output(
    void **dxt_posix_buf,
    int *dxt_posix_buf_sz)
{
    assert(dxt_posix_runtime);

    *dxt_posix_buf_sz = 0;

    dxt_posix_runtime->record_buf = malloc(dxt_posix_runtime->mem_allocated);
    if(!(dxt_posix_runtime->record_buf))
        return;
    memset(dxt_posix_runtime->record_buf, 0, dxt_posix_runtime->mem_allocated);
    dxt_posix_runtime->record_buf_size = 0;

    /* iterate all dxt posix records and serialize them to the output buffer */
    darshan_iter_record_refs(dxt_posix_runtime->rec_id_hash,
        dxt_serialize_posix_records, NULL);

    /* set output */
    *dxt_posix_buf = dxt_posix_runtime->record_buf;
    *dxt_posix_buf_sz = dxt_posix_runtime->record_buf_size;

    dxt_posix_runtime->frozen = 1;

    return;
}

static void dxt_posix_cleanup()
{
    assert(dxt_posix_runtime);

    free(dxt_posix_runtime->record_buf);

    /* cleanup internal structures used for instrumenting */
    darshan_iter_record_refs(dxt_posix_runtime->rec_id_hash,
        dxt_free_record_data, NULL);
    darshan_clear_record_refs(&(dxt_posix_runtime->rec_id_hash), 1);

    free(dxt_posix_runtime);
    dxt_posix_runtime = NULL;

    return;
}

static void dxt_serialize_mpiio_records(void *rec_ref_p, void *user_ptr)
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
}

static void dxt_mpiio_output(
    void **dxt_mpiio_buf,
    int *dxt_mpiio_buf_sz)
{
    assert(dxt_mpiio_runtime);

    *dxt_mpiio_buf_sz = 0;

    dxt_mpiio_runtime->record_buf = malloc(dxt_mpiio_runtime->mem_allocated);
    if(!(dxt_mpiio_runtime->record_buf))
        return;
    memset(dxt_mpiio_runtime->record_buf, 0, dxt_mpiio_runtime->mem_allocated);
    dxt_mpiio_runtime->record_buf_size = 0;

    /* iterate all dxt posix records and serialize them to the output buffer */
    darshan_iter_record_refs(dxt_mpiio_runtime->rec_id_hash,
        dxt_serialize_mpiio_records, NULL);

    /* set output */
    *dxt_mpiio_buf = dxt_mpiio_runtime->record_buf;
    *dxt_mpiio_buf_sz = dxt_mpiio_runtime->record_buf_size;

    dxt_mpiio_runtime->frozen = 1;

    return;
}

static void dxt_mpiio_cleanup()
{
    assert(dxt_mpiio_runtime);

    free(dxt_mpiio_runtime->record_buf);

    /* cleanup internal structures used for instrumenting */
    darshan_iter_record_refs(dxt_mpiio_runtime->rec_id_hash,
        dxt_free_record_data, NULL);
    darshan_clear_record_refs(&(dxt_mpiio_runtime->rec_id_hash), 1);

    free(dxt_mpiio_runtime);
    dxt_mpiio_runtime = NULL;

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
