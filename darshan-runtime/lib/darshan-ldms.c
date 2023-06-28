/*
 *   Copyright (C) 2016 Intel Corporation.
 *   See COPYRIGHT notice in top-level directory.
 *    
 */

#ifdef HAVE_CONFIG_H
# include <darshan-runtime-config.h>
#endif

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "darshan-ldms.h"
#include "darshan.h"

/* Check for LDMS libraries if Darshan is built --with-ldms */
#ifdef HAVE_LDMS
#include <ldms/ldms.h>
#include <ldms/ldmsd_stream.h>
#include <ovis_util/util.h>
#include "ovis_json/ovis_json.h"

struct darshanConnector dC = {
     .ldms_darsh = NULL,
     .exename = NULL,
     .ldms_lib = 0,
     .jobid = 0,
     };

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

ldms_t setup_connection(const char *xprt, const char *host,
                        const char *port, const char *auth)
{
        char hostname[PATH_MAX];
        const char *timeout = "5";
        int rc;
        struct timespec ts;

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

        ldms_g = ldms_xprt_new_with_auth(xprt, auth, NULL);
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

void darshan_ldms_connector_initialize(struct darshan_core_runtime *init_core)
{
     /*TODO: Create environment variable to re-connect to ldms every x seconds
        if(getenv("DARSHAN_LDMS_REINIT"))
            dC.env_ldms_reinit = getenv("DARSHAN_LDMS_REINIT");
        else
            dC.env_ldms_reinit = "1";*/
     
    (void)gethostname(dC.hname, sizeof(dC.hname));
    
    dC.uid = init_core->log_job_p->uid;
    
    if (getenv("SLURM_JOB_ID"))
        dC.jobid = atoi(getenv("SLURM_JOB_ID"));
    else if (getenv("LSB_JOBID"))
    	dC.jobid = atoi(getenv("LSB_JOBID"));
    else if (getenv("JOB_ID"))
        dC.jobid = atoi(getenv("JOB_ID"));
    else if (getenv("LOAD_STEP_ID"))
        dC.jobid = atoi(getenv("LOAD_STEP_ID"));
    else
    /* grab jobid from darshan_core_runtime if slurm, lsf, sge or loadleveler do not exist*/
        dC.jobid = init_core->log_job_p->jobid;
    
    /* grab exe path from darshan_core_runtime */
    dC.exename = strtok(init_core->log_exemnt_p, " ");
     
    /* Pull executable name from proc if no arguemments are given. */
        if (dC.exename == NULL)
        {
         char buff[DARSHAN_EXE_LEN];
         int len = readlink("/proc/self/exe", buff, sizeof(buff)-1);
         buff[len] = '\0';
         dC.exename = buff;
        }

    /* Set flags for various LDMS environment variables */
    if (getenv("POSIX_ENABLE_LDMS"))
        dC.posix_enable_ldms = 0;
    else
        dC.posix_enable_ldms = 1;

    if (getenv("MPIIO_ENABLE_LDMS"))
        dC.mpiio_enable_ldms = 0;
    else
        dC.mpiio_enable_ldms = 1;

    /* Disable STDIO if verbose is enabled to avoid a recursive
    function for darshan_ldms_connector_send() */
    if (getenv("STDIO_ENABLE_LDMS"))
        if (!getenv("DARSHAN_LDMS_VERBOSE"))
            dC.stdio_enable_ldms = 0;
        else
            dC.stdio_enable_ldms = 1;
    else
        dC.stdio_enable_ldms = 1;
    
    if (getenv("HDF5_ENABLE_LDMS"))
        dC.hdf5_enable_ldms = 0;
    else
        dC.hdf5_enable_ldms = 1;

    if (!getenv("DARSHAN_LDMS_STREAM"))
    dC.env_ldms_stream = "darshanConnector";
    
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
            printf("Error setting up connection to LDMS streams daemon: %i -- exiting\n", dC.conn_status);
            pthread_mutex_unlock(&dC.ln_lock);
            return;
        }
        else if (dC.ldms_darsh->disconnected){
            printf("Disconnected from LDMS streams daemon -- exiting\n");
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

void darshan_ldms_connector_send(int64_t record_count, char *rwo, int64_t offset, int64_t length, int64_t max_byte, int64_t rw_switch, int64_t flushes,  double start_time, double end_time, double total_time, char *mod_name, char *data_type)
{
    char jb11[1024];
    int rc, ret, i, size, exists;
    uint64_t micro_s;
    struct timespec tspec_start, tspec_end;
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

    /* convert the start and end times to timespecs and report absolute timestamps */
    tspec_start = darshan_core_abs_timespec_from_wtime(start_time);
    tspec_end = darshan_core_abs_timespec_from_wtime(end_time);

    micro_s = tspec_end.tv_nsec/1.0e3;

    sprintf(jb11,"{ \"uid\":%ld, \"exe\":\"%s\",\"job_id\":%ld,\"rank\":%ld,\"ProducerName\":\"%s\",\"file\":\"%s\",\"record_id\":%"PRIu64",\"module\":\"%s\",\"type\":\"%s\",\"max_byte\":%ld,\"switches\":%ld,\"flushes\":%ld,\"cnt\":%ld,\"op\":\"%s\",\"seg\":[{\"data_set\":\"%s\",\"pt_sel\":%ld,\"irreg_hslab\":%ld,\"reg_hslab\":%ld,\"ndims\":%ld,\"npoints\":%ld,\"off\":%ld,\"len\":%ld,\"start\":%0.6f,\"dur\":%0.6f,\"total\":%.6f,\"timestamp\":%lu.%.6lu}]}", dC.uid, dC.exename, dC.jobid, dC.rank, dC.hname, dC.filename, dC.record_id, mod_name, data_type, max_byte, rw_switch, flushes, record_count, rwo, dC.data_set, dC.hdf5_data[0], dC.hdf5_data[1], dC.hdf5_data[2], dC.hdf5_data[3], dC.hdf5_data[4], offset, length, start_time, end_time-start_time, total_time, tspec_end.tv_sec, micro_s);
    
    if (getenv("DARSHAN_LDMS_VERBOSE"))
            printf("JSON Message: %s\n", jb11);
    
    rc = ldmsd_stream_publish(dC.ldms_darsh, dC.env_ldms_stream, LDMSD_STREAM_JSON, jb11, strlen(jb11) + 1);
    if (rc)
        printf("Error %d publishing data.\n", rc);
    
    out_1:
         return;
}
#else

struct darshanConnector dC = {
 .ldms_lib = 1
 };

void darshan_ldms_connector_initialize(struct darshan_core_runtime *init_core)
{
    return;
}

void darshan_ldms_set_meta(const char *filename, const char *data_set, uint64_t record_id, int64_t rank)
{
    return;
}

void darshan_ldms_connector_send(int64_t record_count, char *rwo, int64_t offset, int64_t length, int64_t max_byte, int64_t rw_switch, int64_t flushes,  double start_time, double end_time, double total_time, char *mod_name, char *data_type)
{
    return;
}
#endif

