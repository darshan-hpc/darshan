/*
 * Copyright (C) 2019 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
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

struct darshanConnector dC = {
     .ldms_darsh = NULL,
     .ldms_lib = 1,
     .jobid = 0,
     };

static void event_cb(ldms_t x, ldms_xprt_event_t e, void *cb_arg)
{
	switch (e->type) {
	case LDMS_XPRT_EVENT_CONNECTED:
		sem_post(&dC.conn_sem);
		dC.conn_status = 0;
		break;
	case LDMS_XPRT_EVENT_REJECTED:
		ldms_xprt_put(x, "rail_ref");
		dC.conn_status = ECONNREFUSED;
		break;
	case LDMS_XPRT_EVENT_DISCONNECTED:
		ldms_xprt_put(x, "rail_ref");
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
		ldms_xprt_put(x, "rail_ref");
                dC.conn_status = ECONNABORTED;
		darshan_core_fprintf(stderr, "LDMS library: Received invalid event type %d.\n", e->type);
	}
}

ldms_t setup_connection(const char *xprt, const char *host,
			const char *port, const char *auth)
{
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

#ifdef LDMS_XPRT_NEW_WITH_AUTH_3
	dC.ldms_g = ldms_xprt_new_with_auth(xprt, auth, NULL);
#elif defined(LDMS_XPRT_NEW_WITH_AUTH_4)
	dC.ldms_g = ldms_xprt_new_with_auth(xprt, NULL, auth, NULL);
#endif
	if (!dC.ldms_g) {
		darshan_core_fprintf(stderr, "LDMS library: Error %d creating the '%s' transport.\n", errno, xprt);
		return NULL;
	}

	sem_init(&dC.recv_sem, 1, 0);
	sem_init(&dC.conn_sem, 1, 0);

	rc = ldms_xprt_connect_by_name(dC.ldms_g, host, port, event_cb, NULL);
	if (rc) {
		darshan_core_fprintf(stderr, "LDMS Library: Error %d connecting to %s:%s \n",rc,host,port);
		return NULL;
	}
	sem_timedwait(&dC.conn_sem, &ts);
	if (dC.conn_status){
		darshan_core_fprintf(stderr, "LDMS library: Error %i setting up connection to LDMS streams daemon.\n", dC.conn_status);
		return NULL;
	}
	return dC.ldms_g;
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

    /* grab exe path from darshan_core_runtime. Save with a tmp that will be used later*/
    dC.exepath = strtok(init_core->log_exemnt_p, " ");
    dC.exe_tmp = dC.exepath;

    /* Set flags for various LDMS environment variables */
    if (getenv("DARSHAN_LDMS_ENABLE_ALL")){
	dC.posix_enable_ldms = 1;
	dC.mpiio_enable_ldms = 1;
	dC.stdio_enable_ldms = 1;
	dC.hdf5_enable_ldms = 1;
	}

    else {
	if (getenv("DARSHAN_LDMS_ENABLE_POSIX"))
		dC.posix_enable_ldms = 1;
	else
		dC.posix_enable_ldms = 0;

	if (getenv("DARSHAN_LDMS_ENABLE_MPIIO"))
		dC.mpiio_enable_ldms = 1;
	else
		dC.mpiio_enable_ldms = 0;

	if (getenv("DARSHAN_LDMS_ENABLE_STDIO"))
		dC.stdio_enable_ldms = 1;
	else
		dC.stdio_enable_ldms = 0;

	if (getenv("DARSHAN_LDMS_ENABLE_HDF5"))
		dC.hdf5_enable_ldms = 1;
	else
		dC.hdf5_enable_ldms = 0;
	}

    const char* env_ldms_xprt	 = getenv("DARSHAN_LDMS_XPRT");
    const char* env_ldms_host	 = getenv("DARSHAN_LDMS_HOST");
    const char* env_ldms_port	 = getenv("DARSHAN_LDMS_PORT");
    const char* env_ldms_auth	 = getenv("DARSHAN_LDMS_AUTH");
    dC.env_ldms_stream           = getenv("DARSHAN_LDMS_STREAM");

	    /* Check/set LDMS deamon connection */
    if (!env_ldms_xprt || *env_ldms_xprt == '\0'){
	darshan_core_fprintf(stderr, "LDMS library: darshanConnector - transport for LDMS streams deamon connection is not set. Setting to default value \"sock\".\n");
	env_ldms_xprt = "sock";}

    if (!env_ldms_host || *env_ldms_host == '\0'){
	darshan_core_fprintf(stderr, "LDMS library: darshanConnector - hostname for LDMS streams deamon connection is not set. Setting to default value \"localhost\".\n");
	env_ldms_host = "localhost";}

    if (!env_ldms_port || *env_ldms_port == '\0'){
	darshan_core_fprintf(stderr, "LDMS library: darshanConnector - port for LDMS streams deamon connection is not set. Setting to default value \"412\".\n");
	env_ldms_port = "412";}

    if (!env_ldms_auth || *env_ldms_auth == '\0'){
	darshan_core_fprintf(stderr, "LDMS library: darshanConnector - authentication for LDMS streams deamon connection is not set. Setting to default value \"munge\".\n");
	env_ldms_auth = "munge";}

    if (!dC.env_ldms_stream || *dC.env_ldms_stream == '\0'){
	darshan_core_fprintf(stderr, "LDMS library: darshanConnector - stream name for LDMS streams deamon connection is not set. Setting to default value \"darshanConnector\".\n");
	dC.env_ldms_stream = "darshanConnector";}


    pthread_mutex_lock(&dC.ln_lock);

    dC.ldms_darsh = setup_connection(env_ldms_xprt, env_ldms_host, env_ldms_port, env_ldms_auth);

    if (dC.ldms_darsh == NULL){
        pthread_mutex_unlock(&dC.ln_lock);
        return;
    }
    else if (dC.ldms_darsh->disconnected){
        darshan_core_fprintf(stderr, "LDMS library: darshanConnector - disconnected from LDMS streams daemon -- exiting.\n");
        pthread_mutex_unlock(&dC.ln_lock);
        return;
    }
    pthread_mutex_unlock(&dC.ln_lock);
    return;
}

void darshan_ldms_connector_send(uint64_t record_id, int64_t rank, int64_t record_count, char *rwo, int64_t offset, int64_t length, int64_t max_byte, int64_t rw_switch, int64_t flushes,  double start_time, double end_time, double total_time, char *mod_name, char *data_type)
{
    char jb11[1024];
    int rc, ret, i, size, exists, found;
    struct timespec tspec_start, tspec_end;
    uint64_t micro_s;

    pthread_mutex_lock(&dC.ln_lock);
    if (dC.ldms_darsh != NULL)
	exists = 1;
    else
	exists = 0;
    pthread_mutex_unlock(&dC.ln_lock);

    if (!exists){
	return;
    }

    /* Current schema name used to query darshan data stored in DSOS.
    * If not storing to DSOS, then this can be ignored.*/
    dC.schema = "darshan_data";

    /* get the full file path from record ID */
    dC.filepath = darshan_core_lookup_record_name(record_id);

    /* set all hdf5 related fields to -1 for all other modules*/
    if (strcmp(mod_name, "H5D") != 0){
	size = sizeof(dC.hdf5_data)/sizeof(dC.hdf5_data[0]);
	for (i=0; i < size; i++)
	    dC.hdf5_data[i] = -1;
    }

    /* set following fields for module data to N/A to reduce message size */
    if (strcmp(data_type, "MOD") == 0)
    {
	dC.filepath = "N/A";
	dC.exepath = "N/A";
	dC.schema = "N/A";
    }
    else
	dC.exepath = dC.exe_tmp;

    /* convert the start and end times to timespecs and report absolute timestamps */
    tspec_start = darshan_core_abs_timespec_from_wtime(start_time);
    tspec_end = darshan_core_abs_timespec_from_wtime(end_time);
    micro_s = tspec_end.tv_nsec/1.0e3;

    sprintf(jb11,"{\"schema\":\"%s\", \"uid\":%ld, \"exe\":\"%s\",\"job_id\":%ld,\"rank\":%ld,\"ProducerName\":\"%s\",\"file\":\"%s\",\"record_id\":%"PRIu64",\"module\":\"%s\",\"type\":\"%s\",\"max_byte\":%ld,\"switches\":%ld,\"flushes\":%ld,\"cnt\":%ld,\"op\":\"%s\",\"seg\":[{\"pt_sel\":%ld,\"irreg_hslab\":%ld,\"reg_hslab\":%ld,\"ndims\":%ld,\"npoints\":%ld,\"off\":%ld,\"len\":%ld,\"start\":%0.6f,\"dur\":%0.6f,\"total\":%0.6f,\"timestamp\":%lu.%.6lu}]}", dC.schema, dC.uid, dC.exepath, dC.jobid, rank, dC.hname, dC.filepath, record_id, mod_name, data_type, max_byte, rw_switch, flushes, record_count, rwo, dC.hdf5_data[0], dC.hdf5_data[1], dC.hdf5_data[2], dC.hdf5_data[3], dC.hdf5_data[4], offset, length, start_time, end_time-start_time, total_time, tspec_end.tv_sec, micro_s);

    if (getenv("DARSHAN_LDMS_VERBOSE"))
       darshan_core_fprintf(stderr, "JSON Message: %s\n", jb11);

    rc = ldmsd_stream_publish(dC.ldms_darsh, dC.env_ldms_stream, LDMSD_STREAM_JSON, jb11, strlen(jb11) + 1);
    if (rc)
       darshan_core_fprintf(stderr, "LDMS library: darshanConnector - error %d publishing stream data.\n", rc);

    out_1:
	 return;
}

#else

struct darshanConnector dC = {
 .ldms_lib = 0
 };

void darshan_ldms_connector_initialize(struct darshan_core_runtime *init_core)
{
    return;
}

void darshan_ldms_connector_send(uint64_t record_id, int64_t rank, int64_t record_count, char *rwo, int64_t offset, int64_t length, int64_t max_byte, int64_t rw_switch, int64_t flushes,  double start_time, double end_time, double total_time, char *mod_name, char *data_type)
{
    return;
}
#endif

