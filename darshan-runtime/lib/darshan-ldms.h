/*
 * Copyright (C) 2019 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_LDMS_H
#define __DARSHAN_LDMS_H
#include "darshan.h"

#ifdef HAVE_LDMS
#include <ldms/ldms.h>
#include <ldms/ldmsd_stream.h>
#include <ovis_util/util.h>
#include <ldms/ldms_xprt.h>
#include <semaphore.h>

typedef struct darshanConnector {
        int to;
        int ldms_lib;
        int posix_enable_ldms;
        int mpiio_enable_ldms;
        int stdio_enable_ldms;
        int hdf5_enable_ldms;
        const char *exepath;
        const char *exe_tmp;
        const char *schema;
        const char *filepath;
        const char* env_ldms_stream;
        const char* env_ldms_reinit;
        int server_rc;
        int64_t jobid;
        int64_t uid;
        char hname[HOST_NAME_MAX];
        int64_t hdf5_data[5];
        int64_t open_count;
        int64_t write_count;
        int conn_status;
        struct timespec ts;
        pthread_mutex_t ln_lock;
        ldms_t ldms_darsh;
        ldms_t ldms_g;
        sem_t recv_sem;
        sem_t conn_sem;
} darshanConnector;

#else

typedef struct darshanConnector {
        int to;
        int ldms_lib;
        int posix_enable_ldms;
        int mpiio_enable_ldms;
        int stdio_enable_ldms;
        int hdf5_enable_ldms;
} darshanConnector;

#endif

extern struct darshanConnector dC;

/* darshan_ldms_connector_initialize(), darshan_ldms_connector_send()
 *
 * LDMS related function to intialize LDMSD streams plugin for realtime data
 * output of the Darshan modules.
 *
 * LDMS related function to retrieve and send the realitme data output of the Darshan
 * specified module from the set environment variables (i.e. *MODULENAME*_ENABLE_LDMS)
 * to LDMSD streams plugin.
 *
 */
void darshan_ldms_connector_initialize(struct darshan_core_runtime *);

void darshan_ldms_connector_send(uint64_t record_id, int64_t rank, int64_t record_count, char *rwo, int64_t offset, int64_t length, int64_t max_byte, int64_t rw_switch, int64_t flushes, double start_time, double end_time, double total_time, char *mod_name, char *data_type);

#endif /* __DARSHAN_LDMS_H */
