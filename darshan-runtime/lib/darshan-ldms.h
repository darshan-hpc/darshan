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
#include "ovis_json/ovis_json.h"

typedef struct darshanConnector {
        int to;
        int ldms_lib;
        int posix_enable_ldms;
        int mpiio_enable_ldms;
        int stdio_enable_ldms;
        int hdf5_enable_ldms;
        int64_t rank;
        uint64_t record_id;
        char *exename;
        const char* env_ldms_stream;
        const char* env_ldms_reinit;
        int server_rc;
        int64_t jobid;
        int64_t uid;
        char hname[HOST_NAME_MAX];
        int64_t hdf5_data[5];
        int64_t open_count;
        int64_t write_count;
        const char *filename;
        const char *data_set;
        int conn_status;
        struct timespec ts;
        pthread_mutex_t ln_lock;
        ldms_t ldms_darsh;
        sem_t conn_sem;
        sem_t recv_sem;
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

/* darshan_ldms_connector_initialize(), darshan_ldms_connector_send()
 *
 * LDMS related function to intialize LDMSD streams plugin for realtime data
 * output of the Darshan modules.
 *
 * LDMS related function to retrieve and send the realitme data output of the Darshan
 * specified module from the set environment variables (i.e. *MODULENAME*_ENABLE_LDMS)
 * to LDMSD streams plugin.
 *
 * LDMS related function to retrieve and set the meta data of each Darshan
 * run (i.e. record id, rank, etc.). These values will not be updated unless a different module
 * is detected or a new run is executed.
 *
 */
void darshan_ldms_connector_initialize(struct darshan_core_runtime *);

void darshan_ldms_connector_send(int64_t record_count, char *rwo, int64_t offset, int64_t length, int64_t max_byte, int64_t rw_switch, int64_t flushes, double start_time, double end_time, struct timespec tspec_start, struct timespec tspec_end, double total_time, char *mod_name, char *data_type);

void darshan_ldms_set_meta(const char *filename, const char *data_set,  uint64_t record_id, int64_t rank);

#endif /* __DARSHAN_LDMS_H */
