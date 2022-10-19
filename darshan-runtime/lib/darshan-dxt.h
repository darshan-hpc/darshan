/*
 * Copyright (C) 2019 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_DXT_H
#define __DARSHAN_DXT_H
/* DXT triggers are used to filter out trace records according to
 * user-specified parameters:
 *  - DXT_SMALL_IO_TRIGGER: only retain DXT file records that exhibit a
 *                          higher ratio of small accesses (<10 KiB) than
 *                          a specified user threshold
 *  - DXT_UNALIGNED_IO_TRIGGER: only retain DXT file records that exhibit a
 *                              higher ratio of unaligned accesses than
 *                              a specified user threshold
 */
enum dxt_trigger_type
{
    DXT_SMALL_IO_TRIGGER,
    DXT_UNALIGNED_IO_TRIGGER
};
struct dxt_trigger
{
    int type;
    union {
        struct
        {
            double thresh_pct;
        } small_io;
        struct
        {
            double thresh_pct;
        } unaligned_io;
    } u;
};

/* dxt_posix_runtime_initialize()
 *
 * DXT function exposed to POSIX module for initializing DXT-POSIX runtime.
 */
void dxt_posix_runtime_initialize(void);

/* dxt_mpiio_runtime_initialize()
 *
 * DXT function exposed to MPIIO module for initializing DXT-MPIIO runtime.
 */
void dxt_mpiio_runtime_initialize(void);

/* dxt_posix_write(), dxt_posix_read()
 *
 * DXT function to trace a POSIX write/read call to file record 'rec_id',
 * at offset 'offset' and with 'length' size. 'start_time' and 'end_time'
 * are starting and ending timestamps for the operation, respectively.
 */
void dxt_posix_write(darshan_record_id rec_id, int64_t offset,
        int64_t length, double start_time, double end_time);
void dxt_posix_read(darshan_record_id rec_id, int64_t offset,
        int64_t length, double start_time, double end_time);

/* dxt_mpiio_write(), dxt_mpiio_read()
 *
 * DXT function to trace a MPIIO write/read call to file record 'rec_id',
 * with 'length' size. 'start_time' and 'end_time' are starting and ending
 * timestamps for the operation, respectively.
 */
void dxt_mpiio_write(darshan_record_id rec_id, int64_t offset,
        int64_t length, double start_time, double end_time);
void dxt_mpiio_read(darshan_record_id rec_id, int64_t offset,
        int64_t length, double start_time, double end_time);

void dxt_posix_apply_trace_filter(struct dxt_trigger *trigger);

#ifdef HAVE_LDMS
#include <ldms/ldms.h>
#include <ldms/ldmsd_stream.h>
#include <ovis_util/util.h>
#include "ovis_json/ovis_json.h"

typedef struct darshanConnector {
        int to;
        int ldms_lib;
        int dxt_enable_ldms;
        int posix_enable_ldms;
        int mpiio_enable_ldms;
        int stdio_enable_ldms;
        int hdf5_enable_ldms;
        int mdhim_enable_ldms;
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
        int dxt_enable_ldms;
        int posix_enable_ldms;
        int mpiio_enable_ldms;
        int stdio_enable_ldms;
        int hdf5_enable_ldms;
        int mdhim_enable_ldms;
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
void darshan_ldms_connector_initialize();

void darshan_ldms_connector_send(int64_t record_count, char *rwo, int64_t offset, int64_t length, int64_t max_byte, int64_t rw_switch, int64_t flushes, double start_time, double end_time, struct timespec tspec_start, struct timespec tspec_end, double total_time, char *mod_name, char *data_type);

void darshan_ldms_set_meta(const char *filename, const char *data_set,  uint64_t record_id, int64_t rank);

#endif /* __DARSHAN_DXT_H */
