/*
 * Copyright (C) 2019 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_DXT_H
#define __DARSHAN_DXT_H

/* dxt_load_trigger_conf()
 *
 * DXT function exposed to Darshan core to read in any trace triggers
 * from the file path in 'trigger_conf_path' before module
 * initialization occurs.
 */
void dxt_load_trigger_conf(
    char *trigger_conf_path);

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

void dxt_posix_filter_dynamic_traces(
    struct darshan_posix_file *(*rec_id_to_psx_file)(darshan_record_id));

#endif /* __DARSHAN_DXT_H */
