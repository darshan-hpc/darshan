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

#endif /* __DARSHAN_DXT_H */
