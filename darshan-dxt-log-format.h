/*
 * Copyright (C) 2016 Intel Corporation.
 * See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_DXT_LOG_FORMAT_H
#define __DARSHAN_DXT_LOG_FORMAT_H

/* current DXT log format version */
#define DXT_POSIX_VER 1
#define DXT_MPIIO_VER 2

#define HOSTNAME_SIZE 64

/*
 * DXT, the segment_info structure maintains detailed Segment IO tracing
 * information
 */
typedef struct segment_info {
    int64_t offset;
    int64_t length;
    double start_time;
    double end_time;
} segment_info;

#define X(a) a,
#undef X

/* file record structure for DXT files. a record is created and stored for
 * every DXT file opened by the original application. For the DXT module,
 * the record includes:
 *      - a darshan_base_record structure, which contains the record id & rank
 *      - integer file I/O statistics (open, read/write counts, etc)
 *      - floating point I/O statistics (timestamps, cumulative timers, etc.)
 */
struct dxt_file_record {
    struct darshan_base_record base_rec;
    int64_t shared_record; /* -1 means it is a shared file record */
    char hostname[HOSTNAME_SIZE];

    int64_t write_count;
    int64_t read_count;
};

#endif /* __DARSHAN_DXT_LOG_FORMAT_H */
