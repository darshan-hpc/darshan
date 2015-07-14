/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_MPIIO_LOG_UTILS_H
#define __DARSHAN_MPIIO_LOG_UTILS_H

#include "darshan-logutils.h"
#include "darshan-mpiio-log-format.h"

extern char *mpiio_counter_names[];
extern char *mpiio_f_counter_names[];

int darshan_log_get_mpiio_file(darshan_fd fd, struct darshan_mpiio_file *file);

#define MPIIO_COUNTER_PRINT(__file_rec, __counter) do { \
    printf("%" PRId64 "\t%" PRIu64 "\t%s\t%" PRId64 "\n", \
        (__file_rec)->rank, (__file_rec)->f_id, mpiio_counter_names[__counter], \
        (__file_rec)->counters[__counter]); \
} while(0)

#define MPIIO_F_COUNTER_PRINT(__file_rec, __counter) do { \
    printf("%" PRId64 "\t%" PRIu64 "\t%s\t%f\n", \
        (__file_rec)->rank, (__file_rec)->f_id, mpiio_f_counter_names[__counter], \
        (__file_rec)->fcounters[__counter]); \
} while(0)

#endif
