/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_POSIX_LOG_UTILS_H
#define __DARSHAN_POSIX_LOG_UTILS_H

#include "darshan-logutils.h"
#include "darshan-posix-log-format.h"

extern char *posix_counter_names[];
extern char *posix_f_counter_names[];

int darshan_log_get_posix_file(darshan_fd fd, struct darshan_posix_file *file);

#define POSIX_COUNTER_PRINT(__file_rec, __counter) do { \
    printf("%" PRId64 "\t%" PRIu64 "\t%s\t%" PRId64 "\n", \
        (__file_rec)->rank, (__file_rec)->f_id, posix_counter_names[__counter], \
        (__file_rec)->counters[__counter]); \
} while(0)

#define POSIX_F_COUNTER_PRINT(__file_rec, __counter) do { \
    printf("%" PRId64 "\t%" PRIu64 "\t%s\t%f\n", \
        (__file_rec)->rank, (__file_rec)->f_id, posix_f_counter_names[__counter], \
        (__file_rec)->fcounters[__counter]); \
} while(0)

#endif
