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

extern struct darshan_mod_logutil_funcs posix_logutils;

int darshan_log_get_posix_file(darshan_fd fd, void **file_rec,
    darshan_record_id *rec_id);
void darshan_log_print_posix_file(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type);

#endif
