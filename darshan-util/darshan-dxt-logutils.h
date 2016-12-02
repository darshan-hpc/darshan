/*
 * Copyright (C) 2015 University of Chicago.
 * Copyright (C) 2016 Intel Corporation.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_DXT_LOG_UTILS_H
#define __DARSHAN_DXT_LOG_UTILS_H

extern struct darshan_mod_logutil_funcs dxt_posix_logutils;
extern struct darshan_mod_logutil_funcs dxt_mpiio_logutils;

void dxt_log_print_posix_file(void *file_rec, char *file_name,
        char *mnt_pt, char *fs_type, struct lustre_record_ref *rec_ref);
void dxt_log_print_mpiio_file(void *file_rec,
        char *file_name, char *mnt_pt, char *fs_type);

#endif
