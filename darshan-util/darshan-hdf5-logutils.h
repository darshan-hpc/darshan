/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_HDF5_LOG_UTILS_H
#define __DARSHAN_HDF5_LOG_UTILS_H

#include "darshan-logutils.h"
#include "darshan-hdf5-log-format.h"

extern char *hdf5_counter_names[];
extern char *hdf5_f_counter_names[];

extern struct darshan_mod_logutil_funcs hdf5_logutils;

int darshan_log_get_hdf5_file(darshan_fd fd, void **file_rec,
    darshan_record_id *rec_id);
void darshan_log_print_hdf5_file(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type);

#endif
