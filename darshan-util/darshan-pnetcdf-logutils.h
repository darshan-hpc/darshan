/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_PNETCDF_LOG_UTILS_H
#define __DARSHAN_PNETCDF_LOG_UTILS_H

#include "darshan-logutils.h"
#include "darshan-pnetcdf-log-format.h"

extern char *pnetcdf_counter_names[];
extern char *pnetcdf_f_counter_names[];

extern struct darshan_mod_logutil_funcs pnetcdf_logutils;

int darshan_log_get_pnetcdf_file(darshan_fd fd, void **file_rec,
    darshan_record_id *rec_id);
void darshan_log_print_pnetcdf_file(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type);

#endif
