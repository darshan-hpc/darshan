/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_PNETCDF_LOG_UTILS_H
#define __DARSHAN_PNETCDF_LOG_UTILS_H

extern char *pnetcdf_file_counter_names[];
extern char *pnetcdf_file_f_counter_names[];

extern char *pnetcdf_var_counter_names[];
extern char *pnetcdf_var_f_counter_names[];

extern struct darshan_mod_logutil_funcs pnetcdf_file_logutils;
extern struct darshan_mod_logutil_funcs pnetcdf_var_logutils;

#endif
