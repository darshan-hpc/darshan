/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_HDF5_LOG_UTILS_H
#define __DARSHAN_HDF5_LOG_UTILS_H

extern char *h5f_counter_names[];
extern char *h5f_f_counter_names[];

extern char *h5d_counter_names[];
extern char *h5d_f_counter_names[];

extern struct darshan_mod_logutil_funcs hdf5_file_logutils;
extern struct darshan_mod_logutil_funcs hdf5_dataset_logutils;

#endif
