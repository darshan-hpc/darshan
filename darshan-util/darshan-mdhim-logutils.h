/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_MDHIM_LOG_UTILS_H
#define __DARSHAN_MDHIM_LOG_UTILS_H

/* declare MDHIM module counter name strings and logutil definition as
 * extern variables so they can be used in other utilities
 */
extern char *mdhim_counter_names[];
extern char *mdhim_f_counter_names[];

extern struct darshan_mod_logutil_funcs mdhim_logutils;

#endif
