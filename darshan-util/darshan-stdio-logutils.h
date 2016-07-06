/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_STDIO_LOG_UTILS_H
#define __DARSHAN_STDIO_LOG_UTILS_H

/* declare STDIO module counter name strings and logutil definition as
 * extern variables so they can be used in other utilities
 */
extern char *stdio_counter_names[];
extern char *stdio_f_counter_names[];

extern struct darshan_mod_logutil_funcs stdio_logutils;

#endif
