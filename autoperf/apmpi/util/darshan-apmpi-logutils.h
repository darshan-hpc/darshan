/*
 * Copyright (C) 2018 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __APMPI_LOG_UTILS_H
#define __APMPI_LOG_UTILS_H

extern char *apmpi_counter_names[];
extern char *apmpi_f_mpiop_totaltime_counter_names[]; 
extern char *apmpi_f_mpiop_synctime_counter_names[];
extern char *apmpi_f_mpi_global_counter_names[];
extern struct darshan_mod_logutil_funcs apmpi_logutils;

#endif

