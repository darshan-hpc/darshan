/*
 *  (C) 2014 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

/* This header file defines an external API for applications to make
 * explicit calls to control Darshan behavior
 */

#ifndef __DARSHAN_EXT_H
#define __DARSHAN_EXT_H

#include <unistd.h>
#include <sys/types.h>
#include <stdint.h>
#include <mpi.h>

void darshan_start_epoch(void);
void darshan_end_epoch(void);

#endif /* __DARSHAN_EXT_H */
