/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_MPIIO_LOG_UTILS_H
#define __DARSHAN_MPIIO_LOG_UTILS_H

#include "darshan-logutils.h"
#include "darshan-mpiio-log-format.h"

int darshan_log_get_mpiio_file(darshan_fd fd, struct darshan_mpiio_file *file);

#endif
