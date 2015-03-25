/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_POSIX_LOG_UTILS_H
#define __DARSHAN_POSIX_LOG_UTILS_H

#include "darshan-logutils.h"
#include "darshan-posix-log-format.h"

int darshan_log_get_posix_file(darshan_fd fd, struct darshan_posix_file *file);

#endif
