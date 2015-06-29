/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_BGQ_LOG_UTILS_H
#define __DARSHAN_BGQ_LOG_UTILS_H

#include "darshan-logutils.h"
#include "darshan-bgq-log-format.h"

int darshan_log_get_bgq_file(darshan_fd fd, struct darshan_bgq_record *file);

#endif
