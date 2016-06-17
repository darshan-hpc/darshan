/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_LOG_UTILS_COMPAT_H
#define __DARSHAN_LOG_UTILS_COMPAT_H

#include "darshan-logutils.h"

int darshan_log_get_namerecs_3_00(void *name_rec_buf, int buf_len,
    int swap_flag, struct darshan_name_record_ref **hash);

#endif
