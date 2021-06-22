/*
 * Copyright (C) 2021 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

/* TODO: explain */

#include "darshan-runtime-config.h"

#include <time.h>
#include <stdio.h>

int darshan_aux_clock_gettime(clockid_t clockid, struct timespec *tp)
{
    return(clock_gettime(clockid, tp));
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
