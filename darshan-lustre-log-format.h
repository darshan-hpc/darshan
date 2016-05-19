/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_LUSTRE_LOG_FORMAT_H
#define __DARSHAN_LUSTRE_LOG_FORMAT_H

/* current Lustre log format version */
#define DARSHAN_LUSTRE_VER 1

/* TODO: add integer counters here (e.g., counter for stripe width, stripe size, etc etc) */
#define LUSTRE_COUNTERS \
    /* number of OSTs for file system */\
    X(LUSTRE_OSTS) \
    /* number of MDTs for file system */\
    X(LUSTRE_MDTS) \
    /* bytes per stripe for file */\
    X(LUSTRE_STRIPE_SIZE) \
    /* number of stripes (OSTs) for file */\
    X(LUSTRE_STRIPE_WIDTH) \
    /* index of first OST for file */\
    X(LUSTRE_STRIPE_OFFSET) \
    /* end of counters */\
    X(LUSTRE_NUM_INDICES)

#define X(a) a,
/* integer statistics for Lustre file records */
enum darshan_lustre_indices
{
    LUSTRE_COUNTERS
};
#undef X

/* record structure for the Lustre module. a record is created and stored for
 * every file opened that belongs to a Lustre file system. This record includes:
 *      - a corresponding record identifier (created by hashing the file path)
 *      - the rank of the process which opened the file (-1 for shared files)
 *      - integer file I/O statistics (stripe size, width, # of OSTs, etc.)
 */
struct darshan_lustre_record
{
    darshan_record_id rec_id;
    int64_t rank;
    int64_t counters[LUSTRE_NUM_INDICES];
};

#endif /* __DARSHAN_LUSTRE_LOG_FORMAT_H */
