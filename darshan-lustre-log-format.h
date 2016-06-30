/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_LUSTRE_LOG_FORMAT_H
#define __DARSHAN_LUSTRE_LOG_FORMAT_H

/* NOTE -- redefining the size of OST_ID will require changing the DARSHAN_BSWAP
 * macro used in darshan-util/darshan-lustre-logutils.c as well
 */
typedef int64_t OST_ID;

/* current Lustre log format version */
#define DARSHAN_LUSTRE_VER 1

#define LUSTRE_COUNTERS \
    /* number of OSTs for file system */\
    X(LUSTRE_OSTS) \
    /* number of MDTs for file system */\
    X(LUSTRE_MDTS) \
    /* index of first OST for file */\
    X(LUSTRE_STRIPE_OFFSET) \
    /* bytes per stripe for file */\
    X(LUSTRE_STRIPE_SIZE) \
    /* number of stripes (OSTs) for file */\
    X(LUSTRE_STRIPE_WIDTH) \
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
    struct darshan_base_record base_rec;
    int64_t counters[LUSTRE_NUM_INDICES];
    OST_ID ost_ids[1];
};

/*
 *  helper function to calculate the size of a record
 */
#define LUSTRE_RECORD_SIZE( osts ) ( sizeof(struct darshan_lustre_record) + sizeof(OST_ID) * (osts - 1) )

#endif /* __DARSHAN_LUSTRE_LOG_FORMAT_H */
