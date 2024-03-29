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

// XXX backwards compatability
/* current Lustre log format version */
#define DARSHAN_LUSTRE_VER 2

// XXX can we still support: LUSTRE_OSTS, LUSTRE_MDTS
// XXX stop supporting STRIPE_OFFSET
#define LUSTRE_COUNTERS \
    /* bytes per stripe for file */\
    X(LUSTRE_COMP_STRIPE_SIZE) \
    /* number of stripes (OSTs) for file */\
    X(LUSTRE_COMP_STRIPE_WIDTH) \
    /* end of counters */\
    X(LUSTRE_COMP_NUM_INDICES)

#define X(a) a,
/* integer statistics for Lustre file records */
enum darshan_lustre_indices
{
    LUSTRE_COUNTERS
};
#undef X

/* XXX */
struct darshan_lustre_component_record
{
    int64_t counters[LUSTRE_COMP_NUM_INDICES];
    //OST_ID ost_ids[1]; // XXX this won't work
};

// XXX update
/* record structure for the Lustre module. a record is created and stored for
 * every file opened that belongs to a Lustre file system. This record includes:
 *      - a corresponding record identifier (created by hashing the file path)
 *      - the rank of the process which opened the file (-1 for shared files)
 *      - integer file I/O statistics (stripe size, width, # of OSTs, etc.)
 */
struct darshan_lustre_record
{
    struct darshan_base_record base_rec;
    int64_t num_comps;
    struct darshan_lustre_component_record comps[1];
};

/*
 *  helper function to calculate the size of a record
 */
#define LUSTRE_RECORD_SIZE(comps) (sizeof(struct darshan_lustre_record) + \
    sizeof(struct darshan_lustre_component_record) * (comps - 1))

#endif /* __DARSHAN_LUSTRE_LOG_FORMAT_H */
