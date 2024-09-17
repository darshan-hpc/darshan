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
#define DARSHAN_LUSTRE_VER 2

#define LUSTRE_COMP_COUNTERS \
    /* component stripe size */\
    X(LUSTRE_COMP_STRIPE_SIZE) \
    /* component stripe count */\
    X(LUSTRE_COMP_STRIPE_COUNT) \
    /* component stripe pattern */\
    X(LUSTRE_COMP_STRIPE_PATTERN) \
    /* component flags */\
    X(LUSTRE_COMP_FLAGS) \
    /* component starting extent */\
    X(LUSTRE_COMP_EXT_START) \
    /* component ending extent */\
    X(LUSTRE_COMP_EXT_END) \
    /* component mirror ID */\
    X(LUSTRE_COMP_MIRROR_ID) \
    /* end of counters */\
    X(LUSTRE_COMP_NUM_INDICES)

#define X(a) a,
/* integer statistics for Lustre file records */
enum darshan_lustre_indices
{
    LUSTRE_COMP_COUNTERS
};
#undef X

/* detailed counters describing parameters of a Lustre file layout component */
struct darshan_lustre_component
{
    int64_t counters[LUSTRE_COMP_NUM_INDICES];
    char pool_name[16];
};

/* file record structure for the Lustre module. a record is created and stored for
 * every file opened that belongs to a Lustre file system. This record includes:
 *      - a corresponding record identifier (created by hashing the file path)
 *      - the rank of the process which opened the file (-1 for shared files)
 *      - total number of file layout components instrumented
 *      - total number of file stripes instrumented
 *      - detailed counters describing each file layout component (e.g., stripe width, count, etc.)
 *      - list of OST IDs corresponding to instrumented file layout components
 */
struct darshan_lustre_record
{
    struct darshan_base_record base_rec;
    int64_t num_comps;
    int64_t num_stripes;
    struct darshan_lustre_component *comps;
    OST_ID *ost_ids;
};

/*
 *  helper macro to calculate the serialized size of a Lustre record
 *  NOTE: this must be kept in sync with the definitions above
 */
#define LUSTRE_RECORD_SIZE(comps, stripes) \
     (sizeof(struct darshan_base_record) + (2*sizeof(int64_t)) + \
     (sizeof(struct darshan_lustre_component) * (comps)) + \
     (sizeof(OST_ID) * (stripes)))

#endif /* __DARSHAN_LUSTRE_LOG_FORMAT_H */
