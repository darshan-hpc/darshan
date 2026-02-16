/*
 * Copyright (C) 2017 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __APSS_LOG_FORMAT_H
#define __APSS_LOG_FORMAT_H

/* current AutoPerf Cray XC log format version */
#define APSS_VER 1

#define APSS_MAGIC ('A'*0x100000000000000+\
                            'U'*0x1000000000000+\
                            'T'*0x10000000000+\
                            'O'*0x100000000+\
                            'P'*0x1000000+\
                            'E'*0x10000+\
                            'R'*0x100+\
                            'F'*0x1)

#define APSS_PERF_COUNTERS \
    /* PAPI counters */\
    X(CQ_CQ_OXE_NUM_STALLS) \
    X(CQ_CQ_OXE_NUM_FLITS) \
    X(CQ_CYCLES_BLOCKED_0) \
    X(CQ_CYCLES_BLOCKED_1) \
    X(CQ_CYCLES_BLOCKED_2) \
    X(CQ_CYCLES_BLOCKED_3) \
    /* end of counters */\
    Z(APSS_NUM_INDICES)


#define X(a) a,
#define Z(a) a
/* integer counters for the "APSS" example module */
enum darshan_apss_perf_indices
{
    APSS_PERF_COUNTERS
};

#undef Z
#undef X

/* the darshan_apss_router_record structure encompasses the data/counters
 * which would actually be logged to file by Darshan for the AP HPE Slingshot
 * module. This example implementation logs the following data for each
 * record:
 *      - a darshan_base_record structure, which contains the record id & rank
 *      - integer I/O counters 
 *      - floating point I/O counters 
 */
struct darshan_apss_perf_record
{
    struct darshan_base_record base_rec;
    int64_t group;
    int64_t chassis;
    int64_t blade;
    int64_t node;
    uint64_t counters[APSS_NUM_INDICES];
};

struct darshan_apss_header_record
{
    struct darshan_base_record base_rec;
    int64_t magic;
    int64_t nblades;
    int64_t nchassis;
    int64_t ngroups;
    uint64_t appid;
};

#endif /* __APSS_LOG_FORMAT_H */
