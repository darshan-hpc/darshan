/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_IO_EVENTS_H
#define __DARSHAN_IO_EVENTS_H

typedef enum
{
    POSIX_OPEN = 0,
    POSIX_CLOSE,
    POSIX_READ,
    POSIX_WRITE,
    BARRIER,
} darshan_event_type;

struct darshan_event
{
    int64_t rank;
    darshan_event_type type;
    double start_time;
    double end_time;
    union
    {
        struct
        {
            uint64_t file;
            int create_flag;
        } open;
        struct
        {
            uint64_t file;
        } close;
        struct
        {
            uint64_t file;
            off_t offset;
            size_t size;
        } read;
        struct
        {
            uint64_t file;
            off_t offset;
            size_t size;
        } write;
        struct
        {
            int64_t proc_count;
            int64_t root;
        } barrier;
    } event_params;
};

#endif
