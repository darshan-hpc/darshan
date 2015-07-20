/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_COMMON_H
#define __DARSHAN_COMMON_H

/* simple macros for manipulating a module's counters
 * 
 * NOTE: These macros assume a module's record stores integer
 * and floating point counters in arrays, named counters and
 * fcounters, respectively. __rec_p is the a pointer to the
 * data record, __counter is the counter in question, and
 * __value is the corresponding data value.
 */
#define DARSHAN_COUNTER_SET(__rec_p, __counter, __value) do{ \
    (__rec_p)->counters[__counter] = __value; \
} while(0)

#define DARSHAN_COUNTER_F_SET(__rec_p, __counter, __value) do{ \
    (__rec_p)->fcounters[__counter] = __value; \
} while(0)

#define DARSHAN_COUNTER_INC(__rec_p, __counter, __value) do{ \
    (__rec_p)->counters[__counter] += __value; \
} while(0)

#define DARSHAN_COUNTER_F_INC(__rec_p, __counter, __value) do{ \
    (__rec_p)->fcounters[__counter] += __value; \
} while(0)

#define DARSHAN_COUNTER_VALUE(__rec_p, __counter) \
    ((__rec_p)->counters[__counter])

#define DARSHAN_COUNTER_F_VALUE(__rec_p, __counter) \
    ((__rec_p)->fcounters[__counter])

/* set __counter equal to the max of __counter or the passed in __value */
#define DARSHAN_COUNTER_MAX(__rec_p, __counter, __value) do{ \
    if((__rec_p)->counters[__counter] < __value) \
        (__rec_p)->counters[__counter] = __value; \
} while(0)

/* increment a timer counter, making sure not to account for overlap
 * with previous operations
 *
 * NOTE: __tm1 is the start timestamp of the operation, __tm2 is the end
 * timestamp of the operation, and __last is the timestamp of the end of
 * the previous I/O operation (which we don't want to overlap with).
 */
#define DARSHAN_COUNTER_F_INC_NO_OVERLAP(__rec_p, __tm1, __tm2, __last, __counter) do{ \
    if(__tm1 > __last) \
        DARSHAN_COUNTER_F_INC(__rec_p, __counter, (__tm2 - __tm1)); \
    else \
        DARSHAN_COUNTER_F_INC(__rec_p, __counter, (__tm2 - __last)); \
    if(__tm2 > __last) \
        __last = __tm2; \
} while(0)

/* increment histogram bucket depending on the given __value
 *
 * NOTE: This macro can be used to build a histogram of access
 * sizes, offsets, etc. It assumes a 10-bucket histogram, with
 * __counter_base representing the first counter in the sequence
 * of buckets (i.e., the smallest bucket). The size ranges of each
 * bucket are:
 *      * 0 - 100 bytes
 *      * 100 - 1 KiB
 *      * 1 KiB - 10 KiB
 *      * 10 KiB - 100 KiB
 *      * 100 KiB - 1 MiB
 *      * 1 MiB - 4 MiB
 *      * 4 MiB - 10 MiB
 *      * 10 MiB - 100 MiB
 *      * 100 MiB - 1 GiB
 *      * 1 GiB+
 */
#define DARSHAN_BUCKET_INC(__rec_p, __counter_base, __value) do {\
    if(__value < 101) \
        (__rec_p)->counters[__counter_base] += 1; \
    else if(__value < 1025) \
        (__rec_p)->counters[__counter_base+1] += 1; \
    else if(__value < 10241) \
        (__rec_p)->counters[__counter_base+2] += 1; \
    else if(__value < 102401) \
        (__rec_p)->counters[__counter_base+3] += 1; \
    else if(__value < 1048577) \
        (__rec_p)->counters[__counter_base+4] += 1; \
    else if(__value < 4194305) \
        (__rec_p)->counters[__counter_base+5] += 1; \
    else if(__value < 10485761) \
        (__rec_p)->counters[__counter_base+6] += 1; \
    else if(__value < 104857601) \
        (__rec_p)->counters[__counter_base+7] += 1; \
    else if(__value < 1073741825) \
        (__rec_p)->counters[__counter_base+8] += 1; \
    else \
        (__rec_p)->counters[__counter_base+9] += 1; \
} while(0)

/* i/o type (read or write) */
enum darshan_io_type
{
    DARSHAN_IO_READ = 1,
    DARSHAN_IO_WRITE = 2,
};

/***********************************************
* darshan-common functions for darshan modules *
***********************************************/

/* darshan_clean_file_path()
 *
 * Allocate a new string that contains a new cleaned-up version of
 * the file path given in 'path' argument. Converts relative paths
 * to absolute paths and filters out some potential noise in the
 * path string.
 */
char* darshan_clean_file_path(const char* path);

#endif /* __DARSHAN_COMMON_H */
