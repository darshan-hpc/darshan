/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_COMMON_H
#define __DARSHAN_COMMON_H

/* increment a timer counter, making sure not to account for overlap
 * with previous operations
 *
 * NOTE: __timer is the corresponding timer counter variable, __tm1 is
 * the start timestamp of the operation, __tm2 is the end timestamp of
 * the operation, and __last is the timestamp of the end of the previous
 * I/O operation (which we don't want to overlap with).
 */
#define DARSHAN_TIMER_INC_NO_OVERLAP(__timer, __tm1, __tm2, __last) do{ \
    if(__tm1 > __last) \
        __timer += (__tm2 - __tm1); \
    else \
        __timer += (__tm2 - __last); \
    if(__tm2 > __last) \
        __last = __tm2; \
} while(0)

/* increment histogram bucket depending on the given __value
 *
 * NOTE: This macro can be used to build a histogram of access
 * sizes, offsets, etc. It assumes a 10-bucket histogram, with
 * __bucket_base_p pointing to the first counter in the sequence
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
#define DARSHAN_BUCKET_INC(__bucket_base_p, __value) do {\
    if(__value < 101) \
        *(__bucket_base_p) += 1; \
    else if(__value < 1025) \
        *(__bucket_base_p + 1) += 1; \
    else if(__value < 10241) \
        *(__bucket_base_p + 2) += 1; \
    else if(__value < 102401) \
        *(__bucket_base_p + 3) += 1; \
    else if(__value < 1048577) \
        *(__bucket_base_p + 4) += 1; \
    else if(__value < 4194305) \
        *(__bucket_base_p + 5) += 1; \
    else if(__value < 10485761) \
        *(__bucket_base_p + 6) += 1; \
    else if(__value < 104857601) \
        *(__bucket_base_p + 7) += 1; \
    else if(__value < 1073741825) \
        *(__bucket_base_p + 8) += 1; \
    else \
        *(__bucket_base_p + 9) += 1; \
} while(0)

/* potentially set or increment a common value counter, depending on the __count
 * for the given __value. This macro ensures common values are stored first in
 * decreasing order of their total count, and second by decreasing order of
 * their value.

 *
 * NOTE: This macro is hardcoded to expect that Darshan will only track the 4
 * most common (i.e., frequently occuring) values. __val_p is a pointer to the
 * base of the value counters (i.e., the first of 4 contiguous common value
 * counters) and __cnt_p is a pointer to the base of the count counters (i.e.
 * the first of 4 contiguous common count counters). It is assumed your counters
 * are stored as int64_t types. __online_flag is set if the common val counters are
 * updated during runtime (as opposed to being updated once at darshan shutdown time).
 */
#define DARSHAN_COMMON_VAL_COUNTER_INC(__val_p, __cnt_p, __value, __count, __online_flag) do {\
    int i; \
    int inc_count, total_count; \
    int64_t tmp_val[4] = {0}; \
    int64_t tmp_cnt[4] = {0}; \
    int tmp_ndx = 0; \
    if(__value == 0) break; \
    if(__online_flag) \
        inc_count = 1; \
    else \
        inc_count = __count; \
    for(i=0; i<4; i++) { \
        if(*(__val_p + i) == __value) { \
            total_count = *(__cnt_p + i) + inc_count; \
            break; \
        } \
    } \
    if(i == 4) total_count = __count; \
    /* first, copy over any counters that should be sorted above this one \
     * (counters with higher counts or equal counts and larger values) \
     */ \
    for(i=0;i < 4; i++) { \
        if((*(__cnt_p + i) > total_count) || \
           ((*(__cnt_p + i) == total_count) && (*(__val_p + i) > __value))) { \
            tmp_val[tmp_ndx] = *(__val_p + i); \
            tmp_cnt[tmp_ndx] = *(__cnt_p + i); \
            tmp_ndx++; \
        } \
        else break; \
    } \
    if(tmp_ndx == 4) break; /* all done, updated counter is not added */ \
    /* next, add the updated counter */ \
    tmp_val[tmp_ndx] = __value; \
    tmp_cnt[tmp_ndx] = total_count; \
    tmp_ndx++; \
    /* last, copy over any remaining counters to make sure we have 4 sets total */ \
    while(tmp_ndx != 4) { \
        if(*(__val_p + i) != __value) { \
            tmp_val[tmp_ndx] = *(__val_p + i); \
            tmp_cnt[tmp_ndx] = *(__cnt_p + i); \
            tmp_ndx++; \
        } \
        i++; \
    } \
    memcpy(__val_p, tmp_val, 4*sizeof(int64_t)); \
    memcpy(__cnt_p, tmp_cnt, 4*sizeof(int64_t)); \
} while(0)

/* maximum number of common values that darshan will track per file at
 * runtime; at shutdown time these will be reduced to the 4 most
 * frequently occuring ones
 */
#define DARSHAN_COMMON_VAL_MAX_RUNTIME_COUNT 32
struct darshan_common_val_counter
{
    int64_t val;
    int freq;
};

/* i/o type (read or write) */
enum darshan_io_type
{
    DARSHAN_IO_READ = 1,
    DARSHAN_IO_WRITE = 2,
};

/* struct used for calculating variances */
struct darshan_variance_dt
{
    double n;
    double T;
    double S;
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
char* darshan_clean_file_path(
    const char *path);

/* darshan_common_val_counter()
 *
 * Potentially increment an existing common value counter or allocate
 * a new one to keep track of commonly occuring values. Example use
 * cases would be to track the most frequent access sizes or strides
 * used by a specific module, for instance. 'common_val_root' is the
 * root pointer for the tree which stores common value info, 
 * 'common_val_count' is a pointer to the number of nodes in the 
 * tree (i.e., the number of allocated common value counters), 'val'
 * is the new value to attempt to add, 'val_p' is a pointer to the
 * base counter (i.e., the first) of the common values (which are
 * assumed to be 4 total and contiguous in memory), and 'cnt_p' is
 * a pointer to the base counter of the common counts (which are
 * again expected to be contiguous in memory).
 */
void darshan_common_val_counter(
    void **common_val_root,
    int *common_val_count,
    int64_t val,
    int64_t *val_p,
    int64_t *cnt_p);

/* darshan_walk_common_vals()
 *
 * Walks the tree of common value counters and determines the 4 most
 * frequently occuring values, storing the common values in the
 * appropriate counter fields of the given record. 'common_val_root'
 * is the root of the tree which stores the common value info, 'val_p'
 * is a pointer to the base counter (i.e., the first) of the common
 * values (which are assumed to be 4 total and contiguous in memory),
 * and 'cnt_p' is a pointer to the base counter of the common counts
 * (which are again expected to be contiguous in memory).
 */
void darshan_walk_common_vals(
    void *common_val_root,
    int64_t *val_p,
    int64_t *cnt_p);

/* darshan_variance_reduce()
 *
 * MPI reduction operation to calculate variances on counters in
 * data records which are shared across all processes. This could
 * be used, for instance, to find the variance in I/O time or total
 * bytes moved for a given data record. This function needs to be
 * passed to MPI_Op_create to obtain a corresponding MPI operation
 * which can be used to complete the reduction.  For more details,
 * consult the documentation for MPI_Op_create. Example use cases
 * can be found in the POSIX and MPIIO modules.
 */
void darshan_variance_reduce(
    void *invec,
    void *inoutvec,
    int *len,
    MPI_Datatype *dt);

#endif /* __DARSHAN_COMMON_H */
