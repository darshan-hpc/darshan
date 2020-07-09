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
    if (__tm1 == 0.0 || __tm2 == 0.0) \
        break; \
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

/* maximum number of common values that darshan will track per file at runtime */
#define DARSHAN_COMMON_VAL_MAX_RUNTIME_COUNT 32
/* maximum number of counters in each common value */
#define DARSHAN_COMMON_VAL_MAX_NCOUNTERS 12

/* potentially set or add common value counters, depending on the __val_count
 * for the given __vals. This macro ensures common values are stored first in
 * decreasing order of their total count, and second by decreasing order of
 * their value.

 *
 * NOTE: This macro is hardcoded to expect that Darshan will only track the 4
 * most common (i.e., frequently occuring) values. __val_p is a pointer to the
 * base of the value counters (i.e., the first of 4 contiguous common value
 * counters) and __cnt_p is a pointer to the base of the count counters (i.e.
 * the first of 4 contiguous common count counters). It is assumed your counters
 * are stored as int64_t types. __add_flag is set if the given count should be
 * added to the common access counter, rather than just setting it.
 */
#define DARSHAN_UPDATE_COMMON_VAL_COUNTERS(__val_p, __cnt_p, __vals, __val_size, __val_count, __add_flag) do {\
    int i_; \
    int total_count = __val_count; \
    int64_t tmp_val[4*DARSHAN_COMMON_VAL_MAX_NCOUNTERS] = {0}; \
    int64_t tmp_cnt[4] = {0}; \
    int tmp_ndx = 0; \
    if(*(int64_t *)__vals == 0) break; \
    for(i_=0; i_<4; i_++) { \
        if(__add_flag && \
            !memcmp(__val_p + (i_ * (__val_size)), \
                __vals, sizeof(*__vals) * (__val_size))) { \
            total_count += *(__cnt_p + i_); \
            break; \
        } \
    } \
    /* first, copy over any counters that should be sorted above this one \
     * (counters with higher counts or equal counts and larger values) \
     */ \
    for(i_=0; i_ < 4; i_++) { \
        if((*(__cnt_p + i_) > total_count) || \
           ((*(__cnt_p + i_) == total_count) && \
           (*(__val_p + (i_ * (__val_size))) > *(int64_t *)__vals))) { \
            memcpy(&tmp_val[tmp_ndx * (__val_size)], __val_p + (i_ * (__val_size)), \
                sizeof(*__vals) * (__val_size)); \
            tmp_cnt[tmp_ndx] = *(__cnt_p + i_); \
            tmp_ndx++; \
        } \
        else break; \
    } \
    if(tmp_ndx == 4) break; /* all done, updated counter is not added */ \
    /* next, add the updated counter */ \
    memcpy(&tmp_val[tmp_ndx * (__val_size)], __vals, sizeof(*__vals) * (__val_size)); \
    tmp_cnt[tmp_ndx] = total_count; \
    tmp_ndx++; \
    /* last, copy over any remaining counters to make sure we have 4 sets total */ \
    while(tmp_ndx != 4) { \
        if(memcmp(__val_p + (i_ * (__val_size)), \
                __vals, sizeof(*__vals) * (__val_size))) { \
            memcpy(&tmp_val[tmp_ndx * (__val_size)], __val_p + (i_ * (__val_size)), \
                sizeof(*__vals) * (__val_size)); \
            tmp_cnt[tmp_ndx] = *(__cnt_p + i_); \
            tmp_ndx++; \
        } \
        i_++; \
    } \
    memcpy(__val_p, tmp_val, 4*sizeof(int64_t)*(__val_size)); \
    memcpy(__cnt_p, tmp_cnt, 4*sizeof(int64_t)); \
} while(0)

struct darshan_common_val_counter
{
    int64_t vals[DARSHAN_COMMON_VAL_MAX_NCOUNTERS];
    int nvals;
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

/* darshan_lookup_record_ref()
 *
 * Lookup a record reference pointer using the given 'handle'.
 * 'handle_sz' is the size of the handle structure, and 'hash_head'
 * is the pointer to the hash table to search.
 * If the handle is found, the corresponding record reference pointer
 * is returned, otherwise NULL is returned.
 */
void *darshan_lookup_record_ref(
    void *hash_head,
    void *handle,
    size_t handle_sz);

/* darshan_add_record_ref()
 *
 * Add the given record reference pointer, 'rec_ref_p' to the hash
 * table whose address is stored in the 'hash_head_p' pointer. The
 * hash is generated from the given 'handle', with size 'handle_sz'.
 * If the record reference is successfully added, 1 is returned,
 * otherwise, 0 is returned.
 */
int darshan_add_record_ref(
    void **hash_head_p,
    void *handle,
    size_t handle_sz,
    void *rec_ref_p);

/* darshan_delete_record_ref()
 *
 * Delete the record reference for the given 'handle', with size
 * 'handle_sz', from the hash table whose address is stored in
 * the 'hash_head_p' pointer.
 * On success deletion, the corresponding record reference pointer
 * is returned, otherwise NULL is returned.
 */
void *darshan_delete_record_ref(
    void **hash_head_p,
    void *handle,
    size_t handle_sz);

/* darshan_clear_record_refs()
 *
 * Clear all record references from the hash table stored in the
 * 'hash_head_p' pointer. If 'free_flag' is set, the corresponding
 * record_reference_pointer is also freed.
 */
void darshan_clear_record_refs(
    void **hash_head_p,    
    int free_flag);

/* darshan_iter_record_ref()
 *
 * Iterate each record reference stored in the hash table pointed
 * to by 'hash_head' and perform the given action 'iter_action'. 
 * The action function takes two pointers as input: the first
 * points to the corresponding record reference pointer and the
 * second is a user-supplied pointer provided for the action.
 * 'user_ptr' is the user-supplied pointer that will be passed
 * as the 2nd argument to 'iter_action'.
 */
void darshan_iter_record_refs(
    void *hash_head,
    void (*iter_action)(void *, void *),
    void *user_ptr);

/* darshan_clean_file_path()
 *
 * Allocate a new string that contains a new cleaned-up version of
 * the file path given in 'path' argument. Converts relative paths
 * to absolute paths and filters out some potential noise in the
 * path string.
 */
char* darshan_clean_file_path(
    const char *path);

/* darshan_record_sort()
 *
 * Sort the records in 'rec_buf' by descending rank to get all
 * shared records in a contiguous region at the end of the buffer.
 * Records are secondarily sorted by ascending record identifiers.
 * 'rec_count' is the number of records in the buffer, and 'rec_size'
 * is the size of the record structure.
 * NOTE: this function only works on fixed-length records.
 */
void darshan_record_sort(
    void *rec_buf,
    int rec_count,
    int rec_size);

/* darshan_track_common_val_counters()
 *
 * Potentially increment an existing common value counter or allocate
 * a new one to keep track of commonly occuring values. Example use
 * cases would be to track the most frequent access sizes or strides
 * used by a specific module, for instance. 'common_val_root' is the
 * root pointer for the tree which stores common value info, 
 * 'common_val_count' is a pointer to the number of nodes in the 
 * tree (i.e., the number of allocated common value counters), 'vals'
 * is the set of new values to attempt to add, and 'nvals' is the
 * total number of values in the 'vals' pointer.
 */
struct darshan_common_val_counter *darshan_track_common_val_counters(
    void **common_val_root,
    int64_t *vals,
    int nvals,
    int *common_val_count);

#ifdef HAVE_MPI
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
#endif

#endif /* __DARSHAN_COMMON_H */
