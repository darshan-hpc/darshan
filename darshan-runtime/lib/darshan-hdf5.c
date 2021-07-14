/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifdef HAVE_CONFIG_H
# include <darshan-runtime-config.h>
#endif

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <errno.h>
#include <search.h>
#include <assert.h>
#include <pthread.h>

#include "darshan.h"
#include "darshan-dynamic.h"

#include <hdf5.h>

/* H5F prototypes */
DARSHAN_FORWARD_DECL(H5Fcreate, hid_t, (const char *filename, unsigned flags, hid_t create_plist, hid_t access_plist));
DARSHAN_FORWARD_DECL(H5Fopen, hid_t, (const char *filename, unsigned flags, hid_t access_plist));
DARSHAN_FORWARD_DECL(H5Fflush, herr_t, (hid_t object_id, H5F_scope_t scope));
DARSHAN_FORWARD_DECL(H5Fclose, herr_t, (hid_t file_id));

/* H5D prototypes */
DARSHAN_FORWARD_DECL(H5Dcreate1, hid_t, (hid_t loc_id, const char *name, hid_t type_id, hid_t space_id, hid_t dcpl_id));
DARSHAN_FORWARD_DECL(H5Dcreate2, hid_t, (hid_t loc_id, const char *name, hid_t dtype_id, hid_t space_id, hid_t lcpl_id, hid_t dcpl_id, hid_t dapl_id));
DARSHAN_FORWARD_DECL(H5Dopen1, hid_t, (hid_t loc_id, const char *name));
DARSHAN_FORWARD_DECL(H5Dopen2, hid_t, (hid_t loc_id, const char *name, hid_t dapl_id));
DARSHAN_FORWARD_DECL(H5Dread, herr_t, (hid_t dataset_id, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t xfer_plist_id, void * buf));
DARSHAN_FORWARD_DECL(H5Dwrite, herr_t, (hid_t dataset_id, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t xfer_plist_id, const void * buf));
#ifdef DARSHAN_HDF5_VERS_1_10_PLUS
DARSHAN_FORWARD_DECL(H5Dflush, herr_t, (hid_t dataset_id));
#endif
DARSHAN_FORWARD_DECL(H5Dclose, herr_t, (hid_t dataset_id));

/* structure that can track i/o stats for a given HDF5 file record at runtime */
struct hdf5_file_record_ref
{
    struct darshan_hdf5_file* file_rec;
    double last_meta_end;
}; 

/* structure that can track i/o stats for a given HDF5 dataset record at runtime */
struct hdf5_dataset_record_ref
{
    struct darshan_hdf5_dataset* dataset_rec;
    enum darshan_io_type last_io_type;
    double last_read_end;
    double last_write_end;
    double last_meta_end;
    void *access_root;
    int access_count;
};

/* struct to encapsulate runtime state for the HDF5 module */
struct hdf5_runtime
{
    void *rec_id_hash;
    void *hid_hash;
    int rec_count;
};

static void hdf5_file_runtime_initialize(
    void);
static void hdf5_dataset_runtime_initialize(
    void);
static struct hdf5_file_record_ref *hdf5_track_new_file_record(
    darshan_record_id rec_id, const char *rec_name);
static struct hdf5_dataset_record_ref *hdf5_track_new_dataset_record(
    darshan_record_id rec_id, const char *rec_name);
static void hdf5_finalize_dataset_records(
    void *rec_ref_p, void *user_ptr);
#ifdef HAVE_MPI
static void hdf5_file_record_reduction_op(
    void* inrec_v, void* inoutrec_v, int *len, MPI_Datatype *datatype);
static void hdf5_dataset_record_reduction_op(
    void* inrec_v, void* inoutrec_v, int *len, MPI_Datatype *datatype);
static void hdf5_shared_dataset_record_variance(
    MPI_Comm mod_comm, struct darshan_hdf5_dataset *inrec_array,
    struct darshan_hdf5_dataset *outrec_array, int shared_rec_count);
static void hdf5_file_mpi_redux(
    void *hdf5_buf, MPI_Comm mod_comm,
    darshan_record_id *shared_recs, int shared_rec_count);
static void hdf5_dataset_mpi_redux(
    void *hdf5_buf, MPI_Comm mod_comm,
    darshan_record_id *shared_recs, int shared_rec_count);
#endif
static void hdf5_file_output(
    void **hdf5_buf, int *hdf5_buf_sz);
static void hdf5_dataset_output(
    void **hdf5_buf, int *hdf5_buf_sz);
static void hdf5_file_cleanup(
    void);
static void hdf5_dataset_cleanup(
    void);

static struct hdf5_runtime *hdf5_file_runtime = NULL;
static struct hdf5_runtime *hdf5_dataset_runtime = NULL;
static pthread_mutex_t hdf5_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int my_rank = -1;

#define HDF5_LOCK() pthread_mutex_lock(&hdf5_runtime_mutex)
#define HDF5_UNLOCK() pthread_mutex_unlock(&hdf5_runtime_mutex)

/*********************************************************
 *        Wrappers for H5F functions of interest         * 
 *********************************************************/

#define H5F_PRE_RECORD() do { \
    HDF5_LOCK(); \
    if(!darshan_core_disabled_instrumentation()) { \
        if(!hdf5_file_runtime) hdf5_file_runtime_initialize(); \
        if(hdf5_file_runtime) break; \
    } \
    HDF5_UNLOCK(); \
    return(ret); \
} while(0)

#define H5F_POST_RECORD() do { \
    HDF5_UNLOCK(); \
} while(0)

#define H5F_RECORD_OPEN(__ret, __path, __use_mpio, __tm1, __tm2) do { \
    darshan_record_id __rec_id; \
    struct hdf5_file_record_ref *__rec_ref; \
    char *__newpath; \
    __newpath = darshan_clean_file_path(__path); \
    if(!__newpath) __newpath = (char *)__path; \
    if(darshan_core_excluded_path(__newpath)) { \
        if(__newpath != __path) free(__newpath); \
        break; \
    } \
    __rec_id = darshan_core_gen_record_id(__newpath); \
    __rec_ref = darshan_lookup_record_ref(hdf5_file_runtime->rec_id_hash, &__rec_id, sizeof(darshan_record_id)); \
    if(!__rec_ref) __rec_ref = hdf5_track_new_file_record(__rec_id, __newpath); \
    if(!__rec_ref) { \
        if(__newpath != __path) free(__newpath); \
        break; \
    } \
    __rec_ref->file_rec->counters[H5F_USE_MPIIO] = __use_mpio; \
    __rec_ref->file_rec->counters[H5F_OPENS] += 1; \
    if(__rec_ref->file_rec->fcounters[H5F_F_OPEN_START_TIMESTAMP] == 0 || \
     __rec_ref->file_rec->fcounters[H5F_F_OPEN_START_TIMESTAMP] > __tm1) \
        __rec_ref->file_rec->fcounters[H5F_F_OPEN_START_TIMESTAMP] = __tm1; \
    __rec_ref->file_rec->fcounters[H5F_F_OPEN_END_TIMESTAMP] = __tm2; \
    DARSHAN_TIMER_INC_NO_OVERLAP(__rec_ref->file_rec->fcounters[H5F_F_META_TIME], \
        __tm1, __tm2, __rec_ref->last_meta_end); \
    darshan_add_record_ref(&(hdf5_file_runtime->hid_hash), &__ret, sizeof(hid_t), __rec_ref); \
    if(__newpath != __path) free(__newpath); \
} while(0)

hid_t DARSHAN_DECL(H5Fcreate)(const char *filename, unsigned flags,
    hid_t create_plist, hid_t access_plist)
{
    hid_t ret;
    char* tmp;
    double tm1, tm2;
    unsigned majnum, minnum, relnum;
    int tmp_rank = my_rank;
    int use_mpio = 0;

    H5get_libversion(&majnum, &minnum, &relnum);
#ifdef DARSHAN_HDF5_VERS_1_10_PLUS
    if((majnum == 1) && (minnum < 10))
    {
        if(tmp_rank < 0)
#ifdef HAVE_MPI
            MPI_Comm_rank(MPI_COMM_WORLD, &tmp_rank);
#else
            tmp_rank = 0;
#endif

        if(tmp_rank == 0)
        {
            darshan_core_fprintf(stderr, "Darshan HDF5 module error: runtime library version (%d.%d) incompatible with Darshan module (1.10+).\n", majnum, minnum);
        }
        return(-1);
    }
#else
    if((majnum > 1) || (minnum >= 10))
    {
        if(tmp_rank < 0)
#ifdef HAVE_MPI
            MPI_Comm_rank(MPI_COMM_WORLD, &tmp_rank);
#else
            tmp_rank = 0;
#endif

        if(tmp_rank == 0)
        {
            darshan_core_fprintf(stderr, "Darshan HDF5 module error: runtime library version (%d.%d) incompatible with Darshan module (1.10-).\n", majnum, minnum);
        }
        return(-1);
    }
#endif

    MAP_OR_FAIL(H5Fcreate);

    tm1 = darshan_core_wtime();
    ret = __real_H5Fcreate(filename, flags, create_plist, access_plist);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        /* use ROMIO approach to strip prefix if present */
        /* strip off prefix if there is one, but only skip prefixes
         * if they are greater than length one to allow for windows
         * drive specifications (e.g. c:\...) 
         */
        tmp = strchr(filename, ':');
        if (tmp > filename + 1) {
            filename = tmp + 1;
        }

#ifdef DARSHAN_HDF5_PAR_BUILD
        if(access_plist != H5P_DEFAULT && H5Pget_driver(access_plist) == H5FD_MPIO)
            use_mpio = 1;
#endif

        H5F_PRE_RECORD();
        H5F_RECORD_OPEN(ret, filename, use_mpio, tm1, tm2);
        H5F_POST_RECORD();
    }

    return(ret);
}

hid_t DARSHAN_DECL(H5Fopen)(const char *filename, unsigned flags,
    hid_t access_plist)
{
    hid_t ret;
    char* tmp;
    double tm1, tm2;
    unsigned majnum, minnum, relnum;
    int tmp_rank = my_rank;
    int use_mpio = 0;

    H5get_libversion(&majnum, &minnum, &relnum);
#ifdef DARSHAN_HDF5_VERS_1_10_PLUS
    if((majnum == 1) && (minnum < 10))
    {
        if(tmp_rank < 0)
#ifdef HAVE_MPI
            MPI_Comm_rank(MPI_COMM_WORLD, &tmp_rank);
#else
            tmp_rank = 0;
#endif

        if(tmp_rank == 0)
        {
            darshan_core_fprintf(stderr, "Darshan HDF5 module error: runtime library version (%d.%d) incompatible with Darshan module (1.10+).\n", majnum, minnum);
        }
        return(-1);
    }
#else
    if((majnum > 1) || (minnum >= 10))
    {
        if(tmp_rank < 0)
#ifdef HAVE_MPI
            MPI_Comm_rank(MPI_COMM_WORLD, &tmp_rank);
#else
            tmp_rank = 0;
#endif

        if(tmp_rank == 0)
        {
            darshan_core_fprintf(stderr, "Darshan HDF5 module error: runtime library version (%d.%d) incompatible with Darshan module (1.10-).\n", majnum, minnum);
        }
        return(-1);
    }
#endif

    MAP_OR_FAIL(H5Fopen);

    tm1 = darshan_core_wtime();
    ret = __real_H5Fopen(filename, flags, access_plist);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        /* use ROMIO approach to strip prefix if present */
        /* strip off prefix if there is one, but only skip prefixes
         * if they are greater than length one to allow for windows
         * drive specifications (e.g. c:\...) 
         */
        tmp = strchr(filename, ':');
        if (tmp > filename + 1) {
            filename = tmp + 1;
        }

#ifdef DARSHAN_HDF5_PAR_BUILD
        if(access_plist != H5P_DEFAULT && H5Pget_driver(access_plist) == H5FD_MPIO)
            use_mpio = 1;
#endif

        H5F_PRE_RECORD();
        H5F_RECORD_OPEN(ret, filename, use_mpio, tm1, tm2);
        H5F_POST_RECORD();
    }

    return(ret);

}

herr_t DARSHAN_DECL(H5Fflush)(hid_t object_id, H5F_scope_t scope)
{
    struct hdf5_file_record_ref *rec_ref;
    hid_t file_id;
    double tm1, tm2;
    herr_t ret;

    MAP_OR_FAIL(H5Fflush);

    tm1 = darshan_core_wtime();
    ret = __real_H5Fflush(object_id, scope);
    tm2 = darshan_core_wtime();

    /* convert object_id to file_id so we can look it up */
    if(ret >= 0)
    {
        file_id = H5Iget_file_id(object_id);
        if(file_id > 0)
        {
            H5F_PRE_RECORD();
            rec_ref = darshan_lookup_record_ref(hdf5_file_runtime->hid_hash,
                &file_id, sizeof(hid_t));
            if(rec_ref)
            {
                rec_ref->file_rec->counters[H5F_FLUSHES] += 1;
                DARSHAN_TIMER_INC_NO_OVERLAP(
                    rec_ref->file_rec->fcounters[H5F_F_META_TIME],
                    tm1, tm2, rec_ref->last_meta_end);
            }
            H5F_POST_RECORD();
        }
    }

    return(ret);
}

herr_t DARSHAN_DECL(H5Fclose)(hid_t file_id)
{
    struct hdf5_file_record_ref *rec_ref;
    double tm1, tm2;
    herr_t ret;

    MAP_OR_FAIL(H5Fclose);

    tm1 = darshan_core_wtime();
    ret = __real_H5Fclose(file_id);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        H5F_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(hdf5_file_runtime->hid_hash,
            &file_id, sizeof(hid_t));
        if(rec_ref)
        {
            if(rec_ref->file_rec->fcounters[H5F_F_CLOSE_START_TIMESTAMP] == 0 ||
             rec_ref->file_rec->fcounters[H5F_F_CLOSE_START_TIMESTAMP] > tm1)
               rec_ref->file_rec->fcounters[H5F_F_CLOSE_START_TIMESTAMP] = tm1;
            rec_ref->file_rec->fcounters[H5F_F_CLOSE_END_TIMESTAMP] = tm2;
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->file_rec->fcounters[H5F_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            darshan_delete_record_ref(&(hdf5_file_runtime->hid_hash),
                &file_id, sizeof(hid_t));
        }
        H5F_POST_RECORD();
    }

    return(ret);

}

/*********************************************************
 *        Wrappers for H5D functions of interest         * 
 *********************************************************/

#define DARSHAN_HDF5_MAX_NAME_LEN 256
#define DARSHAN_HDF5_DATASET_DELIM ":"

#define H5D_PRE_RECORD() do { \
    HDF5_LOCK(); \
    if(!darshan_core_disabled_instrumentation()) { \
        if(!hdf5_dataset_runtime) hdf5_dataset_runtime_initialize(); \
        if(hdf5_dataset_runtime) break; \
    } \
    HDF5_UNLOCK(); \
    return(ret); \
} while(0)

#define H5D_POST_RECORD() do { \
    HDF5_UNLOCK(); \
} while(0)

#define H5D_RECORD_OPEN(__ret, __loc_id, __name, __type_id, __space_id, __dcpl_id, __use_depr,  __tm1, __tm2) do { \
    char *__file_path, *__tmp_ptr; \
    char __rec_name[DARSHAN_HDF5_MAX_NAME_LEN] = {0}; \
    ssize_t __req_name_len = DARSHAN_HDF5_MAX_NAME_LEN-1, __ret_name_len; \
    darshan_record_id __rec_id, __file_rec_id = 0; \
    struct hdf5_dataset_record_ref *__rec_ref; \
    hsize_t __chunk_dims[H5D_MAX_NDIMS] = {0}; \
    int __i, __n_chunk_dims = 0; \
    /* get corresponding file name */\
    __ret_name_len = H5Fget_name(__loc_id, __rec_name, __req_name_len); \
    if(__ret_name_len < 0) break; \
    else if(__ret_name_len < __req_name_len) { \
        /* fully resolve file path */\
        __file_path = darshan_clean_file_path(__rec_name); \
        if(darshan_core_excluded_path(__file_path)) { \
            free(__file_path); \
            break; \
        } \
        __file_rec_id = darshan_core_gen_record_id(__file_path); \
        strncpy(__rec_name, __file_path, __req_name_len); \
        free(__file_path); \
        if(strlen(__rec_name) + 2 <= __req_name_len) { \
            /* append dataset name if we have space */\
            __tmp_ptr = __rec_name + strlen(__rec_name); \
            strcat(__tmp_ptr, DARSHAN_HDF5_DATASET_DELIM); \
            __tmp_ptr += 1; \
            __req_name_len = DARSHAN_HDF5_MAX_NAME_LEN - 1 - strlen(__rec_name); \
            __ret_name_len = H5Iget_name(__loc_id, __tmp_ptr, __req_name_len); \
            if(__ret_name_len < 0) return(ret); \
            else if(__ret_name_len < __req_name_len) { \
                __tmp_ptr = __rec_name + strlen(__rec_name); \
                __req_name_len = DARSHAN_HDF5_MAX_NAME_LEN - 1 - strlen(__rec_name); \
                strncat(__tmp_ptr, __name, __req_name_len); \
            } \
        } \
    } \
    __rec_id = darshan_core_gen_record_id(__rec_name); \
    __rec_ref = darshan_lookup_record_ref(hdf5_dataset_runtime->rec_id_hash, &__rec_id, sizeof(darshan_record_id)); \
    if(!__rec_ref) __rec_ref = hdf5_track_new_dataset_record(__rec_id, __rec_name); \
    if(!__rec_ref) break; \
    __rec_ref->dataset_rec->counters[H5D_OPENS] += 1; \
    __rec_ref->dataset_rec->counters[H5D_USE_DEPRECATED] = __use_depr; \
    if(__rec_ref->dataset_rec->fcounters[H5D_F_OPEN_START_TIMESTAMP] == 0 || \
     __rec_ref->dataset_rec->fcounters[H5D_F_OPEN_START_TIMESTAMP] > __tm1) \
        __rec_ref->dataset_rec->fcounters[H5D_F_OPEN_START_TIMESTAMP] = __tm1; \
    __rec_ref->dataset_rec->fcounters[H5D_F_OPEN_END_TIMESTAMP] = __tm2; \
    DARSHAN_TIMER_INC_NO_OVERLAP(__rec_ref->dataset_rec->fcounters[H5D_F_META_TIME], \
        __tm1, __tm2, __rec_ref->last_meta_end); \
    __rec_ref->dataset_rec->counters[H5D_DATASPACE_NDIMS] = H5Sget_simple_extent_ndims(__space_id); \
    __rec_ref->dataset_rec->counters[H5D_DATASPACE_NPOINTS] = H5Sget_simple_extent_npoints(__space_id); \
    if(__dcpl_id != H5P_DEFAULT && H5Pget_layout(__dcpl_id) == H5D_CHUNKED) { \
        __n_chunk_dims = H5Pget_chunk(__dcpl_id, H5D_MAX_NDIMS, __chunk_dims); \
        __n_chunk_dims = (__n_chunk_dims < H5D_MAX_NDIMS) ? __n_chunk_dims : H5D_MAX_NDIMS; \
        for(__i = 0; __i < __n_chunk_dims; __i++) \
            __rec_ref->dataset_rec->counters[H5D_CHUNK_SIZE_D1 + __i] = __chunk_dims[__n_chunk_dims - __i - 1]; \
    } \
    __rec_ref->dataset_rec->counters[H5D_DATATYPE_SIZE] = H5Tget_size(__type_id); \
    __rec_ref->dataset_rec->file_rec_id = __file_rec_id; \
    darshan_add_record_ref(&(hdf5_dataset_runtime->hid_hash), &__ret, sizeof(hid_t), __rec_ref); \
} while(0)

hid_t DARSHAN_DECL(H5Dcreate1)(hid_t loc_id, const char *name, hid_t type_id, hid_t space_id, hid_t dcpl_id)
{
    double tm1, tm2;
    hid_t ret;

    MAP_OR_FAIL(H5Dcreate1);

    tm1 = darshan_core_wtime();
    ret = __real_H5Dcreate1(loc_id, name, type_id, space_id, dcpl_id);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        H5D_PRE_RECORD();
        H5D_RECORD_OPEN(ret, loc_id, name, type_id, space_id, dcpl_id, 1, tm1, tm2);
        H5D_POST_RECORD();
    }

    return(ret);
}

hid_t DARSHAN_DECL(H5Dcreate2)(hid_t loc_id, const char *name, hid_t dtype_id, hid_t space_id,
    hid_t lcpl_id, hid_t dcpl_id, hid_t dapl_id)
{
    double tm1, tm2;
    hid_t ret;

    MAP_OR_FAIL(H5Dcreate2);

    tm1 = darshan_core_wtime();
    ret = __real_H5Dcreate2(loc_id, name, dtype_id, space_id, lcpl_id, dcpl_id, dapl_id);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        H5D_PRE_RECORD();
        H5D_RECORD_OPEN(ret, loc_id, name, dtype_id, space_id, dcpl_id, 0, tm1, tm2);
        H5D_POST_RECORD();
    }

    return(ret);
}

hid_t DARSHAN_DECL(H5Dopen1)(hid_t loc_id, const char *name)
{
    hid_t dtype_id;
    hid_t space_id;
    hid_t dcpl_id;
    double tm1, tm2;
    hid_t ret;

    MAP_OR_FAIL(H5Dopen1);

    tm1 = darshan_core_wtime();
    ret = __real_H5Dopen1(loc_id, name);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        /* query dataset datatype, dataspace, and creation property list */
        dtype_id = H5Dget_type(ret);
        if(dtype_id < 0)
            return(ret);
        space_id = H5Dget_space(ret);
        if(space_id < 0)
        {
            H5Tclose(dtype_id);
            return(ret);
        }
        dcpl_id = H5Dget_create_plist(ret);
        if(dcpl_id < 0)
        {
            H5Tclose(dtype_id);
            H5Sclose(space_id);
            return(ret);
        }

        H5D_PRE_RECORD();
        H5D_RECORD_OPEN(ret, loc_id, name, dtype_id, space_id, dcpl_id, 1, tm1, tm2);
        H5D_POST_RECORD();

        H5Tclose(dtype_id);
        H5Sclose(space_id);
        H5Pclose(dcpl_id);
    }

    return(ret);
}

hid_t DARSHAN_DECL(H5Dopen2)(hid_t loc_id, const char *name, hid_t dapl_id)
{
    hid_t dtype_id;
    hid_t space_id;
    hid_t dcpl_id;
    double tm1, tm2;
    hid_t ret;

    MAP_OR_FAIL(H5Dopen2);

    tm1 = darshan_core_wtime();
    ret = __real_H5Dopen2(loc_id, name, dapl_id);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        /* query dataset datatype, dataspace, and creation property list */
        dtype_id = H5Dget_type(ret);
        if(dtype_id < 0)
            return(ret);
        space_id = H5Dget_space(ret);
        if(space_id < 0)
        {
            H5Tclose(dtype_id);
            return(ret);
        }
        dcpl_id = H5Dget_create_plist(ret);
        if(dcpl_id < 0)
        {   
            H5Tclose(dtype_id);
            H5Sclose(space_id);
            return(ret);
        }

        H5D_PRE_RECORD();
        H5D_RECORD_OPEN(ret, loc_id, name, dtype_id, space_id, dcpl_id, 0, tm1, tm2);
        H5D_POST_RECORD();

        H5Tclose(dtype_id);
        H5Sclose(space_id);
        H5Pclose(dcpl_id);
    }

    return(ret);
}

herr_t DARSHAN_DECL(H5Dread)(hid_t dataset_id, hid_t mem_type_id, hid_t mem_space_id,
    hid_t file_space_id, hid_t xfer_plist_id, void * buf)
{
    struct hdf5_dataset_record_ref *rec_ref;
    size_t access_size;
    size_t type_size;
    ssize_t file_sel_npoints;
    H5S_sel_type file_sel_type;
    hsize_t start_dims[H5D_MAX_NDIMS] = {0};
    hsize_t stride_dims[H5D_MAX_NDIMS] = {0};
    hsize_t count_dims[H5D_MAX_NDIMS] = {0};
    hsize_t block_dims[H5D_MAX_NDIMS] = {0};
    int64_t common_access_vals[H5D_MAX_NDIMS+H5D_MAX_NDIMS+1] = {0};
    struct darshan_common_val_counter *cvc;
    int i;
    double tm1, tm2, elapsed;
    herr_t ret;

    MAP_OR_FAIL(H5Dread);

    tm1 = darshan_core_wtime();
    ret = __real_H5Dread(dataset_id, mem_type_id, mem_space_id, file_space_id,
        xfer_plist_id, buf);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        H5D_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(hdf5_dataset_runtime->hid_hash,
            &dataset_id, sizeof(hid_t));
        if(rec_ref)
        {
            rec_ref->dataset_rec->counters[H5D_READS] += 1;
            if(rec_ref->last_io_type == DARSHAN_IO_WRITE)
                rec_ref->dataset_rec->counters[H5D_RW_SWITCHES] += 1;
            rec_ref->last_io_type = DARSHAN_IO_READ;
            if(file_space_id == H5S_ALL)
            {
                file_sel_npoints = rec_ref->dataset_rec->counters[H5D_DATASPACE_NPOINTS];
                file_sel_type = H5S_SEL_ALL;
            }
            else
            {
                file_sel_npoints = H5Sget_select_npoints(file_space_id);
                file_sel_type = H5Sget_select_type(file_space_id);
            }
#ifdef DARSHAN_HDF5_VERS_1_10_PLUS
            if(file_sel_type == H5S_SEL_ALL)
                rec_ref->dataset_rec->counters[H5D_REGULAR_HYPERSLAB_SELECTS] += 1;
            else if(file_sel_type == H5S_SEL_POINTS)
                rec_ref->dataset_rec->counters[H5D_POINT_SELECTS] += 1;
            else if (file_sel_type == H5S_SEL_HYPERSLABS)
            {
                if(H5Sis_regular_hyperslab(file_space_id))
                {
                    rec_ref->dataset_rec->counters[H5D_REGULAR_HYPERSLAB_SELECTS] += 1;
                    H5Sget_regular_hyperslab(file_space_id,
                        start_dims, stride_dims, count_dims, block_dims);
                    for(i = 0; i < H5D_MAX_NDIMS; i++)
                    {
                        common_access_vals[1+i] = count_dims[H5D_MAX_NDIMS - i - 1] *
                            block_dims[H5D_MAX_NDIMS - i - 1];
                        common_access_vals[1+i+H5D_MAX_NDIMS] =
                            stride_dims[H5D_MAX_NDIMS - i - 1];
                    }
                }
                else
                    rec_ref->dataset_rec->counters[H5D_IRREGULAR_HYPERSLAB_SELECTS] += 1;
            }
#else
            rec_ref->dataset_rec->counters[H5D_POINT_SELECTS] = -1;
            rec_ref->dataset_rec->counters[H5D_REGULAR_HYPERSLAB_SELECTS] = -1;
            rec_ref->dataset_rec->counters[H5D_IRREGULAR_HYPERSLAB_SELECTS] = -1;
            for(i = 0; i < H5D_MAX_NDIMS; i++)
            {
                common_access_vals[1+i] = -1;
                common_access_vals[1+i+H5D_MAX_NDIMS] = -1;
            }
#endif
            type_size = rec_ref->dataset_rec->counters[H5D_DATATYPE_SIZE];
            access_size = file_sel_npoints * type_size;
            rec_ref->dataset_rec->counters[H5D_BYTES_READ] += access_size;
            DARSHAN_BUCKET_INC(
                &(rec_ref->dataset_rec->counters[H5D_SIZE_READ_AGG_0_100]), access_size);
            common_access_vals[0] = access_size;
            cvc = darshan_track_common_val_counters(&rec_ref->access_root,
                common_access_vals, H5D_MAX_NDIMS+H5D_MAX_NDIMS+1, &rec_ref->access_count);
            if(cvc) DARSHAN_UPDATE_COMMON_VAL_COUNTERS(
                &(rec_ref->dataset_rec->counters[H5D_ACCESS1_ACCESS]),
                &(rec_ref->dataset_rec->counters[H5D_ACCESS1_COUNT]),
                cvc->vals, cvc->nvals, cvc->freq, 0);
#ifdef DARSHAN_HDF5_PAR_BUILD
            if(xfer_plist_id != H5P_DEFAULT)
            {
                herr_t tmp_ret;
                H5FD_mpio_xfer_t xfer_mode;
                tmp_ret = H5Pget_dxpl_mpio(xfer_plist_id, &xfer_mode);
                if(tmp_ret >= 0 && xfer_mode == H5FD_MPIO_COLLECTIVE)
                    rec_ref->dataset_rec->counters[H5D_USE_MPIIO_COLLECTIVE] = 1;
            }
#endif
            if(rec_ref->dataset_rec->fcounters[H5D_F_READ_START_TIMESTAMP] == 0 ||
             rec_ref->dataset_rec->fcounters[H5D_F_READ_START_TIMESTAMP] > tm1)
                rec_ref->dataset_rec->fcounters[H5D_F_READ_START_TIMESTAMP] = tm1;
            rec_ref->dataset_rec->fcounters[H5D_F_READ_END_TIMESTAMP] = tm2;
            elapsed = tm2 - tm1;
            if(rec_ref->dataset_rec->fcounters[H5D_F_MAX_READ_TIME] < elapsed)
            {
                rec_ref->dataset_rec->fcounters[H5D_F_MAX_READ_TIME] = elapsed;
                rec_ref->dataset_rec->counters[H5D_MAX_READ_TIME_SIZE] = access_size;
            }
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->dataset_rec->fcounters[H5D_F_READ_TIME],
                tm1, tm2, rec_ref->last_read_end);
        }
        H5D_POST_RECORD();
    }

    return(ret);
}

herr_t DARSHAN_DECL(H5Dwrite)(hid_t dataset_id, hid_t mem_type_id, hid_t mem_space_id,
    hid_t file_space_id, hid_t xfer_plist_id, const void * buf)
{
    struct hdf5_dataset_record_ref *rec_ref;
    size_t access_size;
    size_t type_size;
    ssize_t file_sel_npoints;
    H5S_sel_type file_sel_type;
    hsize_t start_dims[H5D_MAX_NDIMS] = {0};
    hsize_t stride_dims[H5D_MAX_NDIMS] = {0};
    hsize_t count_dims[H5D_MAX_NDIMS] = {0};
    hsize_t block_dims[H5D_MAX_NDIMS] = {0};
    int64_t common_access_vals[H5D_MAX_NDIMS+H5D_MAX_NDIMS+1] = {0};
    struct darshan_common_val_counter *cvc;
    int i;
    double tm1, tm2, elapsed;
    herr_t ret;

    MAP_OR_FAIL(H5Dwrite);

    tm1 = darshan_core_wtime();
    ret = __real_H5Dwrite(dataset_id, mem_type_id, mem_space_id, file_space_id,
        xfer_plist_id, buf);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        H5D_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(hdf5_dataset_runtime->hid_hash,
            &dataset_id, sizeof(hid_t));
        if(rec_ref)
        {
            rec_ref->dataset_rec->counters[H5D_WRITES] += 1;
            if(rec_ref->last_io_type == DARSHAN_IO_READ)
                rec_ref->dataset_rec->counters[H5D_RW_SWITCHES] += 1;
            rec_ref->last_io_type = DARSHAN_IO_WRITE;
            if(file_space_id == H5S_ALL)
            {
                file_sel_npoints = rec_ref->dataset_rec->counters[H5D_DATASPACE_NPOINTS];
                file_sel_type = H5S_SEL_ALL;
            }
            else
            {
                file_sel_npoints = H5Sget_select_npoints(file_space_id);
                file_sel_type = H5Sget_select_type(file_space_id);
            }
#ifdef DARSHAN_HDF5_VERS_1_10_PLUS
            if(file_sel_type == H5S_SEL_ALL)
                rec_ref->dataset_rec->counters[H5D_REGULAR_HYPERSLAB_SELECTS] += 1;
            else if(file_sel_type == H5S_SEL_POINTS)
                rec_ref->dataset_rec->counters[H5D_POINT_SELECTS] += 1;
            else if (file_sel_type == H5S_SEL_HYPERSLABS)
            {
                if(H5Sis_regular_hyperslab(file_space_id))
                {
                    rec_ref->dataset_rec->counters[H5D_REGULAR_HYPERSLAB_SELECTS] += 1;
                    H5Sget_regular_hyperslab(file_space_id,
                        start_dims, stride_dims, count_dims, block_dims);
                    for(i = 0; i < H5D_MAX_NDIMS; i++)
                    {
                        common_access_vals[1+i] = count_dims[H5D_MAX_NDIMS - i - 1] *
                            block_dims[H5D_MAX_NDIMS - i - 1];
                        common_access_vals[1+i+H5D_MAX_NDIMS] =
                            stride_dims[H5D_MAX_NDIMS - i - 1];
                    }
                }
                else
                    rec_ref->dataset_rec->counters[H5D_IRREGULAR_HYPERSLAB_SELECTS] += 1;
            }
#else
            rec_ref->dataset_rec->counters[H5D_POINT_SELECTS] = -1;
            rec_ref->dataset_rec->counters[H5D_REGULAR_HYPERSLAB_SELECTS] = -1;
            rec_ref->dataset_rec->counters[H5D_IRREGULAR_HYPERSLAB_SELECTS] = -1;
            for(i = 0; i < H5D_MAX_NDIMS; i++)
            {
                common_access_vals[1+i] = -1;
                common_access_vals[1+i+H5D_MAX_NDIMS] = -1;
            }
#endif
            type_size = rec_ref->dataset_rec->counters[H5D_DATATYPE_SIZE];
            access_size = file_sel_npoints * type_size;
            rec_ref->dataset_rec->counters[H5D_BYTES_WRITTEN] += access_size;
            DARSHAN_BUCKET_INC(
                &(rec_ref->dataset_rec->counters[H5D_SIZE_WRITE_AGG_0_100]), access_size);
            common_access_vals[0] = access_size;
            cvc = darshan_track_common_val_counters(&rec_ref->access_root,
                common_access_vals, H5D_MAX_NDIMS+H5D_MAX_NDIMS+1, &rec_ref->access_count);
            if(cvc) DARSHAN_UPDATE_COMMON_VAL_COUNTERS(
                &(rec_ref->dataset_rec->counters[H5D_ACCESS1_ACCESS]),
                &(rec_ref->dataset_rec->counters[H5D_ACCESS1_COUNT]),
                cvc->vals, cvc->nvals, cvc->freq, 0);
#ifdef DARSHAN_HDF5_PAR_BUILD
            if(xfer_plist_id != H5P_DEFAULT)
            {
                herr_t tmp_ret;
                H5FD_mpio_xfer_t xfer_mode;
                tmp_ret = H5Pget_dxpl_mpio(xfer_plist_id, &xfer_mode);
                if(tmp_ret >= 0 && xfer_mode == H5FD_MPIO_COLLECTIVE)
                    rec_ref->dataset_rec->counters[H5D_USE_MPIIO_COLLECTIVE] = 1;
            }
#endif
            if(rec_ref->dataset_rec->fcounters[H5D_F_WRITE_START_TIMESTAMP] == 0 ||
             rec_ref->dataset_rec->fcounters[H5D_F_WRITE_START_TIMESTAMP] > tm1)
                rec_ref->dataset_rec->fcounters[H5D_F_WRITE_START_TIMESTAMP] = tm1;
            rec_ref->dataset_rec->fcounters[H5D_F_WRITE_END_TIMESTAMP] = tm2;
            elapsed = tm2 - tm1;
            if(rec_ref->dataset_rec->fcounters[H5D_F_MAX_WRITE_TIME] < elapsed)
            {
                rec_ref->dataset_rec->fcounters[H5D_F_MAX_WRITE_TIME] = elapsed;
                rec_ref->dataset_rec->counters[H5D_MAX_WRITE_TIME_SIZE] = access_size;
            }
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->dataset_rec->fcounters[H5D_F_WRITE_TIME],
                tm1, tm2, rec_ref->last_write_end);
        }
        H5D_POST_RECORD();
    }

    return(ret);
}

#ifdef DARSHAN_HDF5_VERS_1_10_PLUS
herr_t DARSHAN_DECL(H5Dflush)(hid_t dataset_id)
{
    struct hdf5_dataset_record_ref *rec_ref;
    double tm1, tm2;
    herr_t ret;

    MAP_OR_FAIL(H5Dflush);

    tm1 = darshan_core_wtime();
    ret = __real_H5Dflush(dataset_id);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        H5D_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(hdf5_dataset_runtime->hid_hash,
            &dataset_id, sizeof(hid_t));
        if(rec_ref)
        {
            rec_ref->dataset_rec->counters[H5D_FLUSHES] += 1;
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->dataset_rec->fcounters[H5D_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
        }
        H5D_POST_RECORD();
    }

    return(ret);
}
#endif

herr_t DARSHAN_DECL(H5Dclose)(hid_t dataset_id)
{
    struct hdf5_dataset_record_ref *rec_ref;
    double tm1, tm2;
    herr_t ret;

    MAP_OR_FAIL(H5Dclose);

    tm1 = darshan_core_wtime();
    ret = __real_H5Dclose(dataset_id);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        H5D_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(hdf5_dataset_runtime->hid_hash,
            &dataset_id, sizeof(hid_t));
        if(rec_ref)
        {
            if(rec_ref->dataset_rec->fcounters[H5D_F_CLOSE_START_TIMESTAMP] == 0 ||
             rec_ref->dataset_rec->fcounters[H5D_F_CLOSE_START_TIMESTAMP] > tm1)
               rec_ref->dataset_rec->fcounters[H5D_F_CLOSE_START_TIMESTAMP] = tm1;
            rec_ref->dataset_rec->fcounters[H5D_F_CLOSE_END_TIMESTAMP] = tm2;
            DARSHAN_TIMER_INC_NO_OVERLAP(rec_ref->dataset_rec->fcounters[H5D_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            darshan_delete_record_ref(&(hdf5_dataset_runtime->hid_hash), &dataset_id, sizeof(hid_t));
        }
        H5D_POST_RECORD();
    }

    return(ret);
}

/*********************************************************
 * Internal functions for manipulating HDF5 module state *
 *********************************************************/

/* initialize internal HDF5 module data strucutres and register with darshan-core */
static void hdf5_file_runtime_initialize()
{
    size_t hdf5_buf_size;
    darshan_module_funcs mod_funcs = {
#ifdef HAVE_MPI
    .mod_redux_func = &hdf5_file_mpi_redux,
#endif
    .mod_output_func = &hdf5_file_output,
    .mod_cleanup_func = &hdf5_file_cleanup
    };

    /* try and store the default number of records for this module */
    hdf5_buf_size = DARSHAN_DEF_MOD_REC_COUNT * sizeof(struct darshan_hdf5_file);

    /* register hdf5 module with darshan-core */
    darshan_core_register_module(
        DARSHAN_H5F_MOD,
        mod_funcs,
        &hdf5_buf_size,
        &my_rank,
        NULL);

    hdf5_file_runtime = malloc(sizeof(*hdf5_file_runtime));
    if(!hdf5_file_runtime)
    {
        darshan_core_unregister_module(DARSHAN_H5F_MOD);
        return;
    }
    memset(hdf5_file_runtime, 0, sizeof(*hdf5_file_runtime));

    return;
}

static void hdf5_dataset_runtime_initialize()
{
    size_t hdf5_buf_size;
    darshan_module_funcs mod_funcs = {
#ifdef HAVE_MPI
    .mod_redux_func = &hdf5_dataset_mpi_redux,
#endif
    .mod_output_func = &hdf5_dataset_output,
    .mod_cleanup_func = &hdf5_dataset_cleanup
    };

    /* try and store the default number of records for this module */
    hdf5_buf_size = DARSHAN_DEF_MOD_REC_COUNT * sizeof(struct darshan_hdf5_dataset);

    /* register hdf5 module with darshan-core */
    darshan_core_register_module(
        DARSHAN_H5D_MOD,
        mod_funcs,
        &hdf5_buf_size,
        &my_rank,
        NULL);

    hdf5_dataset_runtime = malloc(sizeof(*hdf5_dataset_runtime));
    if(!hdf5_dataset_runtime)
    {
        darshan_core_unregister_module(DARSHAN_H5D_MOD);
        return;
    }
    memset(hdf5_dataset_runtime, 0, sizeof(*hdf5_dataset_runtime));

    return;
}

static struct hdf5_file_record_ref *hdf5_track_new_file_record(
    darshan_record_id rec_id, const char *path)
{
    struct darshan_hdf5_file *file_rec = NULL;
    struct hdf5_file_record_ref *rec_ref = NULL;
    int ret;

    rec_ref = malloc(sizeof(*rec_ref));
    if(!rec_ref)
        return(NULL);
    memset(rec_ref, 0, sizeof(*rec_ref));

    /* add a reference to this file record based on record id */
    ret = darshan_add_record_ref(&(hdf5_file_runtime->rec_id_hash), &rec_id,
        sizeof(darshan_record_id), rec_ref);
    if(ret == 0)
    {
        free(rec_ref);
        return(NULL);
    }

    /* register the actual file record with darshan-core so it is persisted
     * in the log file
     */
    file_rec = darshan_core_register_record(
        rec_id,
        path,
        DARSHAN_H5F_MOD,
        sizeof(struct darshan_hdf5_file),
        NULL);

    if(!file_rec)
    {
        darshan_delete_record_ref(&(hdf5_file_runtime->rec_id_hash),
            &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        return(NULL);
    }

    /* registering this dataset record was successful, so initialize some fields */
    file_rec->base_rec.id = rec_id;
    file_rec->base_rec.rank = my_rank;
    rec_ref->file_rec = file_rec;
    hdf5_file_runtime->rec_count++;

    return(rec_ref);
}

static struct hdf5_dataset_record_ref *hdf5_track_new_dataset_record(
    darshan_record_id rec_id, const char *path)
{
    struct darshan_hdf5_dataset *dataset_rec = NULL;
    struct hdf5_dataset_record_ref *rec_ref = NULL;
    int ret;

    rec_ref = malloc(sizeof(*rec_ref));
    if(!rec_ref)
        return(NULL);
    memset(rec_ref, 0, sizeof(*rec_ref));

    /* add a reference to this dataset record based on record id */
    ret = darshan_add_record_ref(&(hdf5_dataset_runtime->rec_id_hash), &rec_id,
        sizeof(darshan_record_id), rec_ref);
    if(ret == 0)
    {
        free(rec_ref);
        return(NULL);
    }

    /* register the actual dataset record with darshan-core so it is persisted
     * in the log file
     */
    dataset_rec = darshan_core_register_record(
        rec_id,
        path,
        DARSHAN_H5D_MOD,
        sizeof(struct darshan_hdf5_dataset),
        NULL);

    if(!dataset_rec)
    {
        darshan_delete_record_ref(&(hdf5_dataset_runtime->rec_id_hash),
            &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        return(NULL);
    }

    /* registering this dataset record was successful, so initialize some fields */
    dataset_rec->base_rec.id = rec_id;
    dataset_rec->base_rec.rank = my_rank;
    rec_ref->dataset_rec = dataset_rec;
    hdf5_dataset_runtime->rec_count++;

#ifndef DARSHAN_HDF5_VERS_1_10_PLUS
    /* flushes weren't introduced until H5 version 1.10+ */
    rec_ref->dataset_rec->counters[H5D_FLUSHES] = -1;
#endif

    return(rec_ref);
}

static void hdf5_finalize_dataset_records(void *rec_ref_p, void *user_ptr)
{
    struct hdf5_dataset_record_ref *rec_ref =
        (struct hdf5_dataset_record_ref *)rec_ref_p;

    tdestroy(rec_ref->access_root, free);
    return;
}

#ifdef HAVE_MPI
static void hdf5_file_record_reduction_op(void* inrec_v, void* inoutrec_v,
    int *len, MPI_Datatype *datatype)
{
    struct darshan_hdf5_file tmp_file;
    struct darshan_hdf5_file *inrec = inrec_v;
    struct darshan_hdf5_file *inoutrec = inoutrec_v;
    int i, j;

    for(i=0; i<*len; i++)
    {
        memset(&tmp_file, 0, sizeof(struct darshan_hdf5_file));
        tmp_file.base_rec.id = inrec->base_rec.id;
        tmp_file.base_rec.rank = -1;

        /* sum */
        for(j=H5F_OPENS; j<=H5F_FLUSHES; j++)
        {
            tmp_file.counters[j] = inrec->counters[j] + inoutrec->counters[j];
        }

        if(inoutrec->counters[H5F_USE_MPIIO] == 1 || inrec->counters[H5F_USE_MPIIO] == 1)
            tmp_file.counters[H5F_USE_MPIIO] = 1;

        /* min non-zero (if available) value */
        for(j=H5F_F_OPEN_START_TIMESTAMP; j<=H5F_F_CLOSE_START_TIMESTAMP; j++)
        {
            if((inrec->fcounters[j] < inoutrec->fcounters[j] &&
               inrec->fcounters[j] > 0) || inoutrec->fcounters[j] == 0) 
                tmp_file.fcounters[j] = inrec->fcounters[j];
            else
                tmp_file.fcounters[j] = inoutrec->fcounters[j];
        }

        /* max */
        for(j=H5F_F_OPEN_END_TIMESTAMP; j<=H5F_F_CLOSE_END_TIMESTAMP; j++)
        {
            if(inrec->fcounters[j] > inoutrec->fcounters[j])
                tmp_file.fcounters[j] = inrec->fcounters[j];
            else
                tmp_file.fcounters[j] = inoutrec->fcounters[j];
        }

        /* sum */
        tmp_file.fcounters[H5F_F_META_TIME] =
            inrec->fcounters[H5F_F_META_TIME] + inoutrec->fcounters[H5F_F_META_TIME];

        /* update pointers */
        *inoutrec = tmp_file;
        inoutrec++;
        inrec++;
    }

    return;
}

static void hdf5_dataset_record_reduction_op(void* inrec_v, void* inoutrec_v,
    int *len, MPI_Datatype *datatype)
{
    struct darshan_hdf5_dataset tmp_dataset;
    struct darshan_hdf5_dataset *inrec = inrec_v;
    struct darshan_hdf5_dataset *inoutrec = inoutrec_v;
    int i, j, j2, k, k2;

    for(i=0; i<*len; i++)
    {
        memset(&tmp_dataset, 0, sizeof(struct darshan_hdf5_dataset));
        tmp_dataset.base_rec.id = inrec->base_rec.id;
        tmp_dataset.base_rec.rank = -1;
        tmp_dataset.file_rec_id = inrec->file_rec_id;

        /* sum */
        for(j=H5D_OPENS; j<=H5D_POINT_SELECTS; j++)
        {
            tmp_dataset.counters[j] = inrec->counters[j] + inoutrec->counters[j];
        }

        /* skip H5D_MAX_*_TIME_SIZE; handled in floating point section */

        for(j=H5D_SIZE_READ_AGG_0_100; j<=H5D_SIZE_WRITE_AGG_1G_PLUS; j++)
        {
            tmp_dataset.counters[j] = inrec->counters[j] + inoutrec->counters[j];
        }

        /* first collapse any duplicates */
        for(j=H5D_ACCESS1_ACCESS, j2=H5D_ACCESS1_COUNT; j<=H5D_ACCESS4_ACCESS;
            j+=(H5D_MAX_NDIMS+H5D_MAX_NDIMS+1), j2++)
        {
            for(k=H5D_ACCESS1_ACCESS, k2=H5D_ACCESS1_COUNT; k<=H5D_ACCESS4_ACCESS;
                k+=(H5D_MAX_NDIMS+H5D_MAX_NDIMS+1), k2++)
            {
                if(!memcmp(&inrec->counters[j], &inoutrec->counters[k],
                    sizeof(int64_t) * (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1)))
                {
                    memset(&inoutrec->counters[k], 0, sizeof(int64_t) *
                        (H5D_MAX_NDIMS+H5D_MAX_NDIMS+1));
                    inrec->counters[j2] += inoutrec->counters[k2];
                    inoutrec->counters[k2] = 0;
                }
            }
        }

        /* first set */
        for(j=H5D_ACCESS1_ACCESS, j2=H5D_ACCESS1_COUNT; j<=H5D_ACCESS4_ACCESS;
            j+=(H5D_MAX_NDIMS+H5D_MAX_NDIMS+1), j2++)
        {
            DARSHAN_UPDATE_COMMON_VAL_COUNTERS(
                &(tmp_dataset.counters[H5D_ACCESS1_ACCESS]),
                &(tmp_dataset.counters[H5D_ACCESS1_COUNT]),
                &inrec->counters[j], H5D_MAX_NDIMS+H5D_MAX_NDIMS+1,
                inrec->counters[j2], 0);
        }

        /* second set */
        for(j=H5D_ACCESS1_ACCESS, j2=H5D_ACCESS1_COUNT; j<=H5D_ACCESS4_ACCESS;
            j+=(H5D_MAX_NDIMS+H5D_MAX_NDIMS+1), j2++)
        {
            DARSHAN_UPDATE_COMMON_VAL_COUNTERS(
                &(tmp_dataset.counters[H5D_ACCESS1_ACCESS]),
                &(tmp_dataset.counters[H5D_ACCESS1_COUNT]),
                &inoutrec->counters[j], H5D_MAX_NDIMS+H5D_MAX_NDIMS+1,
                inoutrec->counters[j2], 0);
        }

        tmp_dataset.counters[H5D_DATASPACE_NDIMS] = inrec->counters[H5D_DATASPACE_NDIMS];
        tmp_dataset.counters[H5D_DATASPACE_NPOINTS] = inrec->counters[H5D_DATASPACE_NPOINTS];
        tmp_dataset.counters[H5D_DATATYPE_SIZE] = inrec->counters[H5D_DATATYPE_SIZE];

        for(j=H5D_CHUNK_SIZE_D1; j<=H5D_CHUNK_SIZE_D5; j++)
            tmp_dataset.counters[j] = inrec->counters[j];

        if(inoutrec->counters[H5D_USE_MPIIO_COLLECTIVE] == 1 ||
                inrec->counters[H5D_USE_MPIIO_COLLECTIVE] == 1)
            tmp_dataset.counters[H5D_USE_MPIIO_COLLECTIVE] = 1;

        if(inoutrec->counters[H5D_USE_DEPRECATED] == 1 ||
                inrec->counters[H5D_USE_DEPRECATED] == 1)
            tmp_dataset.counters[H5D_USE_DEPRECATED] = 1;

        /* min non-zero (if available) value */
        for(j=H5D_F_OPEN_START_TIMESTAMP; j<=H5D_F_CLOSE_START_TIMESTAMP; j++)
        {
            if((inrec->fcounters[j] < inoutrec->fcounters[j] &&
               inrec->fcounters[j] > 0) || inoutrec->fcounters[j] == 0) 
                tmp_dataset.fcounters[j] = inrec->fcounters[j];
            else
                tmp_dataset.fcounters[j] = inoutrec->fcounters[j];
        }

        /* max */
        for(j=H5D_F_OPEN_END_TIMESTAMP; j<=H5D_F_CLOSE_END_TIMESTAMP; j++)
        {
            if(inrec->fcounters[j] > inoutrec->fcounters[j])
                tmp_dataset.fcounters[j] = inrec->fcounters[j];
            else
                tmp_dataset.fcounters[j] = inoutrec->fcounters[j];
        }

        /* sum */
        for(j=H5D_F_READ_TIME; j<=H5D_F_META_TIME; j++)
        {
            tmp_dataset.fcounters[j] = inrec->fcounters[j] + inoutrec->fcounters[j];
        }

        /* max (special case) */
        if(inrec->fcounters[H5D_F_MAX_READ_TIME] >
            inoutrec->fcounters[H5D_F_MAX_READ_TIME])
        {
            tmp_dataset.fcounters[H5D_F_MAX_READ_TIME] =
                inrec->fcounters[H5D_F_MAX_READ_TIME];
            tmp_dataset.counters[H5D_MAX_READ_TIME_SIZE] =
                inrec->counters[H5D_MAX_READ_TIME_SIZE];
        }
        else
        {
            tmp_dataset.fcounters[H5D_F_MAX_READ_TIME] =
                inoutrec->fcounters[H5D_F_MAX_READ_TIME];
            tmp_dataset.counters[H5D_MAX_READ_TIME_SIZE] =
                inoutrec->counters[H5D_MAX_READ_TIME_SIZE];
        }

        /* max (special case) */
        if(inrec->fcounters[H5D_F_MAX_WRITE_TIME] >
            inoutrec->fcounters[H5D_F_MAX_WRITE_TIME])
        {
            tmp_dataset.fcounters[H5D_F_MAX_WRITE_TIME] =
                inrec->fcounters[H5D_F_MAX_WRITE_TIME];
            tmp_dataset.counters[H5D_MAX_WRITE_TIME_SIZE] =
                inrec->counters[H5D_MAX_WRITE_TIME_SIZE];
        }
        else
        {
            tmp_dataset.fcounters[H5D_F_MAX_WRITE_TIME] =
                inoutrec->fcounters[H5D_F_MAX_WRITE_TIME];
            tmp_dataset.counters[H5D_MAX_WRITE_TIME_SIZE] =
                inoutrec->counters[H5D_MAX_WRITE_TIME_SIZE];
        }

        /* min (zeroes are ok here; some procs don't do I/O) */
        if(inrec->fcounters[H5D_F_FASTEST_RANK_TIME] <
            inoutrec->fcounters[H5D_F_FASTEST_RANK_TIME])
        {
            tmp_dataset.counters[H5D_FASTEST_RANK] =
                inrec->counters[H5D_FASTEST_RANK];
            tmp_dataset.counters[H5D_FASTEST_RANK_BYTES] =
                inrec->counters[H5D_FASTEST_RANK_BYTES];
            tmp_dataset.fcounters[H5D_F_FASTEST_RANK_TIME] =
                inrec->fcounters[H5D_F_FASTEST_RANK_TIME];
        }
        else
        {
            tmp_dataset.counters[H5D_FASTEST_RANK] =
                inoutrec->counters[H5D_FASTEST_RANK];
            tmp_dataset.counters[H5D_FASTEST_RANK_BYTES] =
                inoutrec->counters[H5D_FASTEST_RANK_BYTES];
            tmp_dataset.fcounters[H5D_F_FASTEST_RANK_TIME] =
                inoutrec->fcounters[H5D_F_FASTEST_RANK_TIME];
        }

        /* max */
        if(inrec->fcounters[H5D_F_SLOWEST_RANK_TIME] >
           inoutrec->fcounters[H5D_F_SLOWEST_RANK_TIME])
        {
            tmp_dataset.counters[H5D_SLOWEST_RANK] =
                inrec->counters[H5D_SLOWEST_RANK];
            tmp_dataset.counters[H5D_SLOWEST_RANK_BYTES] =
                inrec->counters[H5D_SLOWEST_RANK_BYTES];
            tmp_dataset.fcounters[H5D_F_SLOWEST_RANK_TIME] =
                inrec->fcounters[H5D_F_SLOWEST_RANK_TIME];
        }
        else
        {
            tmp_dataset.counters[H5D_SLOWEST_RANK] =
                inoutrec->counters[H5D_SLOWEST_RANK];
            tmp_dataset.counters[H5D_SLOWEST_RANK_BYTES] =
                inoutrec->counters[H5D_SLOWEST_RANK_BYTES];
            tmp_dataset.fcounters[H5D_F_SLOWEST_RANK_TIME] =
                inoutrec->fcounters[H5D_F_SLOWEST_RANK_TIME];
        }

        /* update pointers */
        *inoutrec = tmp_dataset;
        inoutrec++;
        inrec++;
    }

    return;
}

static void hdf5_shared_dataset_record_variance(
    MPI_Comm mod_comm, struct darshan_hdf5_dataset *inrec_array,
    struct darshan_hdf5_dataset *outrec_array, int shared_rec_count)
{
    MPI_Datatype var_dt;
    MPI_Op var_op;
    int i;
    struct darshan_variance_dt *var_send_buf = NULL;
    struct darshan_variance_dt *var_recv_buf = NULL;

    PMPI_Type_contiguous(sizeof(struct darshan_variance_dt),
        MPI_BYTE, &var_dt);
    PMPI_Type_commit(&var_dt);

    PMPI_Op_create(darshan_variance_reduce, 1, &var_op);

    var_send_buf = malloc(shared_rec_count * sizeof(struct darshan_variance_dt));
    if(!var_send_buf)
        return;

    if(my_rank == 0)
    {
        var_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_variance_dt));

        if(!var_recv_buf)
            return;
    }

    /* get total i/o time variances for shared records */

    /* get total i/o time variances for shared records */

    for(i=0; i<shared_rec_count; i++)
    {
        var_send_buf[i].n = 1;
        var_send_buf[i].S = 0;
        var_send_buf[i].T = inrec_array[i].fcounters[H5D_F_READ_TIME] +
                            inrec_array[i].fcounters[H5D_F_WRITE_TIME] +
                            inrec_array[i].fcounters[H5D_F_META_TIME];
    }

    PMPI_Reduce(var_send_buf, var_recv_buf, shared_rec_count,
        var_dt, var_op, 0, mod_comm);

    if(my_rank == 0)
    {
        for(i=0; i<shared_rec_count; i++)
        {
            outrec_array[i].fcounters[H5D_F_VARIANCE_RANK_TIME] =
                (var_recv_buf[i].S / var_recv_buf[i].n);
        }
    }

    /* get total bytes moved variances for shared records */

    for(i=0; i<shared_rec_count; i++)
    {
        var_send_buf[i].n = 1;
        var_send_buf[i].S = 0;
        var_send_buf[i].T = (double)
                            inrec_array[i].counters[H5D_BYTES_READ] +
                            inrec_array[i].counters[H5D_BYTES_WRITTEN];
    }

    PMPI_Reduce(var_send_buf, var_recv_buf, shared_rec_count,
        var_dt, var_op, 0, mod_comm);

    if(my_rank == 0)
    {
        for(i=0; i<shared_rec_count; i++)
        {
            outrec_array[i].fcounters[H5D_F_VARIANCE_RANK_BYTES] =
                (var_recv_buf[i].S / var_recv_buf[i].n);
        }
    }

    PMPI_Type_free(&var_dt);
    PMPI_Op_free(&var_op);
    free(var_send_buf);
    free(var_recv_buf);

    return;
}
#endif

/************************************************************************
 * Functions exported by HDF5 module for coordinating with darshan-core *
 ************************************************************************/

#ifdef HAVE_MPI
static void hdf5_file_mpi_redux(
    void *hdf5_buf,
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count)
{
    int rec_count;
    struct hdf5_file_record_ref *rec_ref;
    struct darshan_hdf5_file *hdf5_rec_buf = (struct darshan_hdf5_file *)hdf5_buf;
    struct darshan_hdf5_file *red_send_buf = NULL;
    struct darshan_hdf5_file *red_recv_buf = NULL;
    MPI_Datatype red_type;
    MPI_Op red_op;
    int i;

    HDF5_LOCK();
    assert(hdf5_file_runtime);

    rec_count = hdf5_file_runtime->rec_count;

    /* necessary initialization of shared records */
    for(i = 0; i < shared_rec_count; i++)
    {
        rec_ref = darshan_lookup_record_ref(hdf5_file_runtime->rec_id_hash,
            &shared_recs[i], sizeof(darshan_record_id));
        assert(rec_ref);

        rec_ref->file_rec->base_rec.rank = -1;
    }

    /* sort the array of records so we get all of the shared records
     * (marked by rank -1) in a contiguous portion at end of the array
     */
    darshan_record_sort(hdf5_rec_buf, rec_count,
        sizeof(struct darshan_hdf5_file));

    /* make *send_buf point to the shared records at the end of sorted array */
    red_send_buf = &(hdf5_rec_buf[rec_count-shared_rec_count]);

    /* allocate memory for the reduction output on rank 0 */
    if(my_rank == 0)
    {
        red_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_hdf5_file));
        if(!red_recv_buf)
        {
            HDF5_UNLOCK();
            return;
        }
    }

    /* construct a datatype for a HDF5 dataset record.  This is serving no purpose
     * except to make sure we can do a reduction on proper boundaries
     */
    PMPI_Type_contiguous(sizeof(struct darshan_hdf5_file), MPI_BYTE, &red_type);
    PMPI_Type_commit(&red_type);

    /* register a HDF5 dataset record reduction operator */
    PMPI_Op_create(hdf5_file_record_reduction_op, 1, &red_op);

    /* reduce shared HDF5 dataset records */
    PMPI_Reduce(red_send_buf, red_recv_buf,
        shared_rec_count, red_type, red_op, 0, mod_comm);

    /* update module state to account for shared file reduction */
    if(my_rank == 0)
    {
        /* overwrite local shared records with globally reduced records */
        int tmp_ndx = rec_count - shared_rec_count;
        memcpy(&(hdf5_rec_buf[tmp_ndx]), red_recv_buf,
            shared_rec_count * sizeof(struct darshan_hdf5_file));
        free(red_recv_buf);
    }
    else
    {
        /* drop shared records on non-zero ranks */
        hdf5_file_runtime->rec_count -= shared_rec_count;
    }

    PMPI_Type_free(&red_type);
    PMPI_Op_free(&red_op);

    HDF5_UNLOCK();
    return;
}

static void hdf5_dataset_mpi_redux(
    void *hdf5_buf,
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count)
{
    int rec_count;
    struct hdf5_dataset_record_ref *rec_ref;
    struct darshan_hdf5_dataset *hdf5_rec_buf = (struct darshan_hdf5_dataset *)hdf5_buf;
    double hdf5_time;
    struct darshan_hdf5_dataset *red_send_buf = NULL;
    struct darshan_hdf5_dataset *red_recv_buf = NULL;
    MPI_Datatype red_type;
    MPI_Op red_op;
    int i;

    HDF5_LOCK();
    assert(hdf5_dataset_runtime);

    rec_count = hdf5_dataset_runtime->rec_count;

    /* necessary initialization of shared records */
    for(i = 0; i < shared_rec_count; i++)
    {
        rec_ref = darshan_lookup_record_ref(hdf5_dataset_runtime->rec_id_hash,
            &shared_recs[i], sizeof(darshan_record_id));
        assert(rec_ref);

        hdf5_time = 
            rec_ref->dataset_rec->fcounters[H5D_F_READ_TIME] +
            rec_ref->dataset_rec->fcounters[H5D_F_WRITE_TIME] +
            rec_ref->dataset_rec->fcounters[H5D_F_META_TIME];

        /* until reduction occurs, we assume that this rank is both
         * the fastest and slowest. It is up to the reduction operator
         * to find the true min and max.
         */
        rec_ref->dataset_rec->counters[H5D_FASTEST_RANK] =
            rec_ref->dataset_rec->base_rec.rank;
        rec_ref->dataset_rec->counters[H5D_FASTEST_RANK_BYTES] =
            rec_ref->dataset_rec->counters[H5D_BYTES_READ] +
            rec_ref->dataset_rec->counters[H5D_BYTES_WRITTEN];
        rec_ref->dataset_rec->fcounters[H5D_F_FASTEST_RANK_TIME] =
            hdf5_time;

        rec_ref->dataset_rec->counters[H5D_SLOWEST_RANK] =
            rec_ref->dataset_rec->counters[H5D_FASTEST_RANK];
        rec_ref->dataset_rec->counters[H5D_SLOWEST_RANK_BYTES] =
            rec_ref->dataset_rec->counters[H5D_FASTEST_RANK_BYTES];
        rec_ref->dataset_rec->fcounters[H5D_F_SLOWEST_RANK_TIME] =
            rec_ref->dataset_rec->fcounters[H5D_F_FASTEST_RANK_TIME];

        rec_ref->dataset_rec->base_rec.rank = -1;
    }

    /* sort the array of records so we get all of the shared records
     * (marked by rank -1) in a contiguous portion at end of the array
     */
    darshan_record_sort(hdf5_rec_buf, rec_count,
        sizeof(struct darshan_hdf5_dataset));

    /* make *send_buf point to the shared records at the end of sorted array */
    red_send_buf = &(hdf5_rec_buf[rec_count-shared_rec_count]);

    /* allocate memory for the reduction output on rank 0 */
    if(my_rank == 0)
    {
        red_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_hdf5_dataset));
        if(!red_recv_buf)
        {
            HDF5_UNLOCK();
            return;
        }
    }

    /* construct a datatype for a HDF5 dataset record.  This is serving no purpose
     * except to make sure we can do a reduction on proper boundaries
     */
    PMPI_Type_contiguous(sizeof(struct darshan_hdf5_dataset),
        MPI_BYTE, &red_type);
    PMPI_Type_commit(&red_type);

    /* register a HDF5 dataset record reduction operator */
    PMPI_Op_create(hdf5_dataset_record_reduction_op, 1, &red_op);

    /* reduce shared HDF5 dataset records */
    PMPI_Reduce(red_send_buf, red_recv_buf,
        shared_rec_count, red_type, red_op, 0, mod_comm);

    /* get the time and byte variances for shared files */
    hdf5_shared_dataset_record_variance(mod_comm, red_send_buf, red_recv_buf,
        shared_rec_count);

    /* update module state to account for shared file reduction */
    if(my_rank == 0)
    {
        /* overwrite local shared records with globally reduced records */
        int tmp_ndx = rec_count - shared_rec_count;
        memcpy(&(hdf5_rec_buf[tmp_ndx]), red_recv_buf,
            shared_rec_count * sizeof(struct darshan_hdf5_dataset));
        free(red_recv_buf);
    }
    else
    {
        /* drop shared records on non-zero ranks */
        hdf5_dataset_runtime->rec_count -= shared_rec_count;
    }

    PMPI_Type_free(&red_type);
    PMPI_Op_free(&red_op);

    HDF5_UNLOCK();
    return;
}
#endif

static void hdf5_file_output(
    void **hdf5_buf,
    int *hdf5_buf_sz)
{
    int rec_count;

    HDF5_LOCK();
    assert(hdf5_file_runtime);

    /* just pass back our updated total buffer size -- no need to update buffer */
    rec_count = hdf5_file_runtime->rec_count;
    *hdf5_buf_sz = rec_count * sizeof(struct darshan_hdf5_file);

    HDF5_UNLOCK();
    return;
}

static void hdf5_dataset_output(
    void **hdf5_buf,
    int *hdf5_buf_sz)
{
    int rec_count;

    HDF5_LOCK();
    assert(hdf5_dataset_runtime);

    /* just pass back our updated total buffer size -- no need to update buffer */
    rec_count = hdf5_dataset_runtime->rec_count;
    *hdf5_buf_sz = rec_count * sizeof(struct darshan_hdf5_dataset);

    HDF5_UNLOCK();
    return;
}

static void hdf5_file_cleanup()
{
    HDF5_LOCK();
    assert(hdf5_file_runtime);

    /* cleanup internal structures used for instrumenting */
    darshan_clear_record_refs(&(hdf5_file_runtime->hid_hash), 0);
    darshan_clear_record_refs(&(hdf5_file_runtime->rec_id_hash), 1);

    free(hdf5_file_runtime);
    hdf5_file_runtime = NULL;

    HDF5_UNLOCK();
    return;
}

static void hdf5_dataset_cleanup()
{
    HDF5_LOCK();
    assert(hdf5_dataset_runtime);

    /* cleanup internal structures used for instrumenting */
    darshan_iter_record_refs(hdf5_dataset_runtime->rec_id_hash,
        &hdf5_finalize_dataset_records, NULL);
    darshan_clear_record_refs(&(hdf5_dataset_runtime->hid_hash), 0);
    darshan_clear_record_refs(&(hdf5_dataset_runtime->rec_id_hash), 1);

    free(hdf5_dataset_runtime);
    hdf5_dataset_runtime = NULL;

    HDF5_UNLOCK();
    return;
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
