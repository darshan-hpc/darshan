/*
 * Copyright (C) 2017 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include "darshan-runtime-config.h"
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <assert.h>

#include "uthash.h"
#include "darshan.h"
#include "darshan-dynamic.h"
#include "darshan-apmpi-log-format.h"

typedef long long ap_bytes_t;
#define MAX(x,y) ((x>y)?x:y)
#define MIN(x,y) ((x==0.0)?y:((x<y)?x:y))
#define APMPI_WTIME() __darshan_disabled ? 0 : darshan_core_wtime();

#ifdef __APMPI_COLL_SYNC

#define TIME_SYNC(FUNC) \
          double tm1, tm2, tm3, tdiff, tsync;\
          int ret; \
          MAP_OR_FAIL(PMPI_Barrier);\
          tm1 = APMPI_WTIME(); \
          ret = __real_PMPI_Barrier(comm); \
          tm2 = APMPI_WTIME(); \
          ret = FUNC; \
          tm3 = APMPI_WTIME(); \
          tdiff = tm3-tm2; \
          tsync = tm2-tm1
#else

#define TIME_SYNC(FUNC) \
          double tm1, tm2, tdiff, tsync;\
          int ret; \
          tm1 = APMPI_WTIME(); \
          ret = FUNC; \
          tm2 = APMPI_WTIME(); \
          tdiff = tm2-tm1; \
          tsync = 0

#endif
#define TIME(FUNC) \
          double tm1, tm2, tdiff;\
          int ret; \
          tm1 = APMPI_WTIME(); \
          ret = FUNC; \
          tm2 = APMPI_WTIME(); \
          tdiff = tm2-tm1

#define BYTECOUNT(TYPE, COUNT) \
          int tsize; \
          ap_bytes_t bytes = 0; \
          if((COUNT > 0) && (TYPE != MPI_DATATYPE_NULL)) { \
              PMPI_Type_size(TYPE, &tsize); \
              bytes = (COUNT) * tsize; \
          }

#define BYTECOUNTND(TYPE, COUNT) \
          int tsize2; \
          bytes = 0; \
          if((COUNT > 0) && (TYPE != MPI_DATATYPE_NULL)) { \
              PMPI_Type_size(TYPE, &tsize2); \
              bytes = (COUNT) * tsize2; \
          }



/* increment histogram bucket depending on the given __value
 *
 * NOTE: This macro can be used to build a histogram of access
 * sizes, offsets, etc. It assumes a 6-bucket histogram, with
 * __bucket_base_p pointing to the first counter in the sequence
 * of buckets (i.e., the smallest bucket). The size ranges of each
 * bucket are:
 *      * 0 - 256 bytes
 *      * 256 - 1 KiB
 *      * 1 KiB - 8 KiB
 *      * 32 KiB - 256 KiB
 *      * 256 KiB - 1 MiB
 *      * 1 MiB +
 */
#define DARSHAN_MSG_BUCKET_INC(__bucket_base_p, __value) do {\
    if(__value < 257) \
        *(__bucket_base_p) += 1; \
    else if(__value < 1025) \
        *(__bucket_base_p + 1) += 1; \
    else if(__value < 8193) \
        *(__bucket_base_p + 2) += 1; \
    else if(__value < 262145) \
        *(__bucket_base_p + 3) += 1; \
    else if(__value < 1048577) \
        *(__bucket_base_p + 4) += 1; \
    else \
        *(__bucket_base_p + 5) += 1; \
} while(0)


DARSHAN_FORWARD_DECL(PMPI_Send, int, (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Ssend, int, (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Rsend, int, (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Bsend, int, (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Recv, int, (void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_Sendrecv, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype, int dest, int sendtag,
                 void *recvbuf, int recvcount, MPI_Datatype recvtype,
                 int source, int recvtag, MPI_Comm comm, MPI_Status * status));
DARSHAN_FORWARD_DECL(PMPI_Sendrecv_replace, int, (const void *buf, int count, MPI_Datatype datatype, int dest, int sendtag,
                 int source, int recvtag, MPI_Comm comm, MPI_Status * status));
DARSHAN_FORWARD_DECL(PMPI_Isend, int, (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request));
DARSHAN_FORWARD_DECL(PMPI_Issend, int, (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request));
DARSHAN_FORWARD_DECL(PMPI_Irsend, int, (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request));
DARSHAN_FORWARD_DECL(PMPI_Ibsend, int, (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request));
DARSHAN_FORWARD_DECL(PMPI_Irecv, int, (void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Request *request));
/*
DARSHAN_FORWARD_DECL(PMPI_Isendrecv, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype, int dest, int sendtag, void *recvbuf,
int recvcount, MPI_Datatype recvtype, int source, int recvtag, MPI_Comm comm, MPI_Request *request));
DARSHAN_FORWARD_DECL(PMPI_Isendrecv_replace, int, (void *buf, int count, MPI_Datatype datatype, int dest, int sendtag, int source, int recvtag, MPI_Comm comm, MPI_Request *request));
*/
DARSHAN_FORWARD_DECL(PMPI_Probe, int, (int source, int tag, MPI_Comm comm, MPI_Status * status));
DARSHAN_FORWARD_DECL(PMPI_Iprobe, int, (int source, int tag, MPI_Comm comm, int *flag, MPI_Status * status));
DARSHAN_FORWARD_DECL(PMPI_Test, int, (MPI_Request *request, int *flag, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_Testany, int, (int count, MPI_Request array_of_requests[], int *indx,
               int *flag, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_Testall, int, (int count, MPI_Request array_of_requests[], int *flag, 
               MPI_Status array_of_statuses[]));
DARSHAN_FORWARD_DECL(PMPI_Testsome, int, (int incount, MPI_Request array_of_requests[], int *outcount,
                 int array_of_indices[], MPI_Status array_of_statuses[]));
DARSHAN_FORWARD_DECL(PMPI_Wait, int, (MPI_Request * request, MPI_Status * status));
DARSHAN_FORWARD_DECL(PMPI_Waitany, int, (int count, MPI_Request array_of_requests[], int *indx, MPI_Status * status));
DARSHAN_FORWARD_DECL(PMPI_Waitall, int, (int count, MPI_Request array_of_requests[], 
               MPI_Status array_of_statuses[]));
DARSHAN_FORWARD_DECL(PMPI_Waitsome, int, (int incount, MPI_Request array_of_requests[],
                 int *outcount, int array_of_indices[], MPI_Status array_of_statuses[]));
DARSHAN_FORWARD_DECL(PMPI_Put, int, (const void *origin_addr, int origin_count, MPI_Datatype
            origin_datatype, int target_rank, MPI_Aint target_disp,
            int target_count, MPI_Datatype target_datatype, MPI_Win win));
DARSHAN_FORWARD_DECL(PMPI_Get, int, (void *origin_addr, int origin_count, MPI_Datatype
            origin_datatype, int target_rank, MPI_Aint target_disp,
            int target_count, MPI_Datatype target_datatype, MPI_Win win));
DARSHAN_FORWARD_DECL(PMPI_Barrier, int, (MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Bcast, int, (void *buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Reduce, int,  (const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Allreduce, int,  (const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Alltoall, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                 void *recvbuf, int recvcount, MPI_Datatype recvtype, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Alltoallv, int, (const void *sendbuf, const int *sendcounts, const int *sdispls,
                 MPI_Datatype sendtype, void *recvbuf, const int *recvcounts, const int *rdispls,
                 MPI_Datatype recvtype, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Alltoallw, int, (const void *sendbuf, const int sendcounts[], const int sdispls[],
                 const MPI_Datatype sendtypes[], void *recvbuf, const int recvcounts[], const int rdispls[],
                 const MPI_Datatype recvtypes[], MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Allgather, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                  void *recvbuf, int recvcount, MPI_Datatype recvtype,
                  MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Allgatherv, int, ());
DARSHAN_FORWARD_DECL(PMPI_Gather , int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
               void *recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Gatherv , int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                void *recvbuf, const int *recvcounts, const int *displs,
                MPI_Datatype recvtype, int root, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Scatter, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
               void *recvbuf, int recvcount, MPI_Datatype recvtype, int root,
               MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Scatterv, int, (const void *sendbuf, const int *sendcounts, const int *displs,
                 MPI_Datatype sendtype, void *recvbuf, int recvcount,
                 MPI_Datatype recvtype,
                 int root, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Reduce_scatter, int, (const void *sendbuf, void *recvbuf, const int recvcounts[],
                       MPI_Datatype datatype, MPI_Op op, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Scan, int, (const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype,
             MPI_Op op, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Exscan, int, (const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype,
               MPI_Op op, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Ibarrier, int, (MPI_Comm comm, MPI_Request * request));
DARSHAN_FORWARD_DECL(PMPI_Ibcast, int, (void *buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm, MPI_Request * request));
DARSHAN_FORWARD_DECL(PMPI_Ireduce, int,  (const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm comm, MPI_Request * request));
DARSHAN_FORWARD_DECL(PMPI_Iallreduce, int,  (const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, MPI_Comm comm, MPI_Request * request));
DARSHAN_FORWARD_DECL(PMPI_Ialltoall, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                 void *recvbuf, int recvcount, MPI_Datatype recvtype, MPI_Comm comm, MPI_Request * request));
DARSHAN_FORWARD_DECL(PMPI_Ialltoallv, int, (const void *sendbuf, const int *sendcounts, const int *sdispls,
                 MPI_Datatype sendtype, void *recvbuf, const int *recvcounts, const int *rdispls,
                 MPI_Datatype recvtype, MPI_Comm comm, MPI_Request * request));
DARSHAN_FORWARD_DECL(PMPI_Ialltoallw, int, (const void *sendbuf, const int sendcounts[], const int sdispls[],
                 const MPI_Datatype sendtypes[], void *recvbuf, const int recvcounts[], const int rdispls[],
                 const MPI_Datatype recvtypes[], MPI_Comm comm, MPI_Request * request));
DARSHAN_FORWARD_DECL(PMPI_Iallgather, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                  void *recvbuf, int recvcount, MPI_Datatype recvtype,
                  MPI_Comm comm, MPI_Request * request));
DARSHAN_FORWARD_DECL(PMPI_Iallgatherv, int, ());
DARSHAN_FORWARD_DECL(PMPI_Igather , int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
               void *recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm, MPI_Request * request));
DARSHAN_FORWARD_DECL(PMPI_Igatherv , int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                void *recvbuf, const int *recvcounts, const int *displs,
                MPI_Datatype recvtype, int root, MPI_Comm comm, MPI_Request * request));
DARSHAN_FORWARD_DECL(PMPI_Iscatter, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
               void *recvbuf, int recvcount, MPI_Datatype recvtype, int root,
               MPI_Comm comm, MPI_Request * request));
DARSHAN_FORWARD_DECL(PMPI_Iscatterv, int, (const void *sendbuf, const int *sendcounts, const int *displs,
                 MPI_Datatype sendtype, void *recvbuf, int recvcount,
                 MPI_Datatype recvtype,
                 int root, MPI_Comm comm, MPI_Request * request));
DARSHAN_FORWARD_DECL(PMPI_Ireduce_scatter, int, (const void *sendbuf, void *recvbuf, const int recvcounts[],
                       MPI_Datatype datatype, MPI_Op op, MPI_Comm comm, MPI_Request * request));
DARSHAN_FORWARD_DECL(PMPI_Iscan, int, (const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype,
             MPI_Op op, MPI_Comm comm, MPI_Request * request)); 
DARSHAN_FORWARD_DECL(PMPI_Iexscan, int, (const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype,
               MPI_Op op, MPI_Comm comm, MPI_Request * request));

DARSHAN_FORWARD_DECL(PMPI_Accumulate, int, (const void *origin_addr, int origin_count, MPI_Datatype
                   origin_datatype, int target_rank, MPI_Aint
                   target_disp, int target_count, MPI_Datatype
                   target_datatype, MPI_Op op, MPI_Win win));

DARSHAN_FORWARD_DECL(PMPI_Get_accumulate, int, (const void *origin_addr, int origin_count,
        MPI_Datatype origin_datatype, void *result_addr, int result_count,
        MPI_Datatype result_datatype, int target_rank, MPI_Aint target_disp,
        int target_count, MPI_Datatype target_datatype, MPI_Op op, MPI_Win win));

DARSHAN_FORWARD_DECL(PMPI_Fetch_and_op, int, (const void *origin_addr, void *result_addr,
        MPI_Datatype datatype, int target_rank, MPI_Aint target_disp,
        MPI_Op op, MPI_Win win));

DARSHAN_FORWARD_DECL(PMPI_Compare_and_swap, int, (const void *origin_addr, const void *compare_addr,
        void *result_addr, MPI_Datatype datatype, int target_rank,
        MPI_Aint target_disp, MPI_Win win));

DARSHAN_FORWARD_DECL(PMPI_Win_fence, int, (int assert, MPI_Win win));

DARSHAN_FORWARD_DECL(PMPI_Win_start, int, (MPI_Group group, int assert, MPI_Win win));

DARSHAN_FORWARD_DECL(PMPI_Win_complete, int, (MPI_Win win));

DARSHAN_FORWARD_DECL(PMPI_Win_post, int, (MPI_Group group, int assert, MPI_Win win));

DARSHAN_FORWARD_DECL(PMPI_Win_wait, int, (MPI_Win win));

DARSHAN_FORWARD_DECL(PMPI_Win_test, int, (MPI_Win win, int *flag));

DARSHAN_FORWARD_DECL(PMPI_Win_lock, int, (int lock_type, int rank, int assert, MPI_Win win));

DARSHAN_FORWARD_DECL(PMPI_Win_unlock, int, (int rank, MPI_Win win));

DARSHAN_FORWARD_DECL(PMPI_Win_unlock_all, int, (MPI_Win win));

DARSHAN_FORWARD_DECL(PMPI_Win_flush, int, (int rank, MPI_Win win));

DARSHAN_FORWARD_DECL(PMPI_Win_flush_all, int, (MPI_Win win));

DARSHAN_FORWARD_DECL(PMPI_Win_flush_local, int, (int rank, MPI_Win win));

DARSHAN_FORWARD_DECL(PMPI_Win_flush_local_all, int, (MPI_Win win));

DARSHAN_FORWARD_DECL(PMPI_Win_sync, int, (MPI_Win win));

/*
 * Global runtime struct for tracking data needed at runtime
 */
struct apmpi_runtime
{
    struct darshan_apmpi_perf_record *perf_record;
    struct darshan_apmpi_header_record *header_record;
    darshan_record_id rec_id;
    darshan_record_id header_id;
    int frozen; /* flag to indicate that the counters should no longer be modified */
};

static struct apmpi_runtime *apmpi_runtime = NULL;
static pthread_mutex_t apmpi_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

static int apmpi_runtime_init_attempted = 0;

/* my_rank indicates the MPI rank of this process */
static int my_rank = -1;

/* internal helper functions for the apmpi module */
static void apmpi_runtime_initialize(void);

/* forward declaration for shutdown function needed to interface with darshan-core */
#ifdef HAVE_MPI
static void apmpi_mpi_redux(
    void *buffer, 
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs, 
    int shared_rec_count);
#endif
static void apmpi_output(
        void **buffer, 
        int *size);
static void apmpi_cleanup(
        void);

/* macros for obtaining/releasing the apmpi module lock */
#define APMPI_LOCK() pthread_mutex_lock(&apmpi_runtime_mutex)
#define APMPI_UNLOCK() pthread_mutex_unlock(&apmpi_runtime_mutex)

/*
 * Initialize counters 
 */
static void initialize_counters (void)
{
    int i;
    for (i = 0; i < APMPI_NUM_INDICES; i++)
    {
        apmpi_runtime->perf_record->counters[i] = 0; 
    }
    for (i = 0; i < APMPI_F_MPIOP_TOTALTIME_NUM_INDICES; i++)
    {
        apmpi_runtime->perf_record->fcounters[i] = 0; 
    }
    for (i = 0; i < APMPI_F_MPIOP_SYNCTIME_NUM_INDICES; i++)
    {
        apmpi_runtime->perf_record->fsynccounters[i] = 0;
    }
    for (i = 0; i < APMPI_F_MPI_GLOBAL_NUM_INDICES; i++)
    {
        apmpi_runtime->perf_record->fsynccounters[i] = 0;
    }
    return;
}

static void finalize_counters (void)
{

    return;
}

/*
 * Function which updates all the counter data
 */
static void capture(struct darshan_apmpi_perf_record *rec,
                    darshan_record_id rec_id)
{
    rec->base_rec.id = rec_id;
    rec->base_rec.rank = my_rank;
    int name_len;
    char name[MPI_MAX_PROCESSOR_NAME];
    MPI_Get_processor_name(name, &name_len);
    strncpy(rec->node_name, name, (name_len < AP_PROCESSOR_NAME_MAX)? name_len : AP_PROCESSOR_NAME_MAX);

    return;
}

static void apmpi_runtime_initialize()
{
    size_t apmpi_buf_size;
    size_t apmpi_rec_count = 1;
    int ret;

    darshan_module_funcs mod_funcs = {
#ifdef HAVE_MPI
        .mod_redux_func = &apmpi_mpi_redux,
#endif
        .mod_output_func = &apmpi_output,
        .mod_cleanup_func = &apmpi_cleanup
        };

    APMPI_LOCK();

    /* if this attempt at initializing fails, we won't try again */
    apmpi_runtime_init_attempted = 1;

    /* don't do anything if already initialized */
    if(apmpi_runtime)
    {
        APMPI_UNLOCK();
        return;
    }

    apmpi_buf_size = sizeof(struct darshan_apmpi_header_record) +
                     sizeof(struct darshan_apmpi_perf_record);

    /* register the apmpi module with the darshan-core component */
    ret = darshan_core_register_module(
        DARSHAN_APMPI_MOD,
        mod_funcs,
        apmpi_buf_size,
        &apmpi_rec_count,
        &my_rank,
        NULL);
    if(ret < 0)
    {
        APMPI_UNLOCK();
        return;
    }

    /* initialize module's global state */
    apmpi_runtime = malloc(sizeof(*apmpi_runtime));
    if(!apmpi_runtime)
    {
        darshan_core_unregister_module(DARSHAN_APMPI_MOD);
        APMPI_UNLOCK();
        return;
    }
    memset(apmpi_runtime, 0, sizeof(*apmpi_runtime));

    if (my_rank == 0)
    {   
        apmpi_runtime->header_id = darshan_core_gen_record_id("darshan-apmpi-header");
        
        /* register the apmpi record with darshan-core */
        apmpi_runtime->header_record = darshan_core_register_record(
            apmpi_runtime->header_id,
            //NULL,
            "darshan-apmpi-header",
            DARSHAN_APMPI_MOD,
            sizeof(struct darshan_apmpi_header_record),
            NULL);
        if(!(apmpi_runtime->header_record))
        {   
            darshan_core_unregister_module(DARSHAN_APMPI_MOD);
            free(apmpi_runtime);
            apmpi_runtime = NULL;
            APMPI_UNLOCK();
           return;
        }
        apmpi_runtime->header_record->base_rec.id = apmpi_runtime->header_id;
        apmpi_runtime->header_record->base_rec.rank = my_rank;
        apmpi_runtime->header_record->magic = APMPI_MAGIC;
#ifdef __APMPI_COLL_SYNC
        apmpi_runtime->header_record->sync_flag = 1;
#else
        apmpi_runtime->header_record->sync_flag = 0;
#endif
    }

    apmpi_runtime->rec_id = darshan_core_gen_record_id("APMPI"); //record name

    apmpi_runtime->perf_record = darshan_core_register_record(
        apmpi_runtime->rec_id,
        "APMPI",
        DARSHAN_APMPI_MOD,
        sizeof(struct darshan_apmpi_perf_record),
        NULL);
    if(!(apmpi_runtime->perf_record))
    {
        darshan_core_unregister_module(DARSHAN_APMPI_MOD);
        free(apmpi_runtime);
        apmpi_runtime = NULL;
        APMPI_UNLOCK();
        return;
    }

    initialize_counters();
    /* collect perf counters */
    capture(apmpi_runtime->perf_record, apmpi_runtime->rec_id);

    APMPI_UNLOCK();

    return;
}
#if 0
static void apmpi_record_reduction_op (void* inrec_v, void* inoutrec_v,
    int *len, MPI_Datatype *datatype)
{
    struct darshan_apmpi_perf_record tmp_rec;
    struct darshan_apmpi_perf_record *inrec = inrec_v;
    struct darshan_apmpi_perf_record *inoutrec = inoutrec_v;
    int i, j, k;

    for (i=0; i<*len; i++)
    {
        memset(&tmp_rec, 0, sizeof(struct darshan_apmpi_perf_record));
        tmp_rec.base_rec.id = inrec->base_rec.id;
        tmp_file.base_rec.rank = -1;
    }
}
#endif
static void apmpi_shared_record_variance(MPI_Comm mod_comm)
{
    MPI_Datatype var_dt;
    MPI_Op var_op;
    struct darshan_variance_dt *var_send_buf = NULL;
    struct darshan_variance_dt *var_recv_buf = NULL;

    PMPI_Type_contiguous(sizeof(struct darshan_variance_dt),
        MPI_BYTE, &var_dt);
    PMPI_Type_commit(&var_dt);

    PMPI_Op_create(darshan_variance_reduce, 1, &var_op);

    var_send_buf = malloc(sizeof(struct darshan_variance_dt));
    if(!var_send_buf)
        return;

    if(my_rank == 0)
    {   
        var_recv_buf = malloc(sizeof(struct darshan_variance_dt));

        if(!var_recv_buf)
            return;
    }   

    /* get total mpi time variances across the ranks */
    var_send_buf->n = 1;
    var_send_buf->S = 0;
    var_send_buf->T = apmpi_runtime->perf_record->fglobalcounters[MPI_TOTAL_COMM_TIME];

    PMPI_Reduce(var_send_buf, var_recv_buf, 1,
        var_dt, var_op, 0, mod_comm);

    if(my_rank == 0)
    {   
       apmpi_runtime->header_record->apmpi_f_variance_total_mpitime =
                (var_recv_buf->S / var_recv_buf->n);
    }
    /* get total mpi sync time variances across the ranks */
    var_send_buf->n = 1;
    var_send_buf->S = 0;
    var_send_buf->T = apmpi_runtime->perf_record->fglobalcounters[MPI_TOTAL_COMM_SYNC_TIME];

    PMPI_Reduce(var_send_buf, var_recv_buf, 1,
        var_dt, var_op, 0, mod_comm);

    if(my_rank == 0)
    {   
       apmpi_runtime->header_record->apmpi_f_variance_total_mpisynctime =
                (var_recv_buf->S / var_recv_buf->n);
    }   
    PMPI_Type_free(&var_dt);
    PMPI_Op_free(&var_op);
    free(var_send_buf);
    free(var_recv_buf);

    return;
}


/********************************************************************************
 * shutdown function exported by this module for coordinating with darshan-core *
 ********************************************************************************/

/* Pass data for the apmpi module back to darshan-core to log to file. */
//#ifdef HAVE_MPI
static void apmpi_mpi_redux(
    void *apmpi_buf,
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count)
{
    int i;
#if 0
    struct darshan_apmpi_perf_record *red_send_buf = NULL;
    struct darshan_apmpi_perf_record *red_recv_buf = NULL;
    struct darshan_apmpi_perf_record *apmpi_rec_buf = (struct darshan_apmpi_perf_record *)apmpi_buf;
#endif
    //MPI_Datatype red_type;
    //MPI_Op red_op;

    APMPI_LOCK();

    if (!apmpi_runtime)
    {
        APMPI_UNLOCK();
        return;
    }
    double mpisync_time = 0.0;
    /* Compute Total MPI time per rank: MPI_TOTAL_COMM_TIME */
    for (i=MPI_SEND_TOTAL_TIME; i<APMPI_F_MPIOP_TOTALTIME_NUM_INDICES; i+=3){     // times (total_time, max_time, min_time)
        apmpi_runtime->perf_record->fglobalcounters[MPI_TOTAL_COMM_TIME] += apmpi_runtime->perf_record->fcounters[i];
    }
    for (i=MPI_BARRIER_TOTAL_SYNC_TIME; i<APMPI_F_MPIOP_SYNCTIME_NUM_INDICES; i++){
        mpisync_time += apmpi_runtime->perf_record->fsynccounters[i];
    }
    apmpi_runtime->perf_record->fglobalcounters[MPI_TOTAL_COMM_TIME] += mpisync_time;
    apmpi_runtime->perf_record->fglobalcounters[MPI_TOTAL_COMM_SYNC_TIME] = mpisync_time;
#if 0
    red_send_buf = apmpi_runtime->perf_record;

    if (my_rank == 0){
        red_recv_buf = malloc(sizeof(struct darshan_apmpi_perf_record));
        if(!red_recv_buf)
            {
                APMPI_UNLOCK();
                return;
            }
    }
    /* construct a datatype for a APMPI file record.  This is serving no purpose
     * except to make sure we can do a reduction on proper boundaries
     */
    PMPI_Type_contiguous(sizeof(struct darshan_apmpi_perf_record),
        MPI_BYTE, &red_type);
    PMPI_Type_commit(&red_type);

    /* register a APMPI file record reduction operator */
    PMPI_Op_create(apmpi_record_reduction_op, 1, &red_op);

    /* reduce shared APMPI file records */
    PMPI_Reduce(red_send_buf, red_recv_buf,
        shared_rec_count, red_type, red_op, 0, mod_comm);
#endif
    /* get the time variance across all ranks */
     apmpi_shared_record_variance(mod_comm);
#if 0
    /* clean up reduction state */
    if(my_rank == 0)
    {   
        free(red_recv_buf);
    }   
#endif    
    //PMPI_Type_free(&red_type);
    //PMPI_Op_free(&red_op);

    APMPI_UNLOCK();

    return;
}

//#endif
static void apmpi_output(
    void **apmpi_buf,
    int *apmpi_buf_sz)
{
    APMPI_LOCK();
    assert(apmpi_runtime);
    *apmpi_buf_sz = 0;
    if(my_rank == 0) {
    *apmpi_buf_sz += sizeof( *apmpi_runtime->header_record);
    }
    *apmpi_buf_sz += sizeof( *apmpi_runtime->perf_record);

    apmpi_runtime->frozen = 1;

    APMPI_UNLOCK();
    return;
}

static void apmpi_cleanup()
{
    APMPI_LOCK();
    assert(apmpi_runtime);

    finalize_counters();
    free(apmpi_runtime);
    apmpi_runtime = NULL;

    APMPI_UNLOCK();
    return;
}

/* note that if the break condition is triggered in this macro, then it
 * will exit the do/while loop holding a lock that will be released in
 * POST_RECORD().  Otherwise it will release the lock here (if held) and
 * return immediately without reaching the POST_RECORD() macro.
 */
#define APMPI_PRE_RECORD() do { \
       if(!__darshan_disabled) { \
           APMPI_LOCK(); \
           if(!apmpi_runtime && !apmpi_runtime_init_attempted) \
               apmpi_runtime_initialize(); \
           if(apmpi_runtime && !apmpi_runtime->frozen) break; \
           APMPI_UNLOCK(); \
       } \
       return(ret); \
   } while(0)

#define APMPI_POST_RECORD() do { \
       APMPI_UNLOCK(); \
   } while(0)

#define APMPI_RECORD_UPDATE(MPI_OP) do { \
    if(ret != MPI_SUCCESS) break; \
    apmpi_runtime->perf_record->counters[MPI_OP ## _CALL_COUNT]++; \
    apmpi_runtime->perf_record->counters[MPI_OP ## _TOTAL_BYTES] += bytes; \
    DARSHAN_MSG_BUCKET_INC(&(apmpi_runtime->perf_record->counters[MPI_OP ## _MSG_SIZE_AGG_0_256]), bytes); \
    apmpi_runtime->perf_record->fcounters[MPI_OP ## _TOTAL_TIME] += tdiff; \
    apmpi_runtime->perf_record->fcounters[MPI_OP ## _MAX_TIME] = MAX(apmpi_runtime->perf_record->fcounters[Y(MPI_OP ## _MAX_TIME)], tdiff); \
    apmpi_runtime->perf_record->fcounters[MPI_OP ## _MIN_TIME] = MIN(apmpi_runtime->perf_record->fcounters[Y(MPI_OP ## _MIN_TIME)], tdiff); \
    } while(0)

#define APMPI_RECORD_UPDATE_NOMSG(MPI_OP) do { \
    if(ret != MPI_SUCCESS) break; \
    apmpi_runtime->perf_record->counters[MPI_OP ## _CALL_COUNT]++; \
    apmpi_runtime->perf_record->fcounters[MPI_OP ## _TOTAL_TIME] += tdiff; \
    apmpi_runtime->perf_record->fcounters[MPI_OP ## _MAX_TIME] = MAX(apmpi_runtime->perf_record->fcounters[Y(MPI_OP ## _MAX_TIME)], tdiff); \
    apmpi_runtime->perf_record->fcounters[MPI_OP ## _MIN_TIME] = MIN(apmpi_runtime->perf_record->fcounters[Y(MPI_OP ## _MIN_TIME)], tdiff); \
    } while(0)

#define APMPI_RECORD_UPDATE_SYNC(MPI_OP) do { \
    if(ret != MPI_SUCCESS) break; \
    apmpi_runtime->perf_record->counters[MPI_OP ## _CALL_COUNT]++; \
    apmpi_runtime->perf_record->counters[MPI_OP ## _TOTAL_BYTES] += bytes; \
    DARSHAN_MSG_BUCKET_INC(&(apmpi_runtime->perf_record->counters[MPI_OP ## _MSG_SIZE_AGG_0_256]), bytes); \
    apmpi_runtime->perf_record->fcounters[MPI_OP ## _TOTAL_TIME] += tdiff; \
    apmpi_runtime->perf_record->fsynccounters[MPI_OP ## _TOTAL_SYNC_TIME] += tsync; \
    apmpi_runtime->perf_record->fcounters[MPI_OP ## _MAX_TIME] = MAX(apmpi_runtime->perf_record->fcounters[Y(MPI_OP ## _MAX_TIME)], tdiff); \
    apmpi_runtime->perf_record->fcounters[MPI_OP ## _MIN_TIME] = MIN(apmpi_runtime->perf_record->fcounters[Y(MPI_OP ## _MIN_TIME)], tdiff); \
    } while(0)

#define APMPI_RECORD_UPDATE_SYNC_NOMSG(MPI_OP) do { \
    if(ret != MPI_SUCCESS) break; \
    apmpi_runtime->perf_record->counters[MPI_OP ## _CALL_COUNT]++; \
    apmpi_runtime->perf_record->fcounters[MPI_OP ## _TOTAL_TIME] += tdiff; \
    apmpi_runtime->perf_record->fsynccounters[MPI_OP ## _TOTAL_SYNC_TIME] += tsync; \
    apmpi_runtime->perf_record->fcounters[MPI_OP ## _MAX_TIME] = MAX(apmpi_runtime->perf_record->fcounters[Y(MPI_OP ## _MAX_TIME)], tdiff); \
    apmpi_runtime->perf_record->fcounters[MPI_OP ## _MIN_TIME] = MIN(apmpi_runtime->perf_record->fcounters[Y(MPI_OP ## _MIN_TIME)], tdiff); \
    } while(0)
#define Y(a) a

/**********************************************************
 *        Wrappers for MPI functions of interest       * 
 **********************************************************/

int DARSHAN_DECL(MPI_Send)(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Send);
    TIME(__real_PMPI_Send(buf, count, datatype, dest, tag, comm));
    BYTECOUNT(datatype, count);
    APMPI_PRE_RECORD();
    // Potential optimization: Lock around the count - lock only if MPI_THREAD_MULTIPLE is used ... locking mutex
    APMPI_RECORD_UPDATE(MPI_SEND);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Send, int,  (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm), MPI_Send)

int DARSHAN_DECL(MPI_Ssend)(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Ssend);
    TIME(__real_PMPI_Ssend(buf, count, datatype, dest, tag, comm));
    BYTECOUNT(datatype, count);
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_SSEND);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Ssend, int,  (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm), MPI_Ssend)

int DARSHAN_DECL(MPI_Rsend)(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Rsend);
    TIME(__real_PMPI_Rsend(buf, count, datatype, dest, tag, comm));
    BYTECOUNT(datatype, count);
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_RSEND);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Rsend, int,  (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm), MPI_Rsend)

int DARSHAN_DECL(MPI_Bsend)(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Bsend);
    TIME(__real_PMPI_Bsend(buf, count, datatype, dest, tag, comm));
    BYTECOUNT(datatype, count);
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_BSEND);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Bsend, int,  (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm), MPI_Bsend)

int DARSHAN_DECL(MPI_Isend)(const void *buf, int count, MPI_Datatype datatype, int dest, int tag,
             MPI_Comm comm, MPI_Request *request)
{
    MAP_OR_FAIL(PMPI_Isend);
    TIME(__real_PMPI_Isend(buf, count, datatype, dest, tag, comm, request));
    BYTECOUNT(datatype, count);
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_ISEND);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Isend, int,  (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request), MPI_Isend)

int DARSHAN_DECL(MPI_Issend)(const void *buf, int count, MPI_Datatype datatype, int dest, int tag,
             MPI_Comm comm, MPI_Request *request)
{
    MAP_OR_FAIL(PMPI_Issend);
    TIME(__real_PMPI_Issend(buf, count, datatype, dest, tag, comm, request));
    BYTECOUNT(datatype, count);
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_ISSEND);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Issend, int,  (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request), MPI_Issend)

int DARSHAN_DECL(MPI_Irsend)(const void *buf, int count, MPI_Datatype datatype, int dest, int tag,
             MPI_Comm comm, MPI_Request *request)
{
    MAP_OR_FAIL(PMPI_Irsend);
    TIME(__real_PMPI_Irsend(buf, count, datatype, dest, tag, comm, request));
    BYTECOUNT(datatype, count);
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_IRSEND);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Irsend, int,  (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request), MPI_Irsend)

int DARSHAN_DECL(MPI_Ibsend)(const void *buf, int count, MPI_Datatype datatype, int dest, int tag,
             MPI_Comm comm, MPI_Request *request)
{
    MAP_OR_FAIL(PMPI_Ibsend);
    TIME(__real_PMPI_Ibsend(buf, count, datatype, dest, tag, comm, request));
    BYTECOUNT(datatype, count);
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_IBSEND);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Ibsend, int,  (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request), MPI_Ibsend)

int DARSHAN_DECL(MPI_Recv)(void *buf, int count, MPI_Datatype datatype, int source, int tag,
             MPI_Comm comm, MPI_Status *status)
{
    MAP_OR_FAIL(PMPI_Recv);
    TIME(__real_PMPI_Recv(buf, count, datatype, source, tag, comm, status));
    int count_received; //, src;
    if (status != MPI_STATUS_IGNORE) {
        PMPI_Get_count(status, datatype, &count_received);
        if (count_received == MPI_UNDEFINED) count_received = count;
        //src = status->MPI_SOURCE;
    } 
    else {
        count_received = count;
        //src = source;
    }

    BYTECOUNT(datatype, count_received);
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_RECV);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Recv, int,  (void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Status *status), MPI_Recv)

int DARSHAN_DECL(MPI_Irecv)(void *buf, int count, MPI_Datatype datatype, int source, int tag,
             MPI_Comm comm, MPI_Request * request)
{
    MAP_OR_FAIL(PMPI_Irecv);

    TIME(__real_PMPI_Irecv(buf, count, datatype, source, tag, comm, request));
    BYTECOUNT(datatype, count);
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_IRECV);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Irecv, int,  (void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Request *request), MPI_Irecv)

int DARSHAN_DECL(MPI_Sendrecv)(const void *sendbuf, int sendcount, MPI_Datatype sendtype, int dest, int sendtag,
                 void *recvbuf, int recvcount, MPI_Datatype recvtype,
                 int source, int recvtag, MPI_Comm comm, MPI_Status * status)
{
    MAP_OR_FAIL(PMPI_Sendrecv);
    
    TIME(__real_PMPI_Sendrecv(sendbuf, sendcount, sendtype, dest, sendtag, recvbuf, recvcount, recvtype, source, recvtag, comm, status));
    
    int count_received; //, src;
    if (status != MPI_STATUS_IGNORE) {
        PMPI_Get_count(status, recvtype, &count_received);
        if (count_received == MPI_UNDEFINED) count_received = recvcount;
        //src = status->MPI_SOURCE;
    }
    else {
        count_received = recvcount;
        //src = source;
    }
    BYTECOUNT(sendtype, sendcount);
    ap_bytes_t sbytes = bytes;
    BYTECOUNTND(recvtype, count_received);
    bytes += sbytes;
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_SENDRECV);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Sendrecv, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype, int dest, int sendtag,
                 void *recvbuf, int recvcount, MPI_Datatype recvtype,
                 int source, int recvtag, MPI_Comm comm, MPI_Status * status), MPI_Sendrecv)
int DARSHAN_DECL(MPI_Sendrecv_replace)(void *buf, int count, MPI_Datatype datatype, int dest, int sendtag,
                 int source, int recvtag, MPI_Comm comm, MPI_Status * status)
{
    MAP_OR_FAIL(PMPI_Sendrecv_replace);
    TIME(__real_PMPI_Sendrecv_replace(buf, count, datatype, dest, sendtag, source, recvtag, comm, status));
    
    int count_received; //, src;
    if (status != MPI_STATUS_IGNORE) {
        PMPI_Get_count(status, datatype, &count_received);
        if (count_received == MPI_UNDEFINED) count_received = count;
        //src = status->MPI_SOURCE;
    }
    else {
        count_received = count;
        //src = source;
    }
    BYTECOUNT(datatype, count + count_received);
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_SENDRECV_REPLACE);
    APMPI_POST_RECORD();

    return ret;
}
/*
int DARSHAN_DECL(MPI_Isendrecv)(const void *sendbuf, int sendcount, MPI_Datatype sendtype, int dest, int sendtag,
                 void *recvbuf, int recvcount, MPI_Datatype recvtype,
                 int source, int recvtag, MPI_Comm comm, MPI_Request *request)
{
    MAP_OR_FAIL(PMPI_Isendrecv);
    
    TIME(__real_PMPI_Isendrecv(sendbuf, sendcount, sendtype, dest, sendtag, recvbuf, recvcount, recvtype, source, recvtag, comm, request));
    
    BYTECOUNT(sendtype, sendcount + recvcount);
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_ISENDRECV);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Isendrecv, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype, int dest, int sendtag,
                 void *recvbuf, int recvcount, MPI_Datatype recvtype,
                 int source, int recvtag, MPI_Comm comm, MPI_Request *request), MPI_Isendrecv)

int DARSHAN_DECL(MPI_Isendrecv_replace)(void *buf, int count, MPI_Datatype datatype, int dest, int sendtag,
                 int source, int recvtag, MPI_Comm comm, MPI_Request *request)
{
    MAP_OR_FAIL(PMPI_Isendrecv_replace);
    
    TIME(__real_PMPI_Isendrecv_replace(buf, count, datatype, dest, sendtag, source, recvtag, comm, request));
    
    BYTECOUNT(datatype, count + count);
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_ISENDRECV_REPLACE);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Isendrecv_replace, int, (void *buf, int count, MPI_Datatype datatype, int dest, int sendtag,
                 int source, int recvtag, MPI_Comm comm, MPI_Request *request), MPI_Isendrecv_replace)
*/
int DARSHAN_DECL(MPI_Put)(const void *origin_addr, int origin_count, MPI_Datatype
            origin_datatype, int target_rank, MPI_Aint target_disp,
            int target_count, MPI_Datatype target_datatype, MPI_Win win)
{
    MAP_OR_FAIL(PMPI_Put);
    TIME(__real_PMPI_Put(origin_addr, origin_count, origin_datatype, target_rank,
            target_disp, target_count, target_datatype, win));
    BYTECOUNT(origin_datatype, origin_count); 
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_PUT);
    APMPI_POST_RECORD();

    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Put, int, (const void *origin_addr, int origin_count, MPI_Datatype
            origin_datatype, int target_rank, MPI_Aint target_disp,
            int target_count, MPI_Datatype target_datatype, MPI_Win win), MPI_Put)

int DARSHAN_DECL(MPI_Get)(void *origin_addr, int origin_count, MPI_Datatype
            origin_datatype, int target_rank, MPI_Aint target_disp,
            int target_count, MPI_Datatype target_datatype, MPI_Win win)
{
    MAP_OR_FAIL(PMPI_Get);
    TIME(__real_PMPI_Get(origin_addr, origin_count, origin_datatype, target_rank,
               target_disp, target_count, target_datatype, win));
    
    BYTECOUNT(target_datatype, target_count); 
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_GET);
    APMPI_POST_RECORD();
    
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Get, int, (void *origin_addr, int origin_count, MPI_Datatype
            origin_datatype, int target_rank, MPI_Aint target_disp,
            int target_count, MPI_Datatype target_datatype, MPI_Win win), MPI_Get)


int DARSHAN_DECL(MPI_Accumulate)(const void *origin_addr, int origin_count, MPI_Datatype
                   origin_datatype, int target_rank, MPI_Aint
                   target_disp, int target_count, MPI_Datatype
                   target_datatype, MPI_Op op, MPI_Win win)
{
    MAP_OR_FAIL(PMPI_Accumulate);
    TIME(__real_PMPI_Accumulate(origin_addr, origin_count, 
                   origin_datatype, target_rank, 
                   target_disp, target_count, 
                   target_datatype, op, win));
    
    BYTECOUNT(target_datatype, target_count);
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_ACCUMULATE);
    APMPI_POST_RECORD();
    
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Accumulate, int, (const void *origin_addr, int origin_count, MPI_Datatype
                   origin_datatype, int target_rank, MPI_Aint
                   target_disp, int target_count, MPI_Datatype
                   target_datatype, MPI_Op op, MPI_Win win), MPI_Accumulate)

int DARSHAN_DECL(MPI_Get_accumulate)(const void *origin_addr, int origin_count,
        MPI_Datatype origin_datatype, void *result_addr, int result_count,
        MPI_Datatype result_datatype, int target_rank, MPI_Aint target_disp,
        int target_count, MPI_Datatype target_datatype, MPI_Op op, MPI_Win win)
{
    MAP_OR_FAIL(PMPI_Get_accumulate);
    TIME(__real_PMPI_Get_accumulate(origin_addr, origin_count,
        origin_datatype, result_addr, result_count,
        result_datatype, target_rank, target_disp,
        target_count, target_datatype, op, win));
    
    BYTECOUNT(target_datatype, target_count);
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_GET_ACCUMULATE);
    APMPI_POST_RECORD();
    
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Get_accumulate, int, (const void *origin_addr, int origin_count,
        MPI_Datatype origin_datatype, void *result_addr, int result_count,
        MPI_Datatype result_datatype, int target_rank, MPI_Aint target_disp,
        int target_count, MPI_Datatype target_datatype, MPI_Op op, MPI_Win win), MPI_Get_accumulate)

int DARSHAN_DECL(MPI_Fetch_and_op)(const void *origin_addr, void *result_addr,
        MPI_Datatype datatype, int target_rank, MPI_Aint target_disp,
        MPI_Op op, MPI_Win win)
{
    MAP_OR_FAIL(PMPI_Fetch_and_op);
    TIME(__real_PMPI_Fetch_and_op(origin_addr, result_addr,
        datatype, target_rank, target_disp,
        op, win));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_FETCH_AND_OP);
    APMPI_POST_RECORD();
    
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Fetch_and_op, int, (const void *origin_addr, void *result_addr,
        MPI_Datatype datatype, int target_rank, MPI_Aint target_disp,
        MPI_Op op, MPI_Win win), MPI_Fetch_and_op)

int DARSHAN_DECL(MPI_Compare_and_swap)(const void *origin_addr, const void *compare_addr,
        void *result_addr, MPI_Datatype datatype, int target_rank,
        MPI_Aint target_disp, MPI_Win win)
{
    MAP_OR_FAIL(PMPI_Compare_and_swap);
    TIME(__real_PMPI_Compare_and_swap(origin_addr, compare_addr,
        result_addr, datatype, target_rank,
        target_disp, win));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_COMPARE_AND_SWAP);
    APMPI_POST_RECORD();
    
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Compare_and_swap, int, (const void *origin_addr, const void *compare_addr,
        void *result_addr, MPI_Datatype datatype, int target_rank,
        MPI_Aint target_disp, MPI_Win win), MPI_Compare_and_swap)

int DARSHAN_DECL(MPI_Win_fence)(int assert, MPI_Win win)
{
    MAP_OR_FAIL(PMPI_Win_fence);
    TIME(__real_PMPI_Win_fence(assert, win));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_WIN_FENCE);
    APMPI_POST_RECORD();
    
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Win_fence, int, (int assert, MPI_Win win), MPI_Win_fence)

int DARSHAN_DECL(MPI_Win_start)(MPI_Group group, int assert, MPI_Win win)
{
    MAP_OR_FAIL(PMPI_Win_start);
    TIME(__real_PMPI_Win_start(group, assert, win));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_WIN_START);
    APMPI_POST_RECORD();
    
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Win_start, int, (MPI_Group group, int assert, MPI_Win win), MPI_Win_start)

int DARSHAN_DECL(MPI_Win_complete)(MPI_Win win)
{
    MAP_OR_FAIL(PMPI_Win_complete);
    TIME(__real_PMPI_Win_complete(win));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_WIN_COMPLETE);
    APMPI_POST_RECORD();
    
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Win_complete, int, (MPI_Win win), MPI_Win_complete)

int DARSHAN_DECL(MPI_Win_post)(MPI_Group group, int assert, MPI_Win win)
{
    MAP_OR_FAIL(PMPI_Win_post);
    TIME(__real_PMPI_Win_post(group, assert, win));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_WIN_POST);
    APMPI_POST_RECORD();
    
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Win_post, int, (MPI_Group group, int assert, MPI_Win win), MPI_Win_post)

int DARSHAN_DECL(MPI_Win_wait)(MPI_Win win)
{
    MAP_OR_FAIL(PMPI_Win_wait);
    TIME(__real_PMPI_Win_wait(win));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_WIN_WAIT);
    APMPI_POST_RECORD();
    
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Win_wait, int, (MPI_Win win), MPI_Win_wait)

int DARSHAN_DECL(MPI_Win_test)(MPI_Win win, int *flag)
{
    MAP_OR_FAIL(PMPI_Win_test);
    TIME(__real_PMPI_Win_test(win, flag));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_WIN_TEST);
    APMPI_POST_RECORD();
    
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Win_test, int, (MPI_Win win, int *flag), MPI_Win_test)

int DARSHAN_DECL(MPI_Win_lock)(int lock_type, int rank, int assert, MPI_Win win)
{
    MAP_OR_FAIL(PMPI_Win_lock);
    TIME(__real_PMPI_Win_lock(lock_type, rank, assert, win));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_WIN_LOCK);
    APMPI_POST_RECORD();
    
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Win_lock, int, (int lock_type, int rank, int assert, MPI_Win win), MPI_Win_lock)

int DARSHAN_DECL(MPI_Win_unlock)(int rank, MPI_Win win)
{
    MAP_OR_FAIL(PMPI_Win_unlock);
    TIME(__real_PMPI_Win_unlock(rank, win));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_WIN_UNLOCK);
    APMPI_POST_RECORD();
    
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Win_unlock, int, (int rank, MPI_Win win), MPI_Win_unlock)

int DARSHAN_DECL(MPI_Win_unlock_all)(MPI_Win win)
{
    MAP_OR_FAIL(PMPI_Win_unlock_all);
    TIME(__real_PMPI_Win_unlock_all(win));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_WIN_UNLOCK_ALL);
    APMPI_POST_RECORD();
    
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Win_unlock_all, int, (MPI_Win win), MPI_Win_unlock_all)

int DARSHAN_DECL(MPI_Win_flush)(int rank, MPI_Win win)
{
    MAP_OR_FAIL(PMPI_Win_flush);
    TIME(__real_PMPI_Win_flush(rank, win));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_WIN_FLUSH);
    APMPI_POST_RECORD();
    
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Win_flush, int, (int rank, MPI_Win win), MPI_Win_flush)

int DARSHAN_DECL(MPI_Win_flush_all)(MPI_Win win)
{
    MAP_OR_FAIL(PMPI_Win_flush_all);
    TIME(__real_PMPI_Win_flush_all(win));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_WIN_FLUSH_ALL);
    APMPI_POST_RECORD();
    
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Win_flush_all, int, (MPI_Win win), MPI_Win_flush_all)

int DARSHAN_DECL(MPI_Win_flush_local)(int rank, MPI_Win win)
{
    MAP_OR_FAIL(PMPI_Win_flush_local);
    TIME(__real_PMPI_Win_flush_local(rank, win));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_WIN_FLUSH_LOCAL);
    APMPI_POST_RECORD();
    
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Win_flush_local, int, (int rank, MPI_Win win), MPI_Win_flush_local)

int DARSHAN_DECL(MPI_Win_flush_local_all)(MPI_Win win)
{
    MAP_OR_FAIL(PMPI_Win_flush_local_all);
    TIME(__real_PMPI_Win_flush_local_all(win));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_WIN_FLUSH_LOCAL_ALL);
    APMPI_POST_RECORD();
    
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Win_flush_local_all, int, (MPI_Win win), MPI_Win_flush_local_all)

int DARSHAN_DECL(MPI_Win_sync)(MPI_Win win)
{
    MAP_OR_FAIL(PMPI_Win_sync);
    TIME(__real_PMPI_Win_sync(win));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_WIN_SYNC);
    APMPI_POST_RECORD();
    
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Win_sync, int, (MPI_Win win), MPI_Win_sync)

int DARSHAN_DECL(MPI_Probe)(int source, int tag, MPI_Comm comm, MPI_Status * status)
{
    MAP_OR_FAIL(PMPI_Probe);
    TIME(__real_PMPI_Probe(source, tag, comm, status));

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_PROBE);
    APMPI_POST_RECORD();

    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Probe, int,  (int source, int tag, MPI_Comm comm, MPI_Status * status), MPI_Probe)

int DARSHAN_DECL(MPI_Iprobe)(int source, int tag, MPI_Comm comm, int *flag, MPI_Status * status)
{
    MAP_OR_FAIL(PMPI_Iprobe);
    TIME(__real_PMPI_Iprobe(source, tag, comm, flag, status));

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_IPROBE);
    APMPI_POST_RECORD();

    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Iprobe, int,  (int source, int tag, MPI_Comm comm, int *flag, MPI_Status * status), MPI_Iprobe)

int DARSHAN_DECL(MPI_Test)(MPI_Request *request, int *flag, MPI_Status *status)
{
    MAP_OR_FAIL(PMPI_Test);
    TIME(__real_PMPI_Test(request, flag, status));

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_TEST);
    APMPI_POST_RECORD();

    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Test, int,  (MPI_Request *request, int *flag, MPI_Status *status), MPI_Test)

int DARSHAN_DECL(MPI_Testany)(int count, MPI_Request array_of_requests[], int *indx,
               int *flag, MPI_Status *status)
{
    MAP_OR_FAIL(PMPI_Testany);
    TIME(__real_PMPI_Testany(count, array_of_requests, indx, flag, status));

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_TESTANY);
    APMPI_POST_RECORD();

    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Testany, int,  (int count, MPI_Request array_of_requests[], int *indx,
               int *flag, MPI_Status *status), MPI_Testany)

int DARSHAN_DECL(MPI_Testall)(int count, MPI_Request array_of_requests[], 
               int *flag, MPI_Status array_of_statuses[])
{
    MAP_OR_FAIL(PMPI_Testall);
    TIME(__real_PMPI_Testall(count, array_of_requests, flag, array_of_statuses));

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_TESTALL);
    APMPI_POST_RECORD();

    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Testall, int,  (int count, MPI_Request array_of_requests[], 
               int *flag, MPI_Status array_of_statuses[]), MPI_Testall)

int DARSHAN_DECL(MPI_Testsome)(int incount, MPI_Request array_of_requests[], int *outcount,
                 int array_of_indices[], MPI_Status array_of_statuses[])
{
    MAP_OR_FAIL(PMPI_Testsome);
    TIME(__real_PMPI_Testsome(incount, array_of_requests, outcount, array_of_indices, array_of_statuses));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_TESTSOME);
    APMPI_POST_RECORD();

    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Testsome, int, (int incount, MPI_Request array_of_requests[], int *outcount,
                 int array_of_indices[], MPI_Status array_of_statuses[]), MPI_Testsome)

int DARSHAN_DECL(MPI_Wait)(MPI_Request * request, MPI_Status * status)
{
    MAP_OR_FAIL(PMPI_Wait);
    TIME(__real_PMPI_Wait(request, status));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_WAIT);
    APMPI_POST_RECORD();

    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Wait, int, (MPI_Request * request, MPI_Status * status), MPI_Wait)

int DARSHAN_DECL(MPI_Waitany)(int count, MPI_Request array_of_requests[], int *indx, MPI_Status * status)
{
    MAP_OR_FAIL(PMPI_Waitany);
    TIME(__real_PMPI_Waitany(count, array_of_requests, indx, status));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_WAITANY);
    APMPI_POST_RECORD();

    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Waitany, int, (int count, MPI_Request array_of_requests[], int *indx, MPI_Status * status), MPI_Waitany)

int DARSHAN_DECL(MPI_Waitall)(int count, MPI_Request array_of_requests[], 
               MPI_Status array_of_statuses[])
{
    MAP_OR_FAIL(PMPI_Waitall);
    TIME(__real_PMPI_Waitall(count, array_of_requests, array_of_statuses));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_WAITALL);
    APMPI_POST_RECORD();

    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Waitall, int, (int count, MPI_Request array_of_requests[], 
               MPI_Status array_of_statuses[]), MPI_Waitall)

int DARSHAN_DECL(MPI_Waitsome)(int incount, MPI_Request array_of_requests[],
                 int *outcount, int array_of_indices[], MPI_Status array_of_statuses[])
{
    MAP_OR_FAIL(PMPI_Waitsome);
    TIME(__real_PMPI_Waitsome(incount, array_of_requests, outcount, array_of_indices, array_of_statuses));
    
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_WAITSOME);
    APMPI_POST_RECORD();

    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Waitsome, int, (int incount, MPI_Request array_of_requests[],
                 int *outcount, int array_of_indices[], MPI_Status array_of_statuses[]), MPI_Waitsome)

int DARSHAN_DECL(MPI_Barrier)(MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Barrier);
    TIME_SYNC(__real_PMPI_Barrier(comm));
  
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_SYNC_NOMSG(MPI_BARRIER);
    APMPI_POST_RECORD();

    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Barrier, int,  (MPI_Comm comm), MPI_Barrier)

int DARSHAN_DECL(MPI_Bcast)(void *buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Bcast);
    
    TIME_SYNC(__real_PMPI_Bcast(buffer, count, datatype, root, comm));

    ap_bytes_t bytes = 0;
    if (root != MPI_PROC_NULL) {
        BYTECOUNTND(datatype, count);
    }

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_SYNC(MPI_BCAST);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Bcast, int, (void *buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm), MPI_Bcast)

int DARSHAN_DECL(MPI_Reduce)(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root,
             MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Reduce);
  
    TIME_SYNC(__real_PMPI_Reduce(sendbuf, recvbuf, count, datatype, op, root, comm));

    ap_bytes_t bytes = 0;
    if (root != MPI_PROC_NULL) {
        BYTECOUNTND(datatype, count);
    }

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_SYNC(MPI_REDUCE);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Reduce, int,  (const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm comm), MPI_Reduce)

int DARSHAN_DECL(MPI_Allreduce)(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op,
             MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Allreduce);

    TIME_SYNC(__real_PMPI_Allreduce(sendbuf, recvbuf, count, datatype, op, comm));

    BYTECOUNT(datatype, count);

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_SYNC(MPI_ALLREDUCE);
    APMPI_POST_RECORD();

    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Allreduce, int,  (const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, MPI_Comm comm), MPI_Allreduce)

int DARSHAN_DECL(MPI_Alltoall)(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                 void *recvbuf, int recvcount, MPI_Datatype recvtype,
                 MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Alltoall);
  
    TIME_SYNC(__real_PMPI_Alltoall(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm));

    BYTECOUNT(recvtype, recvcount);
    int tasks;
    PMPI_Comm_size(comm, &tasks);
    bytes = bytes*tasks;

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_SYNC(MPI_ALLTOALL);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Alltoall, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                 void *recvbuf, int recvcount, MPI_Datatype recvtype,
                 MPI_Comm comm), MPI_Alltoall)

int DARSHAN_DECL(MPI_Alltoallv)(const void *sendbuf, const int *sendcounts, const int *sdispls,
                MPI_Datatype sendtype, void *recvbuf, const int *recvcounts, const int *rdispls,
                MPI_Datatype recvtype, MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Alltoallv);
  
    TIME_SYNC(__real_PMPI_Alltoallv(sendbuf, sendcounts, sdispls, sendtype, recvbuf, recvcounts, rdispls, recvtype, comm));

    int i, tasks, count = 0;
    PMPI_Comm_size(comm, &tasks);
    for (i=0; i<tasks; i++) count += recvcounts[i];
    BYTECOUNT(recvtype, count);

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_SYNC(MPI_ALLTOALLV);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Alltoallv, int, (const void *sendbuf, const int *sendcounts, const int *sdispls,
                 MPI_Datatype sendtype, void *recvbuf, const int *recvcounts, const int *rdispls,
                 MPI_Datatype recvtype, MPI_Comm comm), MPI_Alltoallv)

int DARSHAN_DECL(MPI_Alltoallw)(const void *sendbuf, const int sendcounts[], const int sdispls[],
                const MPI_Datatype sendtypes[], void *recvbuf, const int recvcounts[], const int rdispls[],
                const MPI_Datatype recvtypes[], MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Alltoallw);
  
    TIME_SYNC(__real_PMPI_Alltoallw(sendbuf, sendcounts, sdispls, sendtypes, recvbuf, recvcounts, rdispls, recvtypes, comm));

    ap_bytes_t bytes = 0, tmp_bytes = 0;
    int i, tasks;
    PMPI_Comm_size(comm, &tasks);
    for (i=0; i<tasks; i++) {
      BYTECOUNTND(recvtypes[i], recvcounts[i]);
      tmp_bytes += bytes;
    }
    bytes = tmp_bytes;


    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_SYNC(MPI_ALLTOALLW);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Alltoallw, int, (const void *sendbuf, const int sendcounts[], const int sdispls[],
                 const MPI_Datatype sendtypes[], void *recvbuf, const int recvcounts[], const int rdispls[],
                 const MPI_Datatype recvtypes[], MPI_Comm comm), MPI_Alltoallw)


int DARSHAN_DECL(MPI_Allgather)(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                  void *recvbuf, int recvcount, MPI_Datatype recvtype,
                  MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Allgather);

    TIME_SYNC(__real_PMPI_Allgather(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm));
    
    ap_bytes_t bytes;
    if (sendbuf != MPI_IN_PLACE) {
        BYTECOUNTND(sendtype, sendcount);
    } 
    else {
        BYTECOUNTND(recvtype, recvcount);
    }

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_SYNC(MPI_ALLGATHER);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Allgather, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                  void *recvbuf, int recvcount, MPI_Datatype recvtype,
                  MPI_Comm comm), MPI_Allgather)

int DARSHAN_DECL(MPI_Allgatherv)(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                   void *recvbuf, const int *recvcounts, const int *displs,
                   MPI_Datatype recvtype, MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Allgatherv);

    TIME_SYNC(__real_PMPI_Allgatherv(sendbuf, sendcount, sendtype, recvbuf, recvcounts, displs, recvtype, comm));
    
    ap_bytes_t bytes;
    if (sendbuf != MPI_IN_PLACE) {
         BYTECOUNTND(sendtype, sendcount);
    }
    else {
        int rank;
        PMPI_Comm_rank(comm, &rank);
        BYTECOUNTND(recvtype, recvcounts[rank]);
    }

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_SYNC(MPI_ALLGATHERV);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Allgatherv, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                   void *recvbuf, const int *recvcounts, const int *displs,
                   MPI_Datatype recvtype, MPI_Comm comm), MPI_Allgatherv)

int DARSHAN_DECL(MPI_Gather)(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
               void *recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Gather);

    TIME_SYNC(__real_PMPI_Gather(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, comm));
    
    ap_bytes_t bytes = 0;
    if (root == MPI_PROC_NULL) {
        bytes = 0;
    } 
    else if (root == MPI_ROOT) {
        BYTECOUNTND(recvtype, recvcount);
    } 
    else if (sendbuf == MPI_IN_PLACE) {
        BYTECOUNTND(recvtype, recvcount);
    } 
    else {
        BYTECOUNTND(sendtype, sendcount);
    }

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_SYNC(MPI_GATHER);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Gather , int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
               void *recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm), MPI_Gather )

int DARSHAN_DECL(MPI_Gatherv)(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                void *recvbuf, const int *recvcounts, const int *displs,
                MPI_Datatype recvtype, int root, MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Gatherv);

    TIME_SYNC(__real_PMPI_Gatherv(sendbuf, sendcount, sendtype, recvbuf, recvcounts, displs, recvtype, root, comm));
    
    ap_bytes_t bytes = 0;
    if (root == MPI_PROC_NULL) {
        bytes = 0;
    } 
    else if (root == MPI_ROOT) {
        bytes = 0;
    } 
    else if (sendbuf == MPI_IN_PLACE) {
        BYTECOUNTND(recvtype, recvcounts[root]);
    } 
    else {
        BYTECOUNTND(sendtype, sendcount);
    }

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_SYNC(MPI_GATHERV);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Gatherv, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                void *recvbuf, const int *recvcounts, const int *displs,
                MPI_Datatype recvtype, int root, MPI_Comm comm), MPI_Gatherv)

int DARSHAN_DECL(MPI_Scatter)(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
               void *recvbuf, int recvcount, MPI_Datatype recvtype, int root,
               MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Scatter);

    TIME_SYNC(__real_PMPI_Scatter(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, comm));
    
    ap_bytes_t bytes;
     if (root == MPI_PROC_NULL) {
        bytes = 0;
    } 
    else if (root == MPI_ROOT) {
        BYTECOUNTND(recvtype, recvcount);
    }
    else if (recvbuf == MPI_IN_PLACE) {
        BYTECOUNTND(sendtype, sendcount);
    }
    else {
        BYTECOUNTND(recvtype, recvcount);
    }

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_SYNC(MPI_SCATTER);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Scatter, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
               void *recvbuf, int recvcount, MPI_Datatype recvtype, int root,
               MPI_Comm comm), MPI_Scatter)

int DARSHAN_DECL(MPI_Scatterv)(const void *sendbuf, const int *sendcounts, const int *displs,
                 MPI_Datatype sendtype, void *recvbuf, int recvcount,
                 MPI_Datatype recvtype,
                 int root, MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Scatterv);

    TIME_SYNC(__real_PMPI_Scatterv(sendbuf, sendcounts, displs, sendtype, recvbuf, recvcount, recvtype, root, comm));
    
    ap_bytes_t bytes;
    if (root == MPI_PROC_NULL) {
        bytes = 0;
    }
    else if (root == MPI_ROOT) {
        bytes = 0;
    } 
    else if (recvbuf == MPI_IN_PLACE) {
        BYTECOUNTND(sendtype, sendcounts[root]);
    } else {
        BYTECOUNTND(recvtype, recvcount);
    }

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_SYNC(MPI_SCATTERV);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Scatterv, int, (const void *sendbuf, const int *sendcounts, const int *displs,
                 MPI_Datatype sendtype, void *recvbuf, int recvcount,
                 MPI_Datatype recvtype,
                 int root, MPI_Comm comm), MPI_Scatterv)

int DARSHAN_DECL(MPI_Reduce_scatter)(const void *sendbuf, void *recvbuf, const int recvcounts[],
                       MPI_Datatype datatype, MPI_Op op, MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Reduce_scatter);

    TIME_SYNC(__real_PMPI_Reduce_scatter(sendbuf, recvbuf, recvcounts,
                       datatype, op, comm));

    int i, tasks, num = 0;
    PMPI_Comm_size(comm, &tasks);
    for (i=0; i<tasks; i++) num += recvcounts[i];
    BYTECOUNT(datatype, num);

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_SYNC(MPI_REDUCE_SCATTER);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Reduce_scatter, int, (const void *sendbuf, void *recvbuf, const int recvcounts[],
                       MPI_Datatype datatype, MPI_Op op, MPI_Comm comm), MPI_Reduce_scatter)

int DARSHAN_DECL(MPI_Scan)(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype,
             MPI_Op op, MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Scan);

    TIME_SYNC(__real_PMPI_Scan(sendbuf, recvbuf, count, datatype, op, comm));
    
    BYTECOUNT(datatype, count);
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_SYNC(MPI_SCAN);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Scan, int, (const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype,
             MPI_Op op, MPI_Comm comm), MPI_Scan )

int DARSHAN_DECL(MPI_Exscan)(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype,
               MPI_Op op, MPI_Comm comm)
{
    MAP_OR_FAIL(PMPI_Exscan);

    TIME_SYNC(__real_PMPI_Exscan(sendbuf, recvbuf, count, datatype, op, comm));

    BYTECOUNT(datatype, count);
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_SYNC(MPI_EXSCAN);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Exscan, int, (const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype,
               MPI_Op op, MPI_Comm comm), MPI_Exscan)

/* Nonblocking collective operations */
int DARSHAN_DECL(MPI_Ibarrier)(MPI_Comm comm, MPI_Request * request)
{
    MAP_OR_FAIL(PMPI_Ibarrier);
    TIME(__real_PMPI_Ibarrier(comm, request));
  
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE_NOMSG(MPI_IBARRIER);
    APMPI_POST_RECORD();

    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Ibarrier, int,  (MPI_Comm comm, MPI_Request * request), MPI_Ibarrier)

int DARSHAN_DECL(MPI_Ibcast)(void *buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm, MPI_Request * request)
{
    MAP_OR_FAIL(PMPI_Ibcast);
    
    TIME(__real_PMPI_Ibcast(buffer, count, datatype, root, comm, request));

    ap_bytes_t bytes = 0;
    if (root != MPI_PROC_NULL) {
        BYTECOUNTND(datatype, count);
    }

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_IBCAST);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Ibcast, int, (void *buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm, MPI_Request * request), MPI_Ibcast)

int DARSHAN_DECL(MPI_Ireduce)(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root,
             MPI_Comm comm, MPI_Request * request)
{
    MAP_OR_FAIL(PMPI_Ireduce);
  
    TIME(__real_PMPI_Ireduce(sendbuf, recvbuf, count, datatype, op, root, comm, request));

    ap_bytes_t bytes = 0;
    if (root != MPI_PROC_NULL) {
        BYTECOUNTND(datatype, count);
    }

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_IREDUCE);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Ireduce, int,  (const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm comm, MPI_Request * request), MPI_Ireduce)

int DARSHAN_DECL(MPI_Iallreduce)(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op,
             MPI_Comm comm, MPI_Request * request)
{
    MAP_OR_FAIL(PMPI_Iallreduce);

    TIME(__real_PMPI_Iallreduce(sendbuf, recvbuf, count, datatype, op, comm, request));

    BYTECOUNT(datatype, count);

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_IALLREDUCE);
    APMPI_POST_RECORD();

    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Iallreduce, int,  (const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, MPI_Comm comm, MPI_Request * request), MPI_Iallreduce)

int DARSHAN_DECL(MPI_Ialltoall)(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                 void *recvbuf, int recvcount, MPI_Datatype recvtype,
                 MPI_Comm comm, MPI_Request * request)
{
    MAP_OR_FAIL(PMPI_Ialltoall);
  
    TIME(__real_PMPI_Ialltoall(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm, request));

    BYTECOUNT(recvtype, recvcount);
    int tasks;
    PMPI_Comm_size(comm, &tasks);
    bytes = bytes*tasks;

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_IALLTOALL);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Ialltoall, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                 void *recvbuf, int recvcount, MPI_Datatype recvtype,
                 MPI_Comm comm, MPI_Request * request), MPI_Ialltoall)

int DARSHAN_DECL(MPI_Ialltoallv)(const void *sendbuf, const int *sendcounts, const int *sdispls,
                MPI_Datatype sendtype, void *recvbuf, const int *recvcounts, const int *rdispls,
                MPI_Datatype recvtype, MPI_Comm comm, MPI_Request * request)
{
    MAP_OR_FAIL(PMPI_Ialltoallv);
  
    TIME(__real_PMPI_Ialltoallv(sendbuf, sendcounts, sdispls, sendtype, recvbuf, recvcounts, rdispls, recvtype, comm, request));

    int i, tasks, count = 0;
    PMPI_Comm_size(comm, &tasks);
    for (i=0; i<tasks; i++) count += recvcounts[i];
    BYTECOUNT(recvtype, count);

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_IALLTOALLV);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Ialltoallv, int, (const void *sendbuf, const int *sendcounts, const int *sdispls,
                 MPI_Datatype sendtype, void *recvbuf, const int *recvcounts, const int *rdispls,
                 MPI_Datatype recvtype, MPI_Comm comm, MPI_Request * request), MPI_Ialltoallv)

int DARSHAN_DECL(MPI_Ialltoallw)(const void *sendbuf, const int sendcounts[], const int sdispls[],
                const MPI_Datatype sendtypes[], void *recvbuf, const int recvcounts[], const int rdispls[],
                const MPI_Datatype recvtypes[], MPI_Comm comm, MPI_Request * request)
{
    MAP_OR_FAIL(PMPI_Ialltoallw);
  
    TIME(__real_PMPI_Ialltoallw(sendbuf, sendcounts, sdispls, sendtypes, recvbuf, recvcounts, rdispls, recvtypes, comm, request));

    ap_bytes_t bytes = 0, tmp_bytes = 0;
    int i, tasks;
    PMPI_Comm_size(comm, &tasks);
    for (i=0; i<tasks; i++) {
      BYTECOUNTND(recvtypes[i], recvcounts[i]);
      tmp_bytes += bytes;
    }
    bytes = tmp_bytes;

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_IALLTOALLW);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Ialltoallw, int, (const void *sendbuf, const int sendcounts[], const int sdispls[],
                 const MPI_Datatype sendtypes[], void *recvbuf, const int recvcounts[], const int rdispls[],
                 const MPI_Datatype recvtypes[], MPI_Comm comm, MPI_Request * request), MPI_Ialltoallw)


int DARSHAN_DECL(MPI_Iallgather)(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                  void *recvbuf, int recvcount, MPI_Datatype recvtype,
                  MPI_Comm comm, MPI_Request * request)
{
    MAP_OR_FAIL(PMPI_Iallgather);

    TIME(__real_PMPI_Iallgather(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm, request));
    
    ap_bytes_t bytes;
    if (sendbuf != MPI_IN_PLACE) {
        BYTECOUNTND(sendtype, sendcount);
    } 
    else {
        BYTECOUNTND(recvtype, recvcount);
    }

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_IALLGATHER);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Iallgather, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                  void *recvbuf, int recvcount, MPI_Datatype recvtype,
                  MPI_Comm comm, MPI_Request * request), MPI_Iallgather)

int DARSHAN_DECL(MPI_Iallgatherv)(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                   void *recvbuf, const int recvcounts[], const int displs[],
                   MPI_Datatype recvtype, MPI_Comm comm, MPI_Request * request)
{
    MAP_OR_FAIL(PMPI_Iallgatherv);

    TIME(__real_PMPI_Iallgatherv(sendbuf, sendcount, sendtype, recvbuf, recvcounts, displs, recvtype, comm, request));
    
    ap_bytes_t bytes;
    if (sendbuf != MPI_IN_PLACE) {
         BYTECOUNTND(sendtype, sendcount);
    }
    else {
        int rank;
        PMPI_Comm_rank(comm, &rank);
        BYTECOUNTND(recvtype, recvcounts[rank]);
    }

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_IALLGATHERV);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Iallgatherv, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                   void *recvbuf, const int recvcounts[], const int displs[],
                   MPI_Datatype recvtype, MPI_Comm comm, MPI_Request * request), MPI_Iallgatherv)

int DARSHAN_DECL(MPI_Igather)(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
               void *recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm, MPI_Request * request)
{
    MAP_OR_FAIL(PMPI_Igather);

    TIME(__real_PMPI_Igather(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, comm, request));
    
    ap_bytes_t bytes = 0;
    if (root == MPI_PROC_NULL) {
        bytes = 0;
    } 
    else if (root == MPI_ROOT) {
        BYTECOUNTND(recvtype, recvcount);
    } 
    else if (sendbuf == MPI_IN_PLACE) {
        BYTECOUNTND(recvtype, recvcount);
    } 
    else {
        BYTECOUNTND(sendtype, sendcount);
    }

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_IGATHER);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Igather , int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
               void *recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm, MPI_Request * request), MPI_Igather)

int DARSHAN_DECL(MPI_Igatherv)(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                void *recvbuf, const int *recvcounts, const int *displs,
                MPI_Datatype recvtype, int root, MPI_Comm comm, MPI_Request * request)
{
    MAP_OR_FAIL(PMPI_Igatherv);

    TIME(__real_PMPI_Igatherv(sendbuf, sendcount, sendtype, recvbuf, recvcounts, displs, recvtype, root, comm, request));
    
    ap_bytes_t bytes = 0;
    if (root == MPI_PROC_NULL) {
        bytes = 0;
    } 
    else if (root == MPI_ROOT) {
        bytes = 0;
    } 
    else if (sendbuf == MPI_IN_PLACE) {
        BYTECOUNTND(recvtype, recvcounts[root]);
    } 
    else {
        BYTECOUNTND(sendtype, sendcount);
    }

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_IGATHERV);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Igatherv, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                void *recvbuf, const int recvcounts[],  const int displs[],
                MPI_Datatype recvtype, int root, MPI_Comm comm, MPI_Request * request), MPI_Igatherv)

int DARSHAN_DECL(MPI_Iscatter)(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
               void *recvbuf, int recvcount, MPI_Datatype recvtype, int root,
               MPI_Comm comm, MPI_Request * request)
{
    MAP_OR_FAIL(PMPI_Iscatter);

    TIME(__real_PMPI_Iscatter(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, comm, request));
    
    ap_bytes_t bytes;
     if (root == MPI_PROC_NULL) {
        bytes = 0;
    } 
    else if (root == MPI_ROOT) {
        BYTECOUNTND(recvtype, recvcount);
    }
    else if (recvbuf == MPI_IN_PLACE) {
        BYTECOUNTND(sendtype, sendcount);
    }
    else {
        BYTECOUNTND(recvtype, recvcount);
    }

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_ISCATTER);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Iscatter, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype,
               void *recvbuf, int recvcount, MPI_Datatype recvtype, int root,
               MPI_Comm comm, MPI_Request * request), MPI_Iscatter)

int DARSHAN_DECL(MPI_Iscatterv)(const void *sendbuf, const int sendcounts[], const int displs[],
                 MPI_Datatype sendtype, void *recvbuf, int recvcount,
                 MPI_Datatype recvtype,
                 int root, MPI_Comm comm, MPI_Request * request)
{
    MAP_OR_FAIL(PMPI_Iscatterv);

    TIME(__real_PMPI_Iscatterv(sendbuf, sendcounts, displs, sendtype, recvbuf, recvcount, recvtype, root, comm, request));
    
    ap_bytes_t bytes;
    if (root == MPI_PROC_NULL) {
        bytes = 0;
    }
    else if (root == MPI_ROOT) {
        bytes = 0;
    } 
    else if (recvbuf == MPI_IN_PLACE) {
        BYTECOUNTND(sendtype, sendcounts[root]);
    } else {
        BYTECOUNTND(recvtype, recvcount);
    }

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_ISCATTERV);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Iscatterv, int, (const void *sendbuf, const int sendcounts[], const int displs[],
                 MPI_Datatype sendtype, void *recvbuf, int recvcount,
                 MPI_Datatype recvtype,
                 int root, MPI_Comm comm, MPI_Request * request), MPI_Iscatterv)

int DARSHAN_DECL(MPI_Ireduce_scatter)(const void *sendbuf, void *recvbuf, const int recvcounts[],
                       MPI_Datatype datatype, MPI_Op op, MPI_Comm comm, MPI_Request * request)
{
    MAP_OR_FAIL(PMPI_Ireduce_scatter);

    TIME(__real_PMPI_Ireduce_scatter(sendbuf, recvbuf, recvcounts,
                       datatype, op, comm, request));

    int i, tasks, num = 0;
    PMPI_Comm_size(comm, &tasks);
    for (i=0; i<tasks; i++) num += recvcounts[i];
    BYTECOUNT(datatype, num);

    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_IREDUCE_SCATTER);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Ireduce_scatter, int, (const void *sendbuf, void *recvbuf, const int recvcounts[],
                       MPI_Datatype datatype, MPI_Op op, MPI_Comm comm, MPI_Request * request), MPI_Ireduce_scatter)

int DARSHAN_DECL(MPI_Iscan)(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype,
             MPI_Op op, MPI_Comm comm, MPI_Request * request)
{
    MAP_OR_FAIL(PMPI_Iscan);

    TIME(__real_PMPI_Iscan(sendbuf, recvbuf, count, datatype, op, comm, request));
    
    BYTECOUNT(datatype, count);
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_ISCAN);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Iscan, int, (const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype,
             MPI_Op op, MPI_Comm comm, MPI_Request * request), MPI_Iscan)

int DARSHAN_DECL(MPI_Iexscan)(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype,
               MPI_Op op, MPI_Comm comm, MPI_Request * request)
{
    MAP_OR_FAIL(PMPI_Iexscan);

    TIME(__real_PMPI_Iexscan(sendbuf, recvbuf, count, datatype, op, comm, request));

    BYTECOUNT(datatype, count);
    APMPI_PRE_RECORD();
    APMPI_RECORD_UPDATE(MPI_IEXSCAN);
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_Iexscan, int, (const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype,
               MPI_Op op, MPI_Comm comm, MPI_Request * request), MPI_Iexscan)

/*
int DARSHAN_DECL(MPI_ )()
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_ );

    tm1 = APMPI_WTIME();
    ret = __real_PMPI_();
    tm2 = APMPI_WTIME();
    APMPI_PRE_RECORD();
    apmpi_runtime->perf_record->counters[MPI_ _COUNT]++;
    APMPI_POST_RECORD();
    return ret;
}
DARSHAN_WRAPPER_MAP(PMPI_ , int, (), MPI_ )
*/

#undef Y

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
