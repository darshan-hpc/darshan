/*
 * Copyright (C) 2017 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE
#define csJOBID_ENV_STR "PALS_APP_ID"

#include "darshan-runtime-config.h"
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <assert.h>
#include <papi.h>

#include "uthash.h"
#include "darshan.h"
#include "darshan-dynamic.h"
#include "darshan-apss-log-format.h"

#include "darshan-apss-utils.h"

/*
 * PAPI_events are defined by the Aries counters listed in the log header.
 */
#define X(a) #a,
#define Z(a) #a
static char* PAPI_events[] =
{
    APSS_PERF_COUNTERS
};
#undef X
#undef Z

#define MAX_GROUPS (128)
#define MAX_CHASSIS (MAX_GROUPS*6)
#define MAX_BLADES (MAX_CHASSIS*16)

/*
 * <Description>
 * 
 * This module does not intercept any system calls. It just pulls data
 * from the personality struct at initialization.
 */


/*
 * Global runtime struct for tracking data needed at runtime
 */
struct apss_runtime
{
    struct darshan_apss_header_record *header_record;
    struct darshan_apss_perf_record *perf_record;
    darshan_record_id header_id;
    darshan_record_id rtr_id;
    int PAPI_event_set;
    int PAPI_event_count;
    int group;
    int chassis;
    int blade;
    int node;
    int perf_record_marked;
};

static struct apss_runtime *apss_runtime = NULL;
static pthread_mutex_t apss_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

/* my_rank indicates the MPI rank of this process */
static int my_rank = -1;

/* internal helper functions for the APSS module */
void apss_runtime_initialize(void);

/* forward declaration for shutdown function needed to interface with darshan-core */
//#ifdef HAVE_MPI
static void apss_mpi_redux(
    void *buffer, 
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs, 
    int shared_rec_count);
//#endif
static void apss_output(
        void **buffer, 
        int *size);
static void apss_cleanup(
        void);

/* macros for obtaining/releasing the APSS module lock */
#define APSS_LOCK() pthread_mutex_lock(&apss_runtime_mutex)
#define APSS_UNLOCK() pthread_mutex_unlock(&apss_runtime_mutex)

/*
 * Initialize counters using PAPI
 */
static void initialize_counters (void)
{
    int i;
    int code = 0;

    PAPI_library_init(PAPI_VER_CURRENT);
    apss_runtime->PAPI_event_set = PAPI_NULL;
    PAPI_create_eventset(&apss_runtime->PAPI_event_set);

    /* start with first PAPI counter */
    for (i = AR_RTR_0_0_INQ_PRF_INCOMING_FLIT_VC0;
         strcmp(PAPI_events[i], "APSS_NUM_INDICES") != 0;
         i++)
    {
        PAPI_event_name_to_code(PAPI_events[i], &code);
        PAPI_add_event(apss_runtime->PAPI_event_set, code);
    }

    apss_runtime->PAPI_event_count = i;

    PAPI_start(apss_runtime->PAPI_event_set);

    return;
}

static void finalize_counters (void)
{
    PAPI_cleanup_eventset(apss_runtime->PAPI_event_set);
    PAPI_destroy_eventset(&apss_runtime->PAPI_event_set);
    PAPI_shutdown();

    return;
}

/*
 * Function which updates all the counter data
 */
static void capture(struct darshan_apss_perf_record *rec,
                    darshan_record_id rec_id)
{
    PAPI_stop(apss_runtime->PAPI_event_set,
          (long long*) &rec->counters[AR_RTR_0_0_INQ_PRF_INCOMING_FLIT_VC0]);
    PAPI_reset(apss_runtime->PAPI_event_set);

    rec->group   = apss_runtime->group;
    rec->chassis = apss_runtime->chassis;
    rec->blade   = apss_runtime->blade;
    rec->node    = apss_runtime->node;
    rec->base_rec.id = rec_id;
    rec->base_rec.rank = my_rank;

    return;
}

void apss_runtime_initialize()
{
    size_t apss_buf_size;
    char rtr_rec_name[128];

    darshan_module_funcs mod_funcs = {
//#ifdef HAVE_MPI
        .mod_redux_func = &apss_mpi_redux,
//#endif
        .mod_output_func = &apss_output,
        .mod_cleanup_func = &apss_cleanup
        };

    APSS_LOCK();
    

    /* don't do anything if already initialized */
    if(apss_runtime)
    {
        APSS_UNLOCK();
        return;
    }


    apss_buf_size = sizeof(struct darshan_apss_header_record) + 
                    sizeof(struct darshan_apss_perf_record);

    /* register the APSS module with the darshan-core component */
    darshan_core_register_module(
        DARSHAN_APSS_MOD,
        mod_funcs,
        &apss_buf_size,
        &my_rank,
        NULL);


    /* initialize module's global state */
    apss_runtime = malloc(sizeof(*apss_runtime));
    if(!apss_runtime)
    {
        darshan_core_unregister_module(DARSHAN_APSS_MOD);
        APSS_UNLOCK();
        return;
    }
    memset(apss_runtime, 0, sizeof(*apss_runtime));

    if (my_rank == 0)
    {
        apss_runtime->header_id = darshan_core_gen_record_id("darshan-apss-header");

        /* register the apss file record with darshan-core */
        apss_runtime->header_record = darshan_core_register_record(
            apss_runtime->header_id,
            //NULL,
            "darshan-apss-header",
            DARSHAN_APSS_MOD,
            sizeof(struct darshan_apss_header_record),
            NULL);
        if(!(apss_runtime->header_record))
        {
            darshan_core_unregister_module(DARSHAN_APSS_MOD);
            free(apss_runtime);
            apss_runtime = NULL;
            APSS_UNLOCK();
           return;
        }
        apss_runtime->header_record->base_rec.id = apss_runtime->header_id;
        apss_runtime->header_record->base_rec.rank = my_rank;
        apss_runtime->header_record->magic = APSS_MAGIC;
    }

    get_xc_coords(&apss_runtime->group,
                  &apss_runtime->chassis,
                  &apss_runtime->blade,
                  &apss_runtime->node);

    sprintf(rtr_rec_name, "darshan-apss-rtr-%d-%d-%d",
            apss_runtime->group, apss_runtime->chassis, apss_runtime->blade);
    //apss_runtime->rtr_id = darshan_core_gen_record_id(rtr_rec_name);
    apss_runtime->rtr_id = darshan_core_gen_record_id("APSS");
    apss_runtime->perf_record = darshan_core_register_record(
        apss_runtime->rtr_id,
        //NULL,
        "APSS",   // we want the record for each rank to be treated as shared records so that mpi_redux can operate on
        //rtr_rec_name,
        DARSHAN_APSS_MOD,
        sizeof(struct darshan_apss_perf_record),
        NULL);
    if(!(apss_runtime->perf_record))
    {
        darshan_core_unregister_module(DARSHAN_APSS_MOD);
        free(apss_runtime);
        apss_runtime = NULL;
        APSS_UNLOCK();
        return;
    }

    initialize_counters();
    APSS_UNLOCK();

    return;
}

/********************************************************************************
 * shutdown function exported by this module for coordinating with darshan-core *
 ********************************************************************************/

/* Pass data for the apss module back to darshan-core to log to file. */
//#ifdef HAVE_MPI
static void apss_mpi_redux(
    void *apss_buf,
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count)
{
    int i;
    int color;
    int router_rank;
    int router_count;
    int chassis_count;
    int group_count;
    unsigned int *bitvec;
    unsigned int bitlen;
    unsigned int bitcnt;
    unsigned int bitsiz;
    MPI_Comm router_comm;

    APSS_LOCK();
    if (!apss_runtime)
    {
        APSS_UNLOCK();
        return;
    }

    bitcnt = sizeof(unsigned int) * 8;
    bitlen = sizeof(unsigned int) * (MAX_BLADES/bitcnt);
    bitsiz = bitlen / sizeof(unsigned int);
    bitvec = malloc(bitlen);
    
    /* collect perf counters */
    capture(apss_runtime->perf_record, apss_runtime->rtr_id);

    if (my_rank == 0)
    {
        apss_runtime->header_record->appid = atoi((char*)getenv( csJOBID_ENV_STR ));
    }

    /* count network dimensions */
    if (bitvec)
    {
        int idx;
        unsigned int uchassis;
        unsigned int ublade;

        /* group */
        memset(bitvec, 0, bitlen);
        idx = apss_runtime->group / bitcnt;
        bitvec[idx] |= (1 << apss_runtime->group % bitcnt);
        PMPI_Reduce((my_rank ? bitvec : MPI_IN_PLACE),
                     bitvec,
                     bitsiz,
                     MPI_INT,
                     MPI_BOR,
                     0,
                     MPI_COMM_WORLD);
        group_count = count_bits(bitvec, bitsiz);

        /* chassis */
        memset(bitvec, 0, bitlen);
        uchassis = apss_runtime->group * 6 + apss_runtime->chassis;
        idx = uchassis / bitcnt;
        bitvec[idx] |= (1 << uchassis % bitcnt);
        PMPI_Reduce((my_rank ? bitvec : MPI_IN_PLACE),
                    bitvec,
                    bitsiz,
                    MPI_INT,
                    MPI_BOR,
                    0,
                    MPI_COMM_WORLD);
        chassis_count = count_bits(bitvec, bitsiz);

        /* blade */
        memset(bitvec, 0, bitlen);
        ublade = uchassis * 16 + apss_runtime->blade;
        idx = ublade / bitcnt;
        bitvec[idx] |= (1 << ublade % bitcnt);
        PMPI_Reduce((my_rank ? bitvec : MPI_IN_PLACE),
                    bitvec,
                    bitsiz,
                    MPI_INT,
                    MPI_BOR,
                    0,
                    MPI_COMM_WORLD);
        router_count = count_bits(bitvec, bitsiz);

        if (my_rank == 0)
        {
            apss_runtime->header_record->nblades  = router_count;
            apss_runtime->header_record->nchassis = chassis_count;
            apss_runtime->header_record->ngroups  = group_count;
        }
        free(bitvec);
    }
    else
    {
        apss_runtime->header_record->nblades  = 0;
        apss_runtime->header_record->nchassis = 0;
        apss_runtime->header_record->ngroups  = 0;
    }
    
    /*
     * reduce data
     *
     *  aggregate data from processes which share the same blade and avg.
     *  
     */ 
    color = (apss_runtime->group << (4+3)) + \
            (apss_runtime->chassis << 4) + \
            apss_runtime->blade;
    PMPI_Comm_split(MPI_COMM_WORLD, color, my_rank, &router_comm);
    PMPI_Comm_rank(router_comm,  &router_rank);
    PMPI_Comm_size(router_comm,  &router_count);

    PMPI_Reduce((router_rank?apss_runtime->perf_record->counters:MPI_IN_PLACE),
                apss_runtime->perf_record->counters,
                APSS_NUM_INDICES,
                MPI_LONG_LONG_INT,
                MPI_SUM,
                0,
                router_comm);

    if (router_rank == 0)
    {
        for (i = 0; i < APSS_NUM_INDICES; i++)
        {
            apss_runtime->perf_record->counters[i] /= router_count;
        }
            apss_runtime->perf_record_marked = -1;
    }
    PMPI_Comm_free(&router_comm);

    APSS_UNLOCK();

    return;
}

//#endif
static void apss_output(
    void **apss_buf,
    int *apss_buf_sz)
{
    APSS_LOCK();
    assert(apss_runtime);
    *apss_buf_sz = 0; 
    
    if (my_rank == 0) { 
        *apss_buf_sz += sizeof(*apss_runtime->header_record); 
    }
    
    if (apss_runtime->perf_record_marked == -1) 
     { 
       *apss_buf_sz += sizeof( *apss_runtime->perf_record); 
     }

    APSS_UNLOCK();
    return;
}

static void apss_cleanup()
{
    APSS_LOCK();
    assert(apss_runtime);
    finalize_counters();
    free(apss_runtime);
    apss_runtime = NULL;
    APSS_UNLOCK();
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
