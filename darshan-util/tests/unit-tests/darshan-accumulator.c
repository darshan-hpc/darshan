/*
 * Copyright (C) 2022 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <stdio.h>
#include "munit/munit.h"

#include <darshan-logutils.h>

static MunitResult inject_shared_file_records(const MunitParameter params[], void* data);
static MunitResult inject_unique_file_records(const MunitParameter params[], void* data);
static void* test_context_setup(const MunitParameter params[], void* user_data);
static void test_context_tear_down(void *data);

static void posix_set_dummy_record(void* buffer);
static void posix_validate_double_dummy_record(void* buffer, struct darshan_derived_metrics* metrics, int shared_file_flag);
static void stdio_set_dummy_record(void* buffer);
static void stdio_validate_double_dummy_record(void* buffer, struct darshan_derived_metrics* metrics, int shared_file_flag);
static void mpiio_set_dummy_record(void* buffer);
static void mpiio_validate_double_dummy_record(void* buffer, struct darshan_derived_metrics* metrics, int shared_file_flag);


/* test definition */
static char* module_name_params[] = {"POSIX", "STDIO", "MPI-IO", NULL};

static MunitParameterEnum test_params[]
    = {{"module_name", module_name_params}, {NULL, NULL}};

static MunitTest tests[]
    = {{"/inject-shared-file-records", inject_shared_file_records,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE,
        test_params},
       {"/inject-unique-file-records", inject_unique_file_records,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE,
        test_params},
       {NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL}};

static const MunitSuite test_suite = {
    "/darshan-accumulator", tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};


/* function pointers for testing each module type */
void (*set_dummy_fn[DARSHAN_KNOWN_MODULE_COUNT])(void*) = {
    NULL, /* DARSHAN_NULL_MOD */
    posix_set_dummy_record, /* DARSHAN_POSIX_MOD */
    mpiio_set_dummy_record, /* DARSHAN_MPIIO_MOD */
    NULL, /* DARSHAN_H5F_MOD */
    NULL, /* DARSHAN_H5D_MOD */
    NULL, /* DARSHAN_PNETCDF_FILE_MOD */
    NULL, /* DARSHAN_PNETCDF_VAR_MOD */
    NULL, /* DARSHAN_BGQ_MOD */
    NULL, /* DARSHAN_LUSTRE_MOD */
    stdio_set_dummy_record, /* DARSHAN_STDIO_MOD */
    NULL, /* DXT_POSIX_MOD */
    NULL, /* DXT_MPIIO_MOD */
    NULL, /* DARSHAN_MDHIM_MOD */
    NULL, /* DARSHAN_APXC_MOD */
    NULL, /* DARSHAN_APMPI_MOD */
    NULL /* DARSHAN_HEATMAP_MOD */
};

void (*validate_double_dummy_fn[DARSHAN_KNOWN_MODULE_COUNT])(void*, struct darshan_derived_metrics*, int) = {
    NULL, /* DARSHAN_NULL_MOD */
    posix_validate_double_dummy_record, /* DARSHAN_POSIX_MOD */
    mpiio_validate_double_dummy_record, /* DARSHAN_MPIIO_MOD */
    NULL, /* DARSHAN_H5F_MOD */
    NULL, /* DARSHAN_H5D_MOD */
    NULL, /* DARSHAN_PNETCDF_FILE_MOD */
    NULL, /* DARSHAN_PNETCDF_VAR_MOD */
    NULL, /* DARSHAN_BGQ_MOD */
    NULL, /* DARSHAN_LUSTRE_MOD */
    stdio_validate_double_dummy_record, /* DARSHAN_STDIO_MOD */
    NULL, /* DXT_POSIX_MOD */
    NULL, /* DXT_MPIIO_MOD */
    NULL, /* DARSHAN_MDHIM_MOD */
    NULL, /* DARSHAN_APXC_MOD */
    NULL, /* DARSHAN_APMPI_MOD */
    NULL /* DARSHAN_HEATMAP_MOD */
};

struct test_context {
    darshan_module_id mod_id;
    struct darshan_mod_logutil_funcs* mod_fns;
};

static void* test_context_setup(const MunitParameter params[], void* user_data)
{
    (void) user_data;
    int i;
    int found = 0;

    const char* module_name = munit_parameters_get(params, "module_name");

    struct test_context* ctx = calloc(1, sizeof(*ctx));
    munit_assert_not_null(ctx);

    /* Look for module with this name and keep a reference to it's logutils
     * functions.
     */
    for(i=0; i<DARSHAN_KNOWN_MODULE_COUNT; i++) {
        if (strcmp(darshan_module_names[i], module_name) == 0) {
            ctx->mod_id = i;
            ctx->mod_fns = mod_logutils[i];
            found=1;
            break;
        }
    }
    munit_assert_int(found, ==, 1);

    return ctx;
}

static void test_context_tear_down(void *data)
{
    struct test_context *ctx = (struct test_context*)data;

    free(ctx);
}

/* test accumulating data on shared files (shared in the sense that multiple
 * ranks opened the same file but produced distinct records)
 */
static MunitResult inject_shared_file_records(const MunitParameter params[], void* data)
{
    struct test_context* ctx = (struct test_context*)data;
    int ret;
    darshan_accumulator acc;
    struct darshan_derived_metrics metrics;
    void* record1;
    void* record2;
    void* record_agg;
    struct darshan_base_record* base_rec;

    record1 = malloc(DEF_MOD_BUF_SIZE);
    munit_assert_not_null(record1);
    record2 = malloc(DEF_MOD_BUF_SIZE);
    munit_assert_not_null(record2);
    record_agg = malloc(DEF_MOD_BUF_SIZE);
    munit_assert_not_null(record_agg);

    /* make sure we have a function defined to set example records */
    munit_assert_not_null(set_dummy_fn[ctx->mod_id]);

    /* create example records, shared file but different ranks */
    set_dummy_fn[ctx->mod_id](record1);
    set_dummy_fn[ctx->mod_id](record2);
    base_rec = record2;
    base_rec->rank++;

    /**** shared file aggregation ****/

    ret = darshan_accumulator_create(ctx->mod_id, 4, &acc);
    munit_assert_int(ret, ==, 0);

    /* inject two example records */
    ret = darshan_accumulator_inject(acc, record1, 1);
    munit_assert_int(ret, ==, 0);
    ret = darshan_accumulator_inject(acc, record2, 1);
    munit_assert_int(ret, ==, 0);

    /* emit results */
    ret = darshan_accumulator_emit(acc, &metrics, record_agg);
    munit_assert_int(ret, ==, 0);

    /* sanity check */
    validate_double_dummy_fn[ctx->mod_id](record_agg, &metrics, 1);

    ret = darshan_accumulator_destroy(acc);
    munit_assert_int(ret, ==, 0);

    free(record1);
    free(record2);
    free(record_agg);

    return MUNIT_OK;
}

/* test accumulating data on unique files */
static MunitResult inject_unique_file_records(const MunitParameter params[], void* data)
{
    struct test_context* ctx = (struct test_context*)data;
    int ret;
    darshan_accumulator acc;
    struct darshan_derived_metrics metrics;
    void* record1;
    void* record2;
    void* record_agg;
    struct darshan_base_record* base_rec;

    record1 = malloc(DEF_MOD_BUF_SIZE);
    munit_assert_not_null(record1);
    record2 = malloc(DEF_MOD_BUF_SIZE);
    munit_assert_not_null(record2);
    record_agg = malloc(DEF_MOD_BUF_SIZE);
    munit_assert_not_null(record_agg);

    /* make sure we have a function defined to set example records */
    munit_assert_not_null(set_dummy_fn[ctx->mod_id]);

    /* create example records, shared file but different ranks */
    set_dummy_fn[ctx->mod_id](record1);
    set_dummy_fn[ctx->mod_id](record2);
    base_rec = record2;
    base_rec->rank++;

    /**** unique file aggregation ****/

    /* change id hash in one record */
    base_rec->id++;

    ret = darshan_accumulator_create(ctx->mod_id, 4, &acc);
    munit_assert_int(ret, ==, 0);

    /* inject two example records */
    ret = darshan_accumulator_inject(acc, record1, 1);
    munit_assert_int(ret, ==, 0);
    ret = darshan_accumulator_inject(acc, record2, 1);
    munit_assert_int(ret, ==, 0);

    /* emit results */
    ret = darshan_accumulator_emit(acc, &metrics, record_agg);
    munit_assert_int(ret, ==, 0);

    /* sanity check */
    validate_double_dummy_fn[ctx->mod_id](record_agg, &metrics, 0);

    ret = darshan_accumulator_destroy(acc);
    munit_assert_int(ret, ==, 0);

    free(record1);
    free(record2);
    free(record_agg);

    return MUNIT_OK;
}


int main(int argc, char **argv)
{
    return munit_suite_main(&test_suite, NULL, argc, argv);
}

/* Set example values for record of type posix.  As elsewhere in the
 * logutils API, the size of the buffer is implied.
 */
static void posix_set_dummy_record(void* buffer) {
    struct darshan_posix_file* pfile = buffer;

    /* This function must be updated (or at least checked) if the posix
     * module log format changes
     */
    munit_assert_int(DARSHAN_POSIX_VER, ==, 4);

    pfile->base_rec.id = 15574190512568163195UL;
    pfile->base_rec.rank = 0;

    pfile->counters[POSIX_OPENS] = 16;
    pfile->counters[POSIX_FILENOS] = 0;
    pfile->counters[POSIX_DUPS] = 0;
    pfile->counters[POSIX_READS] = 4;
    pfile->counters[POSIX_WRITES] = 4;
    pfile->counters[POSIX_SEEKS] = 0;
    pfile->counters[POSIX_STATS] = 0;
    pfile->counters[POSIX_MMAPS] = -1;
    pfile->counters[POSIX_FSYNCS] = 0;
    pfile->counters[POSIX_FDSYNCS] = 0;
    pfile->counters[POSIX_RENAME_SOURCES] = 0;
    pfile->counters[POSIX_RENAME_TARGETS] = 0;
    pfile->counters[POSIX_RENAMED_FROM] = 0;
    pfile->counters[POSIX_MODE] = 436;
    pfile->counters[POSIX_BYTES_READ] = 67108864;
    pfile->counters[POSIX_BYTES_WRITTEN] = 67108864;
    pfile->counters[POSIX_MAX_BYTE_READ] = 67108863;
    pfile->counters[POSIX_MAX_BYTE_WRITTEN] = 67108863;
    pfile->counters[POSIX_CONSEC_READS] = 0;
    pfile->counters[POSIX_CONSEC_WRITES] = 0;
    pfile->counters[POSIX_SEQ_READS] = 3;
    pfile->counters[POSIX_SEQ_WRITES] = 3;
    pfile->counters[POSIX_RW_SWITCHES] = 4;
    pfile->counters[POSIX_MEM_NOT_ALIGNED] = 0;
    pfile->counters[POSIX_MEM_ALIGNMENT] = 8;
    pfile->counters[POSIX_FILE_NOT_ALIGNED] = 0;
    pfile->counters[POSIX_FILE_ALIGNMENT] = 4096;
    pfile->counters[POSIX_MAX_READ_TIME_SIZE] = 16777216;
    pfile->counters[POSIX_MAX_WRITE_TIME_SIZE] = 16777216;
    pfile->counters[POSIX_SIZE_READ_0_100] = 0;
    pfile->counters[POSIX_SIZE_READ_100_1K] = 0;
    pfile->counters[POSIX_SIZE_READ_1K_10K] = 0;
    pfile->counters[POSIX_SIZE_READ_10K_100K] = 0;
    pfile->counters[POSIX_SIZE_READ_100K_1M] = 0;
    pfile->counters[POSIX_SIZE_READ_1M_4M] = 0;
    pfile->counters[POSIX_SIZE_READ_4M_10M] = 0;
    pfile->counters[POSIX_SIZE_READ_10M_100M] = 4;
    pfile->counters[POSIX_SIZE_READ_100M_1G] = 0;
    pfile->counters[POSIX_SIZE_READ_1G_PLUS] = 0;
    pfile->counters[POSIX_SIZE_WRITE_0_100] = 0;
    pfile->counters[POSIX_SIZE_WRITE_100_1K] = 0;
    pfile->counters[POSIX_SIZE_WRITE_1K_10K] = 0;
    pfile->counters[POSIX_SIZE_WRITE_10K_100K] = 0;
    pfile->counters[POSIX_SIZE_WRITE_100K_1M] = 0;
    pfile->counters[POSIX_SIZE_WRITE_1M_4M] = 0;
    pfile->counters[POSIX_SIZE_WRITE_4M_10M] = 0;
    pfile->counters[POSIX_SIZE_WRITE_10M_100M] = 4;
    pfile->counters[POSIX_SIZE_WRITE_100M_1G] = 0;
    pfile->counters[POSIX_SIZE_WRITE_1G_PLUS] = 0;
    pfile->counters[POSIX_STRIDE1_STRIDE] = 0;
    pfile->counters[POSIX_STRIDE2_STRIDE] = 0;
    pfile->counters[POSIX_STRIDE3_STRIDE] = 0;
    pfile->counters[POSIX_STRIDE4_STRIDE] = 0;
    pfile->counters[POSIX_STRIDE1_COUNT] = 0;
    pfile->counters[POSIX_STRIDE2_COUNT] = 0;
    pfile->counters[POSIX_STRIDE3_COUNT] = 0;
    pfile->counters[POSIX_STRIDE4_COUNT] = 0;
    pfile->counters[POSIX_ACCESS1_ACCESS] = 16777216;
    pfile->counters[POSIX_ACCESS2_ACCESS] = 0;
    pfile->counters[POSIX_ACCESS3_ACCESS] = 0;
    pfile->counters[POSIX_ACCESS4_ACCESS] = 0;
    pfile->counters[POSIX_ACCESS1_COUNT] = 8;
    pfile->counters[POSIX_ACCESS2_COUNT] = 0;
    pfile->counters[POSIX_ACCESS3_COUNT] = 0;
    pfile->counters[POSIX_ACCESS4_COUNT] = 0;
#if 0
    pfile->counters[POSIX_FASTEST_RANK] = 2;
    pfile->counters[POSIX_FASTEST_RANK_BYTES] = 33554432;
    pfile->counters[POSIX_SLOWEST_RANK] = 3;
    pfile->counters[POSIX_SLOWEST_RANK_BYTES] = 33554432;
#else
    /* this is a non-shared record; we don't expect fastest and slowest
     * fields to be set in this case */
    pfile->counters[POSIX_FASTEST_RANK] = 0;
    pfile->counters[POSIX_FASTEST_RANK_BYTES] = 0;
    pfile->counters[POSIX_SLOWEST_RANK] = 0;
    pfile->counters[POSIX_SLOWEST_RANK_BYTES] = 0;
#endif

    pfile->fcounters[POSIX_F_OPEN_START_TIMESTAMP] = 0.008787;
    pfile->fcounters[POSIX_F_READ_START_TIMESTAMP] = 0.079433;
    pfile->fcounters[POSIX_F_WRITE_START_TIMESTAMP] = 0.009389;
    pfile->fcounters[POSIX_F_CLOSE_START_TIMESTAMP] = 0.008901;
    pfile->fcounters[POSIX_F_OPEN_END_TIMESTAMP] = 0.079599;
    pfile->fcounters[POSIX_F_READ_END_TIMESTAMP] = 0.088423;
    pfile->fcounters[POSIX_F_WRITE_END_TIMESTAMP] = 0.042157;
    pfile->fcounters[POSIX_F_CLOSE_END_TIMESTAMP] = 0.088617;
    pfile->fcounters[POSIX_F_READ_TIME] = 0.030387;
    pfile->fcounters[POSIX_F_WRITE_TIME] = 0.082557;
    pfile->fcounters[POSIX_F_META_TIME] = 0.000177;
    pfile->fcounters[POSIX_F_MAX_READ_TIME] = 0.008990;
    pfile->fcounters[POSIX_F_MAX_WRITE_TIME] = 0.032618;
#if 0
    pfile->fcounters[POSIX_F_FASTEST_RANK_TIME] = 0.015122;
    pfile->fcounters[POSIX_F_SLOWEST_RANK_TIME] = 0.040990;
#else
    /* this is a non-shared record; we don't expect fastest and slowest
     * fields to be set in this case */
    pfile->fcounters[POSIX_F_FASTEST_RANK_TIME] = 0;
    pfile->fcounters[POSIX_F_SLOWEST_RANK_TIME] = 0;
#endif
    pfile->fcounters[POSIX_F_VARIANCE_RANK_TIME] = 0.000090;
    pfile->fcounters[POSIX_F_VARIANCE_RANK_BYTES] = 0.000000;

    return;
}

/* Set example values for record of type stdio.  As elsewhere in the
 * logutils API, the size of the buffer is implied.
 */
static void stdio_set_dummy_record(void* buffer) {
    struct darshan_stdio_file* sfile = buffer;

    /* This function must be updated (or at least checked) if the stdio
     * module log format changes
     */
    munit_assert_int(DARSHAN_STDIO_VER, ==, 2);

    sfile->base_rec.id = 552491373643638544UL;
    sfile->base_rec.rank = 0;

    sfile->counters[STDIO_OPENS] = 1;
    sfile->counters[STDIO_FDOPENS] = 0;
    sfile->counters[STDIO_READS] = 0;
    sfile->counters[STDIO_WRITES] = 1;
    sfile->counters[STDIO_SEEKS] = 0;
    sfile->counters[STDIO_FLUSHES] = 0;
    sfile->counters[STDIO_BYTES_WRITTEN] = 16777216;
    sfile->counters[STDIO_BYTES_READ] = 0;
    sfile->counters[STDIO_MAX_BYTE_READ] = 0;
    sfile->counters[STDIO_MAX_BYTE_WRITTEN] = 16777215;
    sfile->counters[STDIO_FASTEST_RANK] = 0;
    sfile->counters[STDIO_FASTEST_RANK_BYTES] = 0;
    sfile->counters[STDIO_SLOWEST_RANK] = 0;
    sfile->counters[STDIO_SLOWEST_RANK_BYTES] = 0;

    sfile->fcounters[STDIO_F_META_TIME] = 0.000126;
    sfile->fcounters[STDIO_F_WRITE_TIME] = 0.008228;
    sfile->fcounters[STDIO_F_READ_TIME] = 0.000000;
    sfile->fcounters[STDIO_F_OPEN_START_TIMESTAMP] = 0.043529;
    sfile->fcounters[STDIO_F_CLOSE_START_TIMESTAMP] = 0.052128;
    sfile->fcounters[STDIO_F_WRITE_START_TIMESTAMP] = 0.043750;
    sfile->fcounters[STDIO_F_READ_START_TIMESTAMP] = 0.000000;
    sfile->fcounters[STDIO_F_OPEN_END_TIMESTAMP] = 0.043636;
    sfile->fcounters[STDIO_F_CLOSE_END_TIMESTAMP] = 0.052148;
    sfile->fcounters[STDIO_F_WRITE_END_TIMESTAMP] = 0.051978;
    sfile->fcounters[STDIO_F_READ_END_TIMESTAMP] = 0.000000;
    sfile->fcounters[STDIO_F_FASTEST_RANK_TIME] = 0.000000;
    sfile->fcounters[STDIO_F_SLOWEST_RANK_TIME] = 0.000000;
    sfile->fcounters[STDIO_F_VARIANCE_RANK_TIME] = 0.000000;
    sfile->fcounters[STDIO_F_VARIANCE_RANK_BYTES] = 0.000000;

    return;
}

/* Set example values for record of type mpiio.  As elsewhere in the
 * logutils API, the size of the buffer is implied.
 */
static void mpiio_set_dummy_record(void* buffer)
{
    struct darshan_mpiio_file* mfile = buffer;

    /* This function must be updated (or at least checked) if the mpiio
     * module log format changes
     */
    munit_assert_int(DARSHAN_MPIIO_VER, ==, 3);

    mfile->base_rec.id = 15574190512568163195UL;
    mfile->base_rec.rank = 0;

    mfile->counters[MPIIO_INDEP_OPENS] = 8;
    mfile->counters[MPIIO_COLL_OPENS] = 0;
    mfile->counters[MPIIO_INDEP_READS] = 4;
    mfile->counters[MPIIO_INDEP_WRITES] = 4;
    mfile->counters[MPIIO_COLL_READS] = 0;
    mfile->counters[MPIIO_COLL_WRITES] = 0;
    mfile->counters[MPIIO_SPLIT_READS] = 0;
    mfile->counters[MPIIO_SPLIT_WRITES] = 0;
    mfile->counters[MPIIO_NB_READS] = 0;
    mfile->counters[MPIIO_NB_WRITES] = 0;
    mfile->counters[MPIIO_SYNCS] = 0;
    mfile->counters[MPIIO_HINTS] = 0;
    mfile->counters[MPIIO_VIEWS] = 0;
    mfile->counters[MPIIO_MODE] = 9;
    mfile->counters[MPIIO_BYTES_READ] = 67108864;
    mfile->counters[MPIIO_BYTES_WRITTEN] = 67108864;
    mfile->counters[MPIIO_RW_SWITCHES] = 4;
    mfile->counters[MPIIO_MAX_READ_TIME_SIZE] = 16777216;
    mfile->counters[MPIIO_MAX_WRITE_TIME_SIZE] = 16777216;
    mfile->counters[MPIIO_SIZE_READ_AGG_0_100] = 0;
    mfile->counters[MPIIO_SIZE_READ_AGG_100_1K] = 0;
    mfile->counters[MPIIO_SIZE_READ_AGG_1K_10K] = 0;
    mfile->counters[MPIIO_SIZE_READ_AGG_10K_100K] = 0;
    mfile->counters[MPIIO_SIZE_READ_AGG_100K_1M] = 0;
    mfile->counters[MPIIO_SIZE_READ_AGG_1M_4M] = 0;
    mfile->counters[MPIIO_SIZE_READ_AGG_4M_10M] = 0;
    mfile->counters[MPIIO_SIZE_READ_AGG_10M_100M] = 4;
    mfile->counters[MPIIO_SIZE_READ_AGG_100M_1G] = 0;
    mfile->counters[MPIIO_SIZE_READ_AGG_1G_PLUS] = 0;
    mfile->counters[MPIIO_SIZE_WRITE_AGG_0_100] = 0;
    mfile->counters[MPIIO_SIZE_WRITE_AGG_100_1K] = 0;
    mfile->counters[MPIIO_SIZE_WRITE_AGG_1K_10K] = 0;
    mfile->counters[MPIIO_SIZE_WRITE_AGG_10K_100K] = 0;
    mfile->counters[MPIIO_SIZE_WRITE_AGG_100K_1M] = 0;
    mfile->counters[MPIIO_SIZE_WRITE_AGG_1M_4M] = 0;
    mfile->counters[MPIIO_SIZE_WRITE_AGG_4M_10M] = 0;
    mfile->counters[MPIIO_SIZE_WRITE_AGG_10M_100M] = 4;
    mfile->counters[MPIIO_SIZE_WRITE_AGG_100M_1G] = 0;
    mfile->counters[MPIIO_SIZE_WRITE_AGG_1G_PLUS] = 0;
    mfile->counters[MPIIO_ACCESS1_ACCESS] = 16777216;
    mfile->counters[MPIIO_ACCESS2_ACCESS] = 0;
    mfile->counters[MPIIO_ACCESS3_ACCESS] = 0;
    mfile->counters[MPIIO_ACCESS4_ACCESS] = 0;
    mfile->counters[MPIIO_ACCESS1_COUNT] = 8;
    mfile->counters[MPIIO_ACCESS2_COUNT] = 0;
    mfile->counters[MPIIO_ACCESS3_COUNT] = 0;
    mfile->counters[MPIIO_ACCESS4_COUNT] = 0;
#if 0
    mfile->counters[MPIIO_FASTEST_RANK] = 2;
    mfile->counters[MPIIO_FASTEST_RANK_BYTES] = 33554432;
    mfile->counters[MPIIO_SLOWEST_RANK] = 3;
    mfile->counters[MPIIO_SLOWEST_RANK_BYTES] = 33554432;
#else
    /* should not be set for single-rank records */
    mfile->counters[MPIIO_FASTEST_RANK] = 0;
    mfile->counters[MPIIO_FASTEST_RANK_BYTES] = 0;
    mfile->counters[MPIIO_SLOWEST_RANK] = 0;
    mfile->counters[MPIIO_SLOWEST_RANK_BYTES] = 0;
#endif

    mfile->fcounters[MPIIO_F_OPEN_START_TIMESTAMP] = 0.006095;
    mfile->fcounters[MPIIO_F_READ_START_TIMESTAMP] = 0.079428;
    mfile->fcounters[MPIIO_F_WRITE_START_TIMESTAMP] = 0.009384;
    mfile->fcounters[MPIIO_F_CLOSE_START_TIMESTAMP] = 0.018622;
    mfile->fcounters[MPIIO_F_OPEN_END_TIMESTAMP] = 0.079620;
    mfile->fcounters[MPIIO_F_READ_END_TIMESTAMP] = 0.088598;
    mfile->fcounters[MPIIO_F_WRITE_END_TIMESTAMP] = 0.042630;
    mfile->fcounters[MPIIO_F_CLOSE_END_TIMESTAMP] = 0.089300;
    mfile->fcounters[MPIIO_F_READ_TIME] = 0.031077;
    mfile->fcounters[MPIIO_F_WRITE_TIME] = 0.084623;
    mfile->fcounters[MPIIO_F_META_TIME] = 0.020772;
    mfile->fcounters[MPIIO_F_MAX_READ_TIME] = 0.009170;
    mfile->fcounters[MPIIO_F_MAX_WRITE_TIME] = 0.033097;
#if 0
    mfile->fcounters[MPIIO_F_FASTEST_RANK_TIME] = 0.021213;
    mfile->fcounters[MPIIO_F_SLOWEST_RANK_TIME] = 0.046796;
    mfile->fcounters[MPIIO_F_VARIANCE_RANK_TIME] = 0.000087;
    mfile->fcounters[MPIIO_F_VARIANCE_RANK_BYTES] = 0.000000;
#else
    /* should not be set for single-rank records */
    mfile->fcounters[MPIIO_F_FASTEST_RANK_TIME] = 0;
    mfile->fcounters[MPIIO_F_SLOWEST_RANK_TIME] = 0;
    mfile->fcounters[MPIIO_F_VARIANCE_RANK_TIME] = 0;
    mfile->fcounters[MPIIO_F_VARIANCE_RANK_BYTES] = 0;
#endif

    return;
}

/* Validate that the aggregation produced sane values after being used to
 * aggregate 2 rank records.  If shared_file_flag, then the two records
 * refer to the same file (but from different ranks).  Otherwise the two
 * records refer to different files.
 */
static void posix_validate_double_dummy_record(void* buffer, struct darshan_derived_metrics* metrics, int shared_file_flag)
{
    struct darshan_posix_file* pfile = buffer;

    /* This function must be updated (or at least checked) if the posix
     * module log format changes
     */
    munit_assert_int(DARSHAN_POSIX_VER, ==, 4);

    /* check base record */
    if(shared_file_flag)
        munit_assert_int64(pfile->base_rec.id, ==, 15574190512568163195UL);
    else
        munit_assert_int64(pfile->base_rec.id, ==, 0);
    munit_assert_int64(pfile->base_rec.rank, ==, -1);

    /* double */
    munit_assert_int64(pfile->counters[POSIX_OPENS], ==, 32);
    /* stay set at -1 */
    munit_assert_int64(pfile->counters[POSIX_MMAPS], ==, -1);
    /* stay set */
    munit_assert_int64(pfile->counters[POSIX_MODE], ==, 436);

    /* "fastest" behavior should change depending on if records are shared
     * or not
     */
    if(shared_file_flag)
        /* it's a tie; either rank would be correct */
        munit_assert(pfile->counters[POSIX_FASTEST_RANK] == 0 || pfile->counters[POSIX_FASTEST_RANK] == 1);
    else
        munit_assert_int64(pfile->counters[POSIX_FASTEST_RANK], ==, -1);

    /* double */
    munit_assert_double_equal(pfile->fcounters[POSIX_F_READ_TIME], .060774, 6);

    /* variance should be cleared right now */
    munit_assert_int64(pfile->fcounters[POSIX_F_VARIANCE_RANK_TIME], ==, 0);

    /* check derived metrics */
    /* byte values should match regardless, just different values for count
     * depending on if the records refer to a shared file or not
     */
    if(shared_file_flag)
        munit_assert_int64(metrics->category_counters[DARSHAN_ALL_FILES].count, ==, 1);
    else
        munit_assert_int64(metrics->category_counters[DARSHAN_ALL_FILES].count, ==, 2);
    munit_assert_int64(metrics->category_counters[DARSHAN_ALL_FILES].total_read_volume_bytes, ==, 134217728);
    munit_assert_int64(metrics->category_counters[DARSHAN_ALL_FILES].max_offset_bytes, ==, 67108863);

    /* check aggregate perf.  Should be the same for both unique and
     * "shared" records, since both cases have the same amount of data and
     * time.  This is a good apex value to check because it combines several
     * other intermediate derived values into a final calculation.
     */
    munit_assert_double_equal(metrics->agg_perf_by_slowest, 2263.063445, 6);

    return;
}

/* Validate that the aggregation produced sane values after being used to
 * aggregate 2 rank records.  If shared_file_flag, then the two records
 * refer to the same file (but from different ranks).  Otherwise the two
 * records refer to different files.
 */
static void stdio_validate_double_dummy_record(void* buffer, struct darshan_derived_metrics* metrics, int shared_file_flag)
{
    struct darshan_stdio_file* sfile = buffer;

    /* This function must be updated (or at least checked) if the stdio
     * module log format changes
     */
    munit_assert_int(DARSHAN_STDIO_VER, ==, 2);

    /* check base record */
    if(shared_file_flag)
        munit_assert_int64(sfile->base_rec.id, ==, 552491373643638544UL);
    else
        munit_assert_int64(sfile->base_rec.id, ==, 0);
    munit_assert_int64(sfile->base_rec.rank, ==, -1);

    /* double */
    munit_assert_int64(sfile->counters[STDIO_OPENS], ==, 2);
    /* stay set */
    munit_assert_int64(sfile->counters[STDIO_MAX_BYTE_WRITTEN], ==, 16777215);
    /* double */
    munit_assert_int64(sfile->counters[STDIO_BYTES_WRITTEN], ==, 33554432);

    /* "fastest" behavior should change depending on if records are shared
     * or not
     */
    if(shared_file_flag)
        /* it's a tie; either rank would be correct */
        munit_assert(sfile->counters[STDIO_FASTEST_RANK] == 0 || sfile->counters[STDIO_FASTEST_RANK] == 1);
    else
        munit_assert_int64(sfile->counters[STDIO_FASTEST_RANK], ==, -1);

    /* double */
    munit_assert_double_equal(sfile->fcounters[STDIO_F_WRITE_TIME], .016456, 6);

    /* variance should be cleared right now */
    munit_assert_int64(sfile->fcounters[STDIO_F_VARIANCE_RANK_TIME], ==, 0);

    /* check derived metrics */
    /* byte values should match regardless, just different values for count
     * depending on if the records refer to a shared file or not
     */
    if(shared_file_flag)
        munit_assert_int64(metrics->category_counters[DARSHAN_ALL_FILES].count, ==, 1);
    else
        munit_assert_int64(metrics->category_counters[DARSHAN_ALL_FILES].count, ==, 2);
    munit_assert_int64(metrics->category_counters[DARSHAN_ALL_FILES].total_write_volume_bytes, ==, 33554432);
    munit_assert_int64(metrics->category_counters[DARSHAN_ALL_FILES].max_offset_bytes, ==, 16777215);

    /* check aggregate perf.  Should be the same for both unique and
     * "shared" records, since both cases have the same amount of data and
     * time.  This is a good apex value to check because it combines several
     * other intermediate derived values into a final calculation.
     */
    munit_assert_double_equal(metrics->agg_perf_by_slowest, 3830.500359, 6);

    return;
}

/* Validate that the aggregation produced sane values after being used to
 * aggregate 2 rank records.  If shared_file_flag, then the two records
 * refer to the same file (but from different ranks).  Otherwise the two
 * records refer to different files.
 */
static void mpiio_validate_double_dummy_record(void* buffer, struct darshan_derived_metrics* metrics, int shared_file_flag)
{
    struct darshan_mpiio_file* mfile = buffer;

    /* This function must be updated (or at least checked) if the mpiio
     * module log format changes
     */
    munit_assert_int(DARSHAN_MPIIO_VER, ==, 3);

    /* check base record */
    if(shared_file_flag)
        munit_assert_int64(mfile->base_rec.id, ==, 15574190512568163195UL);
    else
        munit_assert_int64(mfile->base_rec.id, ==, 0);
    munit_assert_int64(mfile->base_rec.rank, ==, -1);

    /* double */
    munit_assert_int64(mfile->counters[MPIIO_INDEP_OPENS], ==, 16);
    /* stay set */
    munit_assert_int64(mfile->counters[MPIIO_MODE], ==, 9);
    /* double */
    munit_assert_int64(mfile->counters[MPIIO_BYTES_WRITTEN], ==, 134217728);

    /* "fastest" behavior should change depending on if records are shared
     * or not
     */
    if(shared_file_flag)
        /* it's a tie; either rank would be correct */
        munit_assert(mfile->counters[MPIIO_FASTEST_RANK] == 0 || mfile->counters[MPIIO_FASTEST_RANK] == 1);
    else
        munit_assert_int64(mfile->counters[MPIIO_FASTEST_RANK], ==, -1);

    /* double */
    munit_assert_double_equal(mfile->fcounters[MPIIO_F_WRITE_TIME], .169246, 6);

    /* variance should be cleared right now */
    munit_assert_int64(mfile->fcounters[MPIIO_F_VARIANCE_RANK_TIME], ==, 0);

    /* check derived metrics */
    /* byte values should match regardless, just different values for count
     * depending on if the records refer to a shared file or not
     */
    if(shared_file_flag)
        munit_assert_int64(metrics->category_counters[DARSHAN_ALL_FILES].count, ==, 1);
    else
        munit_assert_int64(metrics->category_counters[DARSHAN_ALL_FILES].count, ==, 2);
    munit_assert_int64(metrics->category_counters[DARSHAN_ALL_FILES].total_write_volume_bytes, ==, 134217728);
    /* the mpiio module doesn't report max offsets */
    munit_assert_int64(metrics->category_counters[DARSHAN_ALL_FILES].max_offset_bytes, ==, -1);

    /* check aggregate perf.  Should be the same for both unique and
     * "shared" records, since both cases have the same amount of data and
     * time.  This is a good apex value to check because it combines several
     * other intermediate derived values into a final calculation.
     */
    munit_assert_double_equal(metrics->agg_perf_by_slowest, 1875.842663, 6);

    return;
}
