/*
 * Copyright (C) 2016 Intel Corporation.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#define _GNU_SOURCE
#include "darshan-util-config.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include "darshan-logutils.h"

static int dxlt_log_get_posix_file(darshan_fd fd, void** dxlt_posix_buf);
static int dxlt_log_put_posix_file(darshan_fd fd, void* dxlt_posix_buf);
static void dxlt_log_print_posix_file(void *file_rec, char *file_name,
        char *mnt_pt, char *fs_type, struct darshan_name_record_ref *ref);
static void dxlt_log_print_posix_description(int ver);
static void dxlt_log_print_posix_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2);
static void dxlt_log_agg_posix_files(void *rec, void *agg_rec, int init_flag);

static int dxlt_log_get_mpiio_file(darshan_fd fd, void** dxlt_mpiio_buf);
static int dxlt_log_put_mpiio_file(darshan_fd fd, void* dxlt_mpiio_buf);
static void dxlt_log_print_mpiio_file(void *file_rec,
        char *file_name, char *mnt_pt, char *fs_type);
static void dxlt_log_print_mpiio_description(int ver);
static void dxlt_log_print_mpiio_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2);
static void dxlt_log_agg_mpiio_files(void *rec, void *agg_rec, int init_flag);

static void *parser_buf = NULL;
static int64_t parser_buf_sz = 0;

struct darshan_mod_logutil_funcs dxlt_posix_logutils =
{
    .log_get_record = &dxlt_log_get_posix_file,
    .log_put_record = &dxlt_log_put_posix_file,
    .log_print_record_dxlt = &dxlt_log_print_posix_file,
//    .log_print_description = &dxlt_log_print_posix_description,
    .log_print_diff = &dxlt_log_print_posix_file_diff,
    .log_agg_records = &dxlt_log_agg_posix_files,
};

struct darshan_mod_logutil_funcs dxlt_mpiio_logutils =
{
    .log_get_record = &dxlt_log_get_mpiio_file,
    .log_put_record = &dxlt_log_put_mpiio_file,
    .log_print_record = &dxlt_log_print_mpiio_file,
//    .log_print_description = &dxlt_log_print_mpiio_description,
    .log_print_diff = &dxlt_log_print_mpiio_file_diff,
    .log_agg_records = &dxlt_log_agg_mpiio_files,
};

void dxlt_swap_file_record(struct dxlt_file_record *file_rec)
{
    DARSHAN_BSWAP64(&file_rec->base_rec.id);
    DARSHAN_BSWAP64(&file_rec->base_rec.rank);
    DARSHAN_BSWAP64(&file_rec->shared_record);

    DARSHAN_BSWAP64(&file_rec->write_count);
    DARSHAN_BSWAP64(&file_rec->read_count);

    DARSHAN_BSWAP64(&file_rec->write_traces);
    DARSHAN_BSWAP64(&file_rec->read_traces);
}

static int dxlt_log_get_posix_file(darshan_fd fd, void** dxlt_posix_buf)
{
    struct dxlt_file_record *file_rec;
    int i, ret;
    int64_t io_trace_size;

    ret = darshan_log_get_mod(fd, DXLT_POSIX_MOD, *dxlt_posix_buf,
                sizeof(struct dxlt_file_record));
    if(ret < 0)
        return (-1);
    else if(ret < sizeof(struct dxlt_file_record))
        return (0);
    else
    {
        file_rec = (struct dxlt_file_record *)(*dxlt_posix_buf);
        if (fd->swap_flag)
        {
            /* swap bytes if necessary */
            dxlt_swap_file_record(file_rec);
        }
    }

    io_trace_size = (file_rec->write_count + file_rec->read_count) *
                        sizeof(segment_info);

    if (parser_buf_sz == 0) {
        parser_buf = (void *)malloc(io_trace_size);
        parser_buf_sz = io_trace_size;
    } else {
        if (parser_buf_sz < io_trace_size) {
            parser_buf = (void *)realloc(parser_buf, io_trace_size);
            parser_buf_sz = io_trace_size;
        }
    }

    ret = darshan_log_get_mod(fd, DXLT_POSIX_MOD, parser_buf,
                io_trace_size);

    if  (ret < 0) {
        return (-1);
    } else {
        if (ret < io_trace_size) {
            return (0);
        } else {
            return (1);
        }
    }
}

static int dxlt_log_get_mpiio_file(darshan_fd fd, void** dxlt_mpiio_buf)
{
    struct dxlt_file_record *file_rec;
    int i, ret;
    int64_t io_trace_size;

    ret = darshan_log_get_mod(fd, DXLT_MPIIO_MOD, *dxlt_mpiio_buf,
        sizeof(struct dxlt_file_record));
    if(ret < 0)
        return(-1);
    else if(ret < sizeof(struct dxlt_file_record))
        return(0);
    else
    {
        file_rec = (struct dxlt_file_record *)(*dxlt_mpiio_buf);
        if (fd->swap_flag)
        {
            /* swap bytes if necessary */
            dxlt_swap_file_record(file_rec);
        }
    }

    io_trace_size = (file_rec->write_count + file_rec->read_count) *
                        sizeof(segment_info);

    if (parser_buf_sz == 0) {
        parser_buf = (void *)malloc(io_trace_size);
        parser_buf_sz = io_trace_size;
    } else {
        if (parser_buf_sz < io_trace_size) {
            parser_buf = (void *)realloc(parser_buf, io_trace_size);
            parser_buf_sz = io_trace_size;
        }
    }

    ret = darshan_log_get_mod(fd, DXLT_MPIIO_MOD, parser_buf,
                io_trace_size);

    if  (ret < 0) {
        return (-1);
    } else {
        if (ret < io_trace_size) {
            return (0);
        } else {
            return (1);
        }
    }
}

static int dxlt_log_put_posix_file(darshan_fd fd, void* dxlt_posix_buf)
{
    struct dxlt_file_record *file_rec =
                (struct dxlt_file_record *)dxlt_posix_buf;
    int ret;

    ret = darshan_log_put_mod(fd, DXLT_POSIX_MOD, file_rec,
                sizeof(struct dxlt_file_record), DXLT_POSIX_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

static int dxlt_log_put_mpiio_file(darshan_fd fd, void* dxlt_mpiio_buf)
{
    struct dxlt_file_record *file_rec =
                (struct dxlt_file_record *)dxlt_mpiio_buf;
    int ret;

    ret = darshan_log_put_mod(fd, DXLT_MPIIO_MOD, file_rec,
                sizeof(struct dxlt_file_record), DXLT_MPIIO_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

static void dxlt_log_print_posix_file(void *posix_file_rec, char *file_name,
    char *mnt_pt, char *fs_type, struct darshan_name_record_ref *ref)
{
    struct dxlt_file_record *file_rec =
                (struct dxlt_file_record *)posix_file_rec;
    int64_t offset;
    int64_t length;
    double start_time;
    double end_time;
    int i;

    darshan_record_id f_id = file_rec->base_rec.id;
    int64_t rank = file_rec->base_rec.rank;

    int64_t write_count = file_rec->write_count;
    int64_t read_count = file_rec->read_count;
    segment_info *io_trace = (segment_info *)parser_buf;

    /* Lustre File System */
    int lustreFS = !strcmp(fs_type, "lustre");
    int32_t stripe_size;
    int32_t stripe_count;
    int64_t cur_offset;
    int print_count;
    int ost_idx;
    

    printf("\n# DXLT, file_id: %" PRIu64 ", file_name: %s\n", f_id, file_name);
    printf("# DXLT, rank: %d, write_count: %d, read_count: %d\n",
                rank, write_count, read_count);

    if (lustreFS) {
        stripe_size = ref->stripe_size;
        stripe_count = ref->stripe_count;

        printf("# DXLT, mnt_pt: %s, fs_type: %s\n", mnt_pt, fs_type);
        printf("# DXLT, stripe_size: %d, stripe_count: %d\n", stripe_size, stripe_count);
        for (i = 0; i < ref->stripe_count; i++) {
            printf("# DXLT, OST: %d\n", (ref->ost_ids)[i]);
        }
    }

    /* Print header */
    printf("# Module    Rank  Wt/Rd  Segment          Offset       Length    Start(s)      End(s)");

    if (lustreFS) {
        printf("  [OST]");
    }
    printf("\n");

    /* Print IO Traces information */
    for (i = 0; i < write_count; i++) {
        offset = io_trace[i].offset;
        length = io_trace[i].length;
        start_time = io_trace[i].start_time;
        end_time = io_trace[i].end_time;

        printf("%8s%8d%7s%9lld%16lld%13lld%12.4f%12.4f", "X_POSIX", rank, "write", i, offset, length, start_time, end_time);

        if (lustreFS) {
            cur_offset = offset;
            ost_idx = (offset / stripe_size) % stripe_count;

            print_count = 0;
            while (cur_offset < offset + length) {
                printf("  [%3d]", (ref->ost_ids)[ost_idx]);

                cur_offset = (cur_offset / stripe_size + 1) * stripe_size;
                ost_idx = (ost_idx == stripe_count - 1) ? 0 : ost_idx + 1;

                print_count++;
                if (print_count >= stripe_count)
                    break;
            }
        }

        printf("\n");
    }

    for (i = write_count; i < write_count + read_count; i++) {
        offset = io_trace[i].offset;
        length = io_trace[i].length;
        start_time = io_trace[i].start_time;
        end_time = io_trace[i].end_time;

        printf("%8s%8d%7s%9lld%16lld%13lld%12.4f%12.4f", "X_POSIX", rank, "read", i - write_count, offset, length, start_time, end_time);

        if (lustreFS) {
            cur_offset = offset;
            ost_idx = (offset / stripe_size) % stripe_count;

            print_count = 0;
            while (cur_offset < offset + length) {
                printf("  [%3d]", (ref->ost_ids)[ost_idx]);

                cur_offset = (cur_offset / stripe_size + 1) * stripe_size;
                ost_idx = (ost_idx == stripe_count - 1) ? 0 : ost_idx + 1;

                print_count++;
                if (print_count >= stripe_count)
                    break;
            }
        }

        printf("\n");
    }
    return;
}

static void dxlt_log_print_mpiio_file(void *mpiio_file_rec, char *file_name,
    char *mnt_pt, char *fs_type)
{
    struct dxlt_file_record *file_rec =
                (struct dxlt_file_record *)mpiio_file_rec;

    int64_t offset;
    int64_t length;
    double start_time;
    double end_time;
    int i;

    darshan_record_id f_id = file_rec->base_rec.id;
    int64_t rank = file_rec->base_rec.rank;

    int64_t write_count = file_rec->write_count;
    int64_t read_count = file_rec->read_count;

    segment_info *io_trace = (segment_info *)parser_buf;

    printf("\n# DXLT, file_id: %" PRIu64 ", file_name: %s\n", f_id, file_name);
    printf("# DXLT, rank: %d, write_count: %d, read_count: %d\n",
                rank, write_count, read_count);

    printf("# DXLT, mnt_pt: %s, fs_type: %s\n", mnt_pt, fs_type);

    /* Print header */
    printf("# Module    Rank  Wt/Rd  Segment       Length    Start(s)      End(s)\n");

    /* Print IO Traces information */
    for (i = 0; i < write_count; i++) {
        offset = io_trace[i].offset;
        length = io_trace[i].length;
        start_time = io_trace[i].start_time;
        end_time = io_trace[i].end_time;

        printf("%8s%8d%7s%9lld%13lld%12.4f%12.4f\n", "X_MPIIO", rank, "write", i, length, start_time, end_time);
    }

    for (i = write_count; i < write_count + read_count; i++) {
        offset = io_trace[i].offset;
        length = io_trace[i].length;
        start_time = io_trace[i].start_time;
        end_time = io_trace[i].end_time;

        printf("%8s%8d%7s%9lld%13lld%12.4f%12.4f\n", "X_MPIIO", rank, "read", i - write_count, length, start_time, end_time);
    }

    return;
}

static void dxlt_log_print_posix_description(int ver)
{
}

static void dxlt_log_print_mpiio_description(int ver)
{
}

static void dxlt_log_print_posix_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2)
{
}

static void dxlt_log_print_mpiio_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2)
{
}

static void dxlt_log_agg_posix_files(void *rec, void *agg_rec, int init_flag)
{
}

static void dxlt_log_agg_mpiio_files(void *rec, void *agg_rec, int init_flag)
{
}

void dxlt_logutils_cleanup()
{
    if (parser_buf_sz != 0 || parser_buf) {
        free(parser_buf);
    }

    parser_buf_sz = 0;
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
