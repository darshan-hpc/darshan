/*
 * Copyright (C) 2016 Intel Corporation.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifdef HAVE_CONFIG_H
# include "darshan-util-config.h"
#endif

#define _GNU_SOURCE
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

static int dxt_log_get_posix_file(darshan_fd fd, void** dxt_posix_buf_p);
static int dxt_log_put_posix_file(darshan_fd fd, void* dxt_posix_buf);

static int dxt_log_get_mpiio_file(darshan_fd fd, void** dxt_mpiio_buf_p);
static int dxt_log_put_mpiio_file(darshan_fd fd, void* dxt_mpiio_buf);

static void dxt_log_print_posix_file_darshan(void *file_rec,
            char *file_name, char *mnt_pt, char *fs_type);
static void dxt_log_print_mpiio_file_darshan(void *file_rec,
            char *file_name, char *mnt_pt, char *fs_type);

static void dxt_swap_file_record(struct dxt_file_record *file_rec);
static void dxt_swap_file_record(struct dxt_file_record *file_rec);

struct darshan_mod_logutil_funcs dxt_posix_logutils =
{
    .log_get_record = &dxt_log_get_posix_file,
    .log_put_record = &dxt_log_put_posix_file,
    .log_print_record = &dxt_log_print_posix_file_darshan,
    .log_print_description = NULL,
    .log_print_diff = NULL,
    .log_agg_records = NULL,
};

struct darshan_mod_logutil_funcs dxt_mpiio_logutils =
{
    .log_get_record = &dxt_log_get_mpiio_file,
    .log_put_record = &dxt_log_put_mpiio_file,
    .log_print_record = &dxt_log_print_mpiio_file_darshan,
    .log_print_description = NULL,
    .log_print_diff = NULL,
    .log_agg_records = NULL,
};

static void dxt_swap_file_record(struct dxt_file_record *file_rec)
{
    DARSHAN_BSWAP64(&file_rec->base_rec.id);
    DARSHAN_BSWAP64(&file_rec->base_rec.rank);
    DARSHAN_BSWAP64(&file_rec->shared_record);
    DARSHAN_BSWAP64(&file_rec->write_count);
    DARSHAN_BSWAP64(&file_rec->read_count);
}

static void dxt_swap_segments(struct dxt_file_record *file_rec)
{
    int i;
    segment_info *tmp_seg;

    tmp_seg = (segment_info *)((void *)file_rec + sizeof(struct dxt_file_record));
    for(i = 0; i < (file_rec->write_count + file_rec->read_count); i++)
    {
        DARSHAN_BSWAP64(&tmp_seg->offset);
        DARSHAN_BSWAP64(&tmp_seg->length);
        DARSHAN_BSWAP64(&tmp_seg->start_time);
        DARSHAN_BSWAP64(&tmp_seg->end_time);
        tmp_seg++;
    }
}

static int dxt_log_get_posix_file(darshan_fd fd, void** dxt_posix_buf_p)
{
    struct dxt_file_record *rec = *((struct dxt_file_record **)dxt_posix_buf_p);
    struct dxt_file_record tmp_rec;
    int ret;
    int64_t io_trace_size;

    if(fd->mod_map[DXT_POSIX_MOD].len == 0)
        return(0);

    if(fd->mod_ver[DXT_POSIX_MOD] == 0 ||
        fd->mod_ver[DXT_POSIX_MOD] > DXT_POSIX_VER)
    {
        fprintf(stderr, "Error: Invalid DXT POSIX module version number (got %d)\n",
            fd->mod_ver[DXT_POSIX_MOD]);
        return(-1);
    }

    ret = darshan_log_get_mod(fd, DXT_POSIX_MOD, &tmp_rec,
                sizeof(struct dxt_file_record));
    if(ret < 0)
        return (-1);
    else if(ret < sizeof(struct dxt_file_record))
        return (0);

    if (fd->swap_flag)
    {
        /* swap bytes if necessary */
        dxt_swap_file_record(&tmp_rec);
    }

    io_trace_size = (tmp_rec.write_count + tmp_rec.read_count) *
                        sizeof(segment_info);

    if (*dxt_posix_buf_p == NULL)
    {
        rec = malloc(sizeof(struct dxt_file_record) + io_trace_size);
        if (!rec)
            return(-1);
    }
    memcpy(rec, &tmp_rec, sizeof(struct dxt_file_record));

    if (io_trace_size > 0)
    {
        void *tmp_p = (void *)rec + sizeof(struct dxt_file_record);

        ret = darshan_log_get_mod(fd, DXT_POSIX_MOD, tmp_p,
                    io_trace_size);
        if (ret < io_trace_size)
            ret = -1;
        else
        {
            ret = 1;
            if(fd->swap_flag)
            {
                /* byte swap trace data if necessary */
                dxt_swap_segments(rec);
            }
        }
    }
    else
    {
        ret = 1;
    }

    if(*dxt_posix_buf_p == NULL)
    {   
        if(ret == 1)
            *dxt_posix_buf_p = rec;
        else
            free(rec);
    }

    return(ret);
}

static int dxt_log_get_mpiio_file(darshan_fd fd, void** dxt_mpiio_buf_p)
{
    struct dxt_file_record *rec = *((struct dxt_file_record **)dxt_mpiio_buf_p);
    struct dxt_file_record tmp_rec;
    int i;
    int ret;
    int64_t io_trace_size;

    if(fd->mod_map[DXT_MPIIO_MOD].len == 0)
        return(0);

    if(fd->mod_ver[DXT_MPIIO_MOD] == 0 ||
        fd->mod_ver[DXT_MPIIO_MOD] > DXT_MPIIO_VER)
    {
        fprintf(stderr, "Error: Invalid DXT MPIIO module version number (got %d)\n",
            fd->mod_ver[DXT_MPIIO_MOD]);
        return(-1);
    }

    ret = darshan_log_get_mod(fd, DXT_MPIIO_MOD, &tmp_rec,
                sizeof(struct dxt_file_record));
    if(ret < 0)
        return (-1);
    else if(ret < sizeof(struct dxt_file_record))
        return (0);

    if (fd->swap_flag)
    {
        /* swap bytes if necessary */
        dxt_swap_file_record(&tmp_rec);
    }

    io_trace_size = (tmp_rec.write_count + tmp_rec.read_count) *
                        sizeof(segment_info);

    if (*dxt_mpiio_buf_p == NULL)
    {
        rec = malloc(sizeof(struct dxt_file_record) + io_trace_size);
        if (!rec)
            return(-1);
    }
    memcpy(rec, &tmp_rec, sizeof(struct dxt_file_record));

    if (io_trace_size > 0)
    {
        void *tmp_p = (void *)rec + sizeof(struct dxt_file_record);

        ret = darshan_log_get_mod(fd, DXT_MPIIO_MOD, tmp_p,
                    io_trace_size);
        if (ret < io_trace_size)
            ret = -1;
        else
        {
            ret = 1;
            if(fd->swap_flag)
            {
                /* byte swap trace data if necessary */
                dxt_swap_segments(rec);
            }

            if(fd->mod_ver[DXT_MPIIO_MOD] == 1)
            {
                /* make sure to indicate offsets are invalid in version 1 */
                for(i = 0; i < (tmp_rec.write_count + tmp_rec.read_count); i++)
                {
                    ((segment_info *)tmp_p)[i].offset = -1;
                }
            }
        }
    }
    else
    {
        ret = 1;
    }

    if(*dxt_mpiio_buf_p == NULL)
    {
        if(ret == 1)
            *dxt_mpiio_buf_p = rec;
        else
            free(rec);
    }

    return(ret);
}

static int dxt_log_put_posix_file(darshan_fd fd, void* dxt_posix_buf)
{
    struct dxt_file_record *file_rec =
                (struct dxt_file_record *)dxt_posix_buf;
    int rec_size = sizeof(struct dxt_file_record) + (sizeof(segment_info) * 
        (file_rec->write_count + file_rec->read_count));
    int ret;

    ret = darshan_log_put_mod(fd, DXT_POSIX_MOD, file_rec,
                rec_size, DXT_POSIX_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

static int dxt_log_put_mpiio_file(darshan_fd fd, void* dxt_mpiio_buf)
{
    struct dxt_file_record *file_rec =
                (struct dxt_file_record *)dxt_mpiio_buf;
    int rec_size = sizeof(struct dxt_file_record) + (sizeof(segment_info) * 
        (file_rec->write_count + file_rec->read_count));
    int ret;

    ret = darshan_log_put_mod(fd, DXT_MPIIO_MOD, file_rec,
                rec_size, DXT_MPIIO_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

static void dxt_log_print_posix_file_darshan(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type)
{
}

static void dxt_log_print_mpiio_file_darshan(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type)
{
}

void dxt_log_print_posix_file(void *posix_file_rec, char *file_name,
    char *mnt_pt, char *fs_type, struct lustre_record_ref *lustre_rec_ref)
{
    struct dxt_file_record *file_rec =
                (struct dxt_file_record *)posix_file_rec;
    int64_t offset;
    int64_t length;
    double start_time;
    double end_time;
    int i;

    darshan_record_id f_id = file_rec->base_rec.id;
    int64_t rank = file_rec->base_rec.rank;
    char *hostname = file_rec->hostname;

    int64_t write_count = file_rec->write_count;
    int64_t read_count = file_rec->read_count;
    segment_info *io_trace = (segment_info *)
        ((void *)file_rec + sizeof(struct dxt_file_record));

    /* Lustre File System */
    struct darshan_lustre_record *rec;
    int lustreFS = !strcmp(fs_type, "lustre");
    int32_t stripe_size;
    int32_t stripe_count;
    int64_t cur_offset;
    int print_count;
    int ost_idx;
    
    if (!lustre_rec_ref) {
        lustreFS = 0;
    }

    printf("\n# DXT, file_id: %" PRIu64 ", file_name: %s\n", f_id, file_name);
    printf("# DXT, rank: %" PRId64 ", hostname: %s\n", rank, hostname);
    printf("# DXT, write_count: %" PRId64 ", read_count: %" PRId64 "\n",
                write_count, read_count);

    printf("# DXT, mnt_pt: %s, fs_type: %s\n", mnt_pt, fs_type);
    if (lustreFS) {
        rec = lustre_rec_ref->rec;
        stripe_size = rec->counters[LUSTRE_STRIPE_SIZE];
        stripe_count = rec->counters[LUSTRE_STRIPE_WIDTH];

        printf("# DXT, Lustre stripe_size: %d, Lustre stripe_count: %d\n", stripe_size, stripe_count);

        printf("# DXT, Lustre OST obdidx:");
        for (i = 0; i < stripe_count; i++) {
            printf(" %" PRId64, (rec->ost_ids)[i]);
        }
        printf("\n");
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

        printf("%8s%8" PRId64 "%7s%9d%16" PRId64 "%16" PRId64 "%12.4f%12.4f", "X_POSIX", rank, "write", i, offset, length, start_time, end_time);

        if (lustreFS) {
            cur_offset = offset;
            ost_idx = (offset / stripe_size) % stripe_count;

            print_count = 0;
            while (cur_offset < offset + length) {
                printf("  [%3" PRId64 "]", (rec->ost_ids)[ost_idx]);

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

        printf("%8s%8" PRId64 "%7s%9d%16" PRId64 "%16" PRId64 "%12.4f%12.4f", "X_POSIX", rank, "read", (int)(i - write_count), offset, length, start_time, end_time);

        if (lustreFS) {
            cur_offset = offset;
            ost_idx = (offset / stripe_size) % stripe_count;

            print_count = 0;
            while (cur_offset < offset + length) {
                printf("  [%3" PRId64 "]", (rec->ost_ids)[ost_idx]);

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

void dxt_log_print_mpiio_file(void *mpiio_file_rec, char *file_name,
    char *mnt_pt, char *fs_type)
{
    struct dxt_file_record *file_rec =
                (struct dxt_file_record *)mpiio_file_rec;

    int64_t length;
    int64_t offset;
    double start_time;
    double end_time;
    int i;

    darshan_record_id f_id = file_rec->base_rec.id;
    int64_t rank = file_rec->base_rec.rank;
    char *hostname = file_rec->hostname;

    int64_t write_count = file_rec->write_count;
    int64_t read_count = file_rec->read_count;

    segment_info *io_trace = (segment_info *)
        ((void *)file_rec + sizeof(struct dxt_file_record));

    printf("\n# DXT, file_id: %" PRIu64 ", file_name: %s\n", f_id, file_name);
    printf("# DXT, rank: %" PRId64 ", hostname: %s\n", rank, hostname);
    printf("# DXT, write_count: %" PRId64 ", read_count: %" PRId64 "\n",
                write_count, read_count);

    printf("# DXT, mnt_pt: %s, fs_type: %s\n", mnt_pt, fs_type);

    /* Print header */
    printf("# Module    Rank  Wt/Rd  Segment          Offset       Length    Start(s)      End(s)\n");

    /* Print IO Traces information */
    for (i = 0; i < write_count; i++) {
        offset = io_trace[i].offset;
        length = io_trace[i].length;
        start_time = io_trace[i].start_time;
        end_time = io_trace[i].end_time;

        printf("%8s%8" PRId64 "%7s%9d%16" PRId64 "%16" PRId64 "%12.4f%12.4f\n", "X_MPIIO", rank, "write", i, offset, length, start_time, end_time);
    }

    for (i = write_count; i < write_count + read_count; i++) {
        offset = io_trace[i].offset;
        length = io_trace[i].length;
        start_time = io_trace[i].start_time;
        end_time = io_trace[i].end_time;

        printf("%8s%8" PRId64 "%7s%9d%16" PRId64 "%16" PRId64 "%12.4f%12.4f\n", "X_MPIIO", rank, "read", (int)(i - write_count), offset, length, start_time, end_time);
    }

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
