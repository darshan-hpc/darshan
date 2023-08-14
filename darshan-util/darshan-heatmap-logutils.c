/*
 * Copyright (C) 2015 University of Chicago.
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

/* prototypes for each of the heatmap module's logutil functions */
static int darshan_log_get_heatmap_record(darshan_fd fd, void** heatmap_buf_p);
static int darshan_log_put_heatmap_record(darshan_fd fd, void* heatmap_buf);
static void darshan_log_print_heatmap_record(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type);
static void darshan_log_print_heatmap_description(int ver);

/* structure storing each function needed for implementing the darshan
 * logutil interface. these functions are used for reading, writing, and
 * printing module data in a consistent manner.
 */
struct darshan_mod_logutil_funcs heatmap_logutils =
{
    .log_get_record = &darshan_log_get_heatmap_record,
    .log_put_record = &darshan_log_put_heatmap_record,
    .log_print_record = &darshan_log_print_heatmap_record,
    .log_print_description = &darshan_log_print_heatmap_description,
    /* _diff is deliberately not implelmented; it's not clear what a heatmap
     * diff would represent or how it might be used.
     */
    .log_print_diff = NULL,
    /* _agg is deliberately not implemented; there are no shared records in
     * the heatmap; it is always reported per process
     */
    .log_agg_records = NULL
};

/* retrieve a heatmap record from log file descriptor 'fd', storing the
 * data in the buffer address pointed to by 'heatmap_buf_p'. Return 1 on
 * successful record read, 0 on no more data, and -1 on error.
 */
static int darshan_log_get_heatmap_record(darshan_fd fd, void** heatmap_buf_p)
{
    struct darshan_heatmap_record *rec = *((struct darshan_heatmap_record **)heatmap_buf_p);
    struct darshan_heatmap_record static_rec = {0};
    void* trailing;
    int ret;
    int i;
    int total_rec_size;

    if(fd->mod_map[DARSHAN_HEATMAP_MOD].len == 0)
        return(0);

    if(fd->mod_ver[DARSHAN_HEATMAP_MOD] == 0 ||
        fd->mod_ver[DARSHAN_HEATMAP_MOD] > DARSHAN_HEATMAP_VER)
    {
        fprintf(stderr, "Error: Invalid HEATMAP module version number (got %d)\n",
            fd->mod_ver[DARSHAN_HEATMAP_MOD]);
        return(-1);
    }

    if(*heatmap_buf_p == NULL)
        rec = &static_rec;

    /* read base record; it is a fixed size */
    ret = darshan_log_get_mod(fd, DARSHAN_HEATMAP_MOD, rec,
        sizeof(struct darshan_heatmap_record));
    if(ret < 0)
        return(-1);
    else if(ret < sizeof(struct darshan_heatmap_record))
        return(0);

    /* do byte swapping if necessary */
    if(fd->swap_flag)
    {
        DARSHAN_BSWAP64(&rec->base_rec.id);
        DARSHAN_BSWAP64(&rec->base_rec.rank);
        DARSHAN_BSWAP64(&rec->bin_width_seconds);
        DARSHAN_BSWAP64(&rec->nbins);
    }
    /* If this is the first heatmap record that we have seen for this log,
     * then record the initial bin count and width.  We may need to correct
     * subsequent records that are off by one.
     */
    if(fd->first_heatmap_record_nbins == 0)
        fd->first_heatmap_record_nbins = rec->nbins;
    if(fd->first_heatmap_record_bin_width_seconds == 0)
        fd->first_heatmap_record_bin_width_seconds = rec->bin_width_seconds;

    /* if buffer was provided by caller, then it is implied that it is
     * DEF_MOD_BUF_SIZE bytes in size.  Make sure it is big enough, or if we
     * are allocating the buffer malloc enough size */
    total_rec_size = sizeof(struct darshan_heatmap_record) + rec->nbins*2*sizeof(int64_t);
    if(*heatmap_buf_p)
    {
        if(total_rec_size > DEF_MOD_BUF_SIZE)
        {
            fprintf(stderr, "Error: HEATMAP record is %d bytes, but DEF_MOD_BUF_SIZE is only %d bytes\n", total_rec_size, DEF_MOD_BUF_SIZE);
            return(-1);
        }
    }
    else
    {
        *heatmap_buf_p = malloc(total_rec_size);
        if(!(*heatmap_buf_p))
            return(-1);
        memcpy(*heatmap_buf_p, rec, sizeof(*rec));
        rec = *heatmap_buf_p;
    }

    /* set pointer for trailing data */
    trailing = (void*)((intptr_t)(*heatmap_buf_p) + sizeof(*rec));
    ret = darshan_log_get_mod(fd, DARSHAN_HEATMAP_MOD, trailing,
        rec->nbins*2*sizeof(int64_t));
    if(ret < rec->nbins*2*sizeof(int64_t))
        return(-1);

    /* set pointers and byteswap trailing data */
    rec->write_bins = (int64_t*)((uintptr_t)rec + sizeof(*rec));
    rec->read_bins = (int64_t*)((uintptr_t)rec + sizeof(*rec) + rec->nbins*sizeof(uint64_t));
    if(fd->swap_flag)
    {
        for(i=0; i<rec->nbins; i++)
        {
            DARSHAN_BSWAP64(&rec->write_bins[i]);
            DARSHAN_BSWAP64(&rec->read_bins[i]);
        }
    }
    /* On the fly correction if we find a record that is off by one in the
     * number of bins.
     *
     * NOTE: only do this if a) the bin width is correct and b) the size is
     * only off by one.  We don't want to make dramatic changes here, just
     * fix minor clock skew problems as described below, caused by a runtime bug
     * when shared reductions were disabled in Darshan 3.4.0 to 3.4.3.
     * https://github.com/darshan-hpc/darshan/issues/941
     */
    if(rec->bin_width_seconds == fd->first_heatmap_record_bin_width_seconds &&
        rec->nbins == (fd->first_heatmap_record_nbins + 1))
    {
        /* One too many bins in this record.  Just drop one. */
        /* note that this will leave a hole between the write bins array and
         * read bins array; that's fine because we have already set pointers
         * to the correct start of each one.
         */
        rec->nbins--;
    }
    else if(rec->bin_width_seconds == fd->first_heatmap_record_bin_width_seconds &&
        rec->nbins == (fd->first_heatmap_record_nbins - 1))
    {
        /* One too few bins; need to add one */

        /* we have to shift the read bins down because the data is
         * contiguous in memory
         */
        memmove(&rec->read_bins[1], &rec->read_bins[0],
                rec->nbins*sizeof(uint64_t));
        /* zero out values in new bins */
        rec->write_bins[rec->nbins] = 0;
        rec->read_bins[rec->nbins] = 0;
        rec->nbins++;
    }

    return(1);
}

/* write the heatmap record stored in 'heatmap_buf' to log file descriptor 'fd'.
 * Return 0 on success, -1 on failure
 */
static int darshan_log_put_heatmap_record(darshan_fd fd, void* heatmap_buf)
{
    struct darshan_heatmap_record *rec = (struct darshan_heatmap_record *)heatmap_buf;
    int ret;

    /* append heatmap record to darshan log file */
    ret = darshan_log_put_mod(fd, DARSHAN_HEATMAP_MOD, rec,
        sizeof(struct darshan_heatmap_record) + rec->nbins*2*sizeof(int64_t), DARSHAN_HEATMAP_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

/* print all I/O data record statistics for the given heatmap record */
static void darshan_log_print_heatmap_record(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type)
{
    struct darshan_heatmap_record *heatmap_rec =
        (struct darshan_heatmap_record *)file_rec;
    char counter_name_buffer[256];
    int i;

    DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_HEATMAP_MOD],
        heatmap_rec->base_rec.rank, heatmap_rec->base_rec.id,
        "HEATMAP_F_BIN_WIDTH_SECONDS",
        heatmap_rec->bin_width_seconds, file_name, mnt_pt, fs_type);

    for(i=0; i<heatmap_rec->nbins; i++)
    {
        snprintf(counter_name_buffer, 256, "HEATMAP_READ_BIN_%d", i);
        DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_HEATMAP_MOD],
            heatmap_rec->base_rec.rank, heatmap_rec->base_rec.id,
            counter_name_buffer,
            heatmap_rec->read_bins[i], file_name, mnt_pt, fs_type);
    }

    for(i=0; i<heatmap_rec->nbins; i++)
    {
        snprintf(counter_name_buffer, 256, "HEATMAP_WRITE_BIN_%d", i);
        DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_HEATMAP_MOD],
            heatmap_rec->base_rec.rank, heatmap_rec->base_rec.id,
            counter_name_buffer,
            heatmap_rec->write_bins[i], file_name, mnt_pt, fs_type);
    }

    return;
}

/* print out a description of the heatmap module record fields */
static void darshan_log_print_heatmap_description(int ver)
{
    printf("\n# description of heatmap counters:\n");
    printf("#   HEATMAP_F_BIN_WIDTH_SECONDS: time duration of each heatmap bin\n");
    printf("#   HEATMAP_{READ|WRITE}_BIN_{*}: number of bytes read or written within specified heatmap bin\n");

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
