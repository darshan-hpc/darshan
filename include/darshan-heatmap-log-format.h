/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_HEATMAP_LOG_FORMAT_H
#define __DARSHAN_HEATMAP_LOG_FORMAT_H

/* current HEATMAP log format version */
#define DARSHAN_HEATMAP_VER 1

/* record structure for a Darshan heatmap.  These should be one per
 * API/category that registers heatmap data.  Each is variable size
 * according to the nbins field.
 */
struct darshan_heatmap_record
{
    struct darshan_base_record base_rec;
    double  bin_width_seconds; /* time duration of each bin */
    int64_t nbins;             /* number of bins */
    int64_t *write_bins;       /* pointer to write bin array (trails struct in log */
    int64_t *read_bins;        /* pointer to read bin array (trails write bin array in log */
};

#endif /* __DARSHAN_HEATMAP_LOG_FORMAT_H */
