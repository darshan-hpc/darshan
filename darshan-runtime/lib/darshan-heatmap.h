/*
 * Copyright (C) 2021 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_HEATMAP_H
#define __DARSHAN_HEATMAP_H

#include <stdint.h>

#define HEATMAP_READ 1
#define HEATMAP_WRITE 2

/* heatmap_register()
 *
 * registers a heatmap with specified name.  Returns record id to use for
 * subsequent updates
 */
darshan_record_id heatmap_register(const char* name);

/* heatmap_read()
 *
 * functions to record read and write traffic
 */
void heatmap_update(darshan_record_id heatmap_id, int rw_flag,
    int64_t size, double start_time, double end_time);

#endif /* __DARSHAN_HEATMAP_H */
