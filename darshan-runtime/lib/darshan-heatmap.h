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

#ifdef DARSHAN_HEATMAP

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

#else

/* The heatmap API functions are often invoked from within large macros in
 * other Darshan modules, making them tedious to ifdef guard there. To make
 * things easier we just provide stub functions when the heatmap module
 * is disabled so that other modules do not need preprocessor modifications.
 */

static inline darshan_record_id heatmap_register(const char* name) {
    return(0);
}

#define heatmap_update(heatmap_id, rw_flag, size, start_time, end_time) \
do {} while(0)

#endif

#endif /* __DARSHAN_HEATMAP_H */
