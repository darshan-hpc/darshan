/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

/* This is an API for finding and tracking file records by file name and/or
 * various file handles
 */

#ifndef __DARSHAN_FILE_H
#define __DARSHAN_FILE_H

#include "darshan.h"

enum darshan_handle_type 
{
    DARSHAN_FD = 1,
    DARSHAN_FH,
    DARSHAN_NCID,
    DARSHAN_HID
};

struct darshan_file_runtime* darshan_file_by_name(const char* name);

struct darshan_file_runtime* darshan_file_by_name_sethandle(
    const char* name,
    const void* handle,
    int handle_sz,
    enum darshan_handle_type handle_type);

struct darshan_file_runtime* darshan_file_by_handle(
    const void* handle,
    int handle_sz,
    enum darshan_handle_type handle_type);

struct darshan_file_runtime* darshan_file_closehandle(
    const void* handle,
    int handle_sz,
    enum darshan_handle_type handle_type);


#endif /* __DARSHAN_FILE_H */
