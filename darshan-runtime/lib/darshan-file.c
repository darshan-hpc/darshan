/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

/* This is an API for finding and tracking file records by file name and/or
 * various file handles
 */

#include <pthread.h>
#include "mpi.h"

#include "darshan-file.h"


static pthread_mutex_t darshan_file_lock = PTHREAD_MUTEX_INITIALIZER;

#define DARSHAN_FILE_HANDLE_MAX (sizeof(MPI_File))
struct darshan_file_ref
{
    struct darshan_file_runtime* file;
    char handle[DARSHAN_FILE_HANDLE_MAX];
    int handle_sz;
    enum darshan_handle_type type;
    struct darshan_file_ref* next;
    struct darshan_file_ref* prev;
};

#define DARSHAN_FILE_TABLE_SIZE 16
static struct darshan_file_ref* darshan_file_table[DARSHAN_FILE_TABLE_SIZE] = {NULL};

struct darshan_file_runtime* darshan_file_by_name(const char* name)
{
    return(NULL);
}

struct darshan_file_runtime* darshan_file_by_name_sethandle(
    const char* name,
    const void* handle,
    int handle_sz,
    enum darshan_handle_type handle_type)
{
    return(NULL);
}

struct darshan_file_runtime* darshan_file_by_handle(
    const void* handle,
    int handle_sz,
    enum darshan_handle_type handle_type)
{
    return(NULL);
}

struct darshan_file_runtime* darshan_file_closehandle(
    const void* handle,
    int handle_sz,
    enum darshan_handle_type handle_type)
{
    return(NULL);
}

