/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include "mpi.h"
#include "darshan.h"
#include "darshan-config.h"

/* hope this doesn't change any time soon */
typedef int hid_t; 
typedef int herr_t;

extern hid_t __real_H5Fcreate(const char *filename, unsigned flags,
    hid_t create_plist, hid_t access_plist);
extern hid_t __real_H5Fopen(const char *filename, unsigned flags,
    hid_t access_plist);
extern herr_t __real_H5Fclose(hid_t file_id);

static struct darshan_file_runtime* darshan_file_by_hid(int hid);

hid_t __wrap_H5Fcreate(const char *filename, unsigned flags,
    hid_t create_plist, hid_t access_plist)
{
    int ret;
    struct darshan_file_runtime* file;
    char* tmp;
    int hash_index;

    ret = __real_H5Fcreate(filename, flags, create_plist, access_plist);
    if(ret >= 0)
    {  
        CP_LOCK();
        /* use ROMIO approach to strip prefix if present */
        /* strip off prefix if there is one, but only skip prefixes
         * if they are greater than length one to allow for windows
         * drive specifications (e.g. c:\...) 
         */
        tmp = strchr(filename, ':');
        if (tmp > filename + 1) {
            filename = tmp + 1;
        }

        file = darshan_file_by_name(filename);
        /* TODO: handle the case of multiple concurrent opens */
        if(file && (file->hid == -1))
        {
            file->hid = ret;
            CP_INC(file, CP_HDF5_OPENS, 1);
            hash_index = file->hid & CP_HASH_MASK;
            file->hid_prev = NULL;
            file->hid_next = darshan_global_job->hid_table[hash_index];
            if(file->hid_next) 
                file->hid_next->hid_prev = file;
            darshan_global_job->hid_table[hash_index] = file;
        }
        CP_UNLOCK();
    }

    return(ret);
}

hid_t __wrap_H5Fopen(const char *filename, unsigned flags,
    hid_t access_plist)
{
    int ret;
    struct darshan_file_runtime* file;
    char* tmp;
    int hash_index;

    ret = __real_H5Fopen(filename, flags, access_plist);
    if(ret >= 0)
    {  
        CP_LOCK();
        /* use ROMIO approach to strip prefix if present */
        /* strip off prefix if there is one, but only skip prefixes
         * if they are greater than length one to allow for windows
         * drive specifications (e.g. c:\...) 
         */
        tmp = strchr(filename, ':');
        if (tmp > filename + 1) {
            filename = tmp + 1;
        }

        file = darshan_file_by_name(filename);
        /* TODO: handle the case of multiple concurrent opens */
        if(file && (file->hid == -1))
        {
            file->hid = ret;
            CP_INC(file, CP_HDF5_OPENS, 1);
            hash_index = file->hid & CP_HASH_MASK;
            file->hid_prev = NULL;
            file->hid_next = darshan_global_job->hid_table[hash_index];
            if(file->hid_next) 
                file->hid_next->hid_prev = file;
            darshan_global_job->hid_table[hash_index] = file;
        }

        CP_UNLOCK();
    }

    return(ret);

}

herr_t __wrap_H5Fclose(hid_t file_id)
{
    struct darshan_file_runtime* file;
    int hash_index;
    int tmp_hid = file_id;
    int ret;

    ret = __real_H5Fclose(file_id);

    CP_LOCK();
    file = darshan_file_by_hid(file_id);
    if(file)
    {
        file->hid = -1;
        if(file->hid_prev == NULL)
        {
            /* head of hid hash table list */
            hash_index = tmp_hid & CP_HASH_MASK;
            darshan_global_job->hid_table[hash_index] = file->hid_next;
            if(file->hid_next)
                file->hid_next->hid_prev = NULL;
        }
        else
        {
            if(file->hid_prev)
                file->hid_prev->hid_next = file->hid_next;
            if(file->hid_next)
                file->hid_next->hid_prev = file->hid_prev;
        }
        file->hid_prev = NULL;
        file->hid_next = NULL;
        darshan_global_job->darshan_mru_file = file; /* in case we open it again */
    }
    CP_UNLOCK();

    return(ret);

}

static struct darshan_file_runtime* darshan_file_by_hid(int hid)
{
    int hash_index;
    struct darshan_file_runtime* tmp_file;

    if(!darshan_global_job)
    {
        return(NULL);
    }

    /* if we have already condensed the data, then just hand the first file
     * back
     */
    if(darshan_global_job->flags & CP_FLAG_CONDENSED)
    {
        return(&darshan_global_job->file_runtime_array[0]);
    }

    /* try mru first */
    if(darshan_global_job->darshan_mru_file && darshan_global_job->darshan_mru_file->hid == hid)
    {
        return(darshan_global_job->darshan_mru_file);
    }

    /* search hash table */
    hash_index = hid & CP_HASH_MASK;
    tmp_file = darshan_global_job->hid_table[hash_index];
    while(tmp_file)
    {
        if(tmp_file->hid == hid)
        {
            darshan_global_job->darshan_mru_file = tmp_file;
            return(tmp_file);
        }
        tmp_file = tmp_file->hid_next;
    }

    return(NULL);
}


/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
