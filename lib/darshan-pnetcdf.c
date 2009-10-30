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

extern int __real_ncmpi_create(MPI_Comm comm, const char *path, 
    int cmode, MPI_Info info, int *ncidp);
extern int __real_ncmpi_open(MPI_Comm comm, const char *path, 
    int omode, MPI_Info info, int *ncidp);
extern int __real_ncmpi_close(int ncid);

static struct darshan_file_runtime* darshan_file_by_ncid(int ncid);

int __wrap_ncmpi_create(MPI_Comm comm, const char *path, 
    int cmode, MPI_Info info, int *ncidp)
{
    int ret;
    struct darshan_file_runtime* file;
    char* tmp;
    int comm_size;
    int hash_index;

    ret = __real_ncmpi_create(comm, path, cmode, info, ncidp);
    if(ret == 0)
    {  
        CP_LOCK();
        /* use ROMIO approach to strip prefix if present */
        /* strip off prefix if there is one, but only skip prefixes
         * if they are greater than length one to allow for windows
         * drive specifications (e.g. c:\...) 
         */
        tmp = strchr(path, ':');
        if (tmp > path + 1) {
            path = tmp + 1;
        }

        file = darshan_file_by_name(path);
        /* TODO: handle the case of multiple concurrent opens */
        if(file && (file->ncid == -1))
        {
            file->ncid = *ncidp;

            PMPI_Comm_size(comm, &comm_size);
            if(comm_size == 1)
            {
                CP_INC(file, CP_INDEP_NC_OPENS, 1);
            }
            else
            {
                CP_INC(file, CP_COLL_NC_OPENS, 1);
            }

            hash_index = file->ncid & CP_HASH_MASK;
            file->ncid_prev = NULL;
            file->ncid_next = darshan_global_job->ncid_table[hash_index];
            if(file->ncid_next) 
                file->ncid_next->ncid_prev = file;
            darshan_global_job->ncid_table[hash_index] = file;
        }
        CP_UNLOCK();
    }

    return(ret);
}

int __wrap_ncmpi_open(MPI_Comm comm, const char *path, 
    int omode, MPI_Info info, int *ncidp)
{
    int ret;
    struct darshan_file_runtime* file;
    char* tmp;
    int comm_size;
    int hash_index;

    ret = __real_ncmpi_open(comm, path, omode, info, ncidp);
    if(ret == 0)
    {  
        CP_LOCK();
        /* use ROMIO approach to strip prefix if present */
        /* strip off prefix if there is one, but only skip prefixes
         * if they are greater than length one to allow for windows
         * drive specifications (e.g. c:\...) 
         */
        tmp = strchr(path, ':');
        if (tmp > path + 1) {
            path = tmp + 1;
        }

        file = darshan_file_by_name(path);
        /* TODO: handle the case of multiple concurrent opens */
        if(file && (file->ncid == -1))
        {
            file->ncid = *ncidp;

            PMPI_Comm_size(comm, &comm_size);
            if(comm_size == 1)
            {
                CP_INC(file, CP_INDEP_NC_OPENS, 1);
            }
            else
            {
                CP_INC(file, CP_COLL_NC_OPENS, 1);
            }

            hash_index = file->ncid & CP_HASH_MASK;
            file->ncid_prev = NULL;
            file->ncid_next = darshan_global_job->ncid_table[hash_index];
            if(file->ncid_next) 
                file->ncid_next->ncid_prev = file;
            darshan_global_job->ncid_table[hash_index] = file;
        }
        CP_UNLOCK();
    }

    return(ret);

}

int __wrap_ncmpi_close(int ncid)
{
    struct darshan_file_runtime* file;
    int hash_index;
    int tmp_ncid = ncid;
    int ret;

    ret = __real_ncmpi_close(ncid);

    CP_LOCK();
    file = darshan_file_by_ncid(ncid);
    if(file)
    {
        file->ncid = -1;
        if(file->ncid_prev == NULL)
        {
            /* head of ncid hash table list */
            hash_index = tmp_ncid & CP_HASH_MASK;
            darshan_global_job->ncid_table[hash_index] = file->ncid_next;
            if(file->ncid_next)
                file->ncid_next->ncid_prev = NULL;
        }
        else
        {
            if(file->ncid_prev)
                file->ncid_prev->ncid_next = file->ncid_next;
            if(file->ncid_next)
                file->ncid_next->ncid_prev = file->ncid_prev;
        }
        file->ncid_prev = NULL;
        file->ncid_next = NULL;
        darshan_global_job->darshan_mru_file = file; /* in case we open it again */
    }
    CP_UNLOCK();

    return(ret);

}

static struct darshan_file_runtime* darshan_file_by_ncid(int ncid)
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
    if(darshan_global_job->darshan_mru_file && darshan_global_job->darshan_mru_file->ncid == ncid)
    {
        return(darshan_global_job->darshan_mru_file);
    }

    /* search hash table */
    hash_index = ncid & CP_HASH_MASK;
    tmp_file = darshan_global_job->ncid_table[hash_index];
    while(tmp_file)
    {
        if(tmp_file->ncid == ncid)
        {
            darshan_global_job->darshan_mru_file = tmp_file;
            return(tmp_file);
        }
        tmp_file = tmp_file->ncid_next;
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
