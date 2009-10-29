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
        if(file && (file->ncid == 0))
        {
            file->ncid = *ncidp;
        }

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

        CP_UNLOCK();
    }

    return(ret);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
