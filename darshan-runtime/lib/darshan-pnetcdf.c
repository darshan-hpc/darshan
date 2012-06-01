/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include "darshan-runtime-config.h"
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include "mpi.h"
#include "darshan.h"

#ifdef DARSHAN_PRELOAD
#define __USE_GNU
#include <dlfcn.h>
#include <stdlib.h>

#define DARSHAN_FORWARD_DECL(name,ret,args) \
  ret (*__real_ ## name)args = NULL;
         
#define DARSHAN_DECL(__name) __name

#define MAP_OR_FAIL(func) \
    if (!(__real_ ## func)) \
    { \
        __real_ ## func = dlsym(RTLD_NEXT, #func); \
        if(!(__real_ ## func)) { \
            fprintf(stderr, "Darshan failed to map symbol: %s\n", #func); \
            exit(1); \
        } \
    }
 
#else   
    
#define DARSHAN_FORWARD_DECL(name,ret,args) \
  extern ret __real_ ## name args;

#define DARSHAN_DECL(__name) __wrap_ ## __name

#define MAP_OR_FAIL(func)

#endif

DARSHAN_FORWARD_DECL(ncmpi_create, int, (MPI_Comm comm, const char *path, int cmode, MPI_Info info, int *ncidp));
DARSHAN_FORWARD_DECL(ncmpi_open, int, (MPI_Comm comm, const char *path, int omode, MPI_Info info, int *ncidp));
DARSHAN_FORWARD_DECL(ncmpi_close, int, (int ncid));

static struct darshan_file_runtime* darshan_file_by_ncid(int ncid);
static void darshan_file_close_ncid(int ncid);
static struct darshan_file_runtime* darshan_file_by_name_setncid(const char* name, int ncid);

int DARSHAN_DECL(ncmpi_create)(MPI_Comm comm, const char *path, 
    int cmode, MPI_Info info, int *ncidp)
{
    int ret;
    struct darshan_file_runtime* file;
    char* tmp;
    int comm_size;

    MAP_OR_FAIL(ncmpi_create);

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

        file = darshan_file_by_name_setncid(path, (*ncidp));
        if(file)
        {
            if(CP_F_VALUE(file, CP_F_OPEN_TIMESTAMP) == 0)
                CP_F_SET(file, CP_F_OPEN_TIMESTAMP,
                PMPI_Wtime());
            PMPI_Comm_size(comm, &comm_size);
            if(comm_size == 1)
            {
                CP_INC(file, CP_INDEP_NC_OPENS, 1);
            }
            else
            {
                CP_INC(file, CP_COLL_NC_OPENS, 1);
            }
        }
        CP_UNLOCK();
    }

    return(ret);
}

int DARSHAN_DECL(ncmpi_open)(MPI_Comm comm, const char *path, 
    int omode, MPI_Info info, int *ncidp)
{
    int ret;
    struct darshan_file_runtime* file;
    char* tmp;
    int comm_size;

    MAP_OR_FAIL(ncmpi_open);

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

        file = darshan_file_by_name_setncid(path, (*ncidp));
        if(file)
        {
            if(CP_F_VALUE(file, CP_F_OPEN_TIMESTAMP) == 0)
                CP_F_SET(file, CP_F_OPEN_TIMESTAMP,
                PMPI_Wtime());
            PMPI_Comm_size(comm, &comm_size);
            if(comm_size == 1)
            {
                CP_INC(file, CP_INDEP_NC_OPENS, 1);
            }
            else
            {
                CP_INC(file, CP_COLL_NC_OPENS, 1);
            }
        }
        CP_UNLOCK();
    }

    return(ret);

}

int DARSHAN_DECL(ncmpi_close)(int ncid)
{
    struct darshan_file_runtime* file;
    int ret;

    MAP_OR_FAIL(ncmpi_close); 

    ret = __real_ncmpi_close(ncid);

    CP_LOCK();
    file = darshan_file_by_ncid(ncid);
    if(file)
    {
        CP_F_SET(file, CP_F_CLOSE_TIMESTAMP, PMPI_Wtime());
        darshan_file_close_ncid(ncid);
    }
    CP_UNLOCK();

    return(ret);
}

static struct darshan_file_runtime* darshan_file_by_name_setncid(const char* name, int ncid)
{
    struct darshan_file_runtime* tmp_file;

    tmp_file = darshan_file_by_name_sethandle(name, &ncid, sizeof(ncid), DARSHAN_NCID);
    return(tmp_file);
}

static void darshan_file_close_ncid(int ncid)
{
    darshan_file_closehandle(&ncid, sizeof(ncid), DARSHAN_NCID);
    return;
}

static struct darshan_file_runtime* darshan_file_by_ncid(int ncid)
{
    struct darshan_file_runtime* tmp_file;

    tmp_file = darshan_file_by_handle(&ncid, sizeof(ncid), DARSHAN_NCID);
    
    return(tmp_file);
}



/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
