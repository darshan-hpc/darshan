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

/* hope this doesn't change any time soon */
typedef int hid_t; 
typedef int herr_t;

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

DARSHAN_FORWARD_DECL(H5Fcreate, hid_t, (const char *filename, unsigned flags, hid_t create_plist, hid_t access_plist));
DARSHAN_FORWARD_DECL(H5Fopen, hid_t, (const char *filename, unsigned flags, hid_t access_plist));
DARSHAN_FORWARD_DECL(H5Fclose, herr_t, (hid_t file_id));

static struct darshan_file_runtime* darshan_file_by_hid(int hid);
static void darshan_file_close_hid(int hid);
static struct darshan_file_runtime* darshan_file_by_name_sethid(const char* name, int hid);

hid_t DARSHAN_DECL(H5Fcreate)(const char *filename, unsigned flags,
    hid_t create_plist, hid_t access_plist)
{
    int ret;
    struct darshan_file_runtime* file;
    char* tmp;

    MAP_OR_FAIL(H5Fcreate);

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

        file = darshan_file_by_name_sethid(filename, ret);
        if(file)
        {
            if(CP_F_VALUE(file, CP_F_OPEN_TIMESTAMP) == 0)
                CP_F_SET(file, CP_F_OPEN_TIMESTAMP,
                PMPI_Wtime());
            CP_INC(file, CP_HDF5_OPENS, 1);
        }
        CP_UNLOCK();
    }

    return(ret);
}

hid_t DARSHAN_DECL(H5Fopen)(const char *filename, unsigned flags,
    hid_t access_plist)
{
    int ret;
    struct darshan_file_runtime* file;
    char* tmp;

    MAP_OR_FAIL(H5Fopen);

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

        file = darshan_file_by_name_sethid(filename, ret);
        if(file)
        {
            if(CP_F_VALUE(file, CP_F_OPEN_TIMESTAMP) == 0)
                CP_F_SET(file, CP_F_OPEN_TIMESTAMP,
                PMPI_Wtime());
            CP_INC(file, CP_HDF5_OPENS, 1);
        }

        CP_UNLOCK();
    }

    return(ret);

}

herr_t DARSHAN_DECL(H5Fclose)(hid_t file_id)
{
    struct darshan_file_runtime* file;
    int ret;

    MAP_OR_FAIL(H5Fclose);

    ret = __real_H5Fclose(file_id);

    CP_LOCK();
    file = darshan_file_by_hid(file_id);
    if(file)
    {
        CP_F_SET(file, CP_F_CLOSE_TIMESTAMP, PMPI_Wtime());
        darshan_file_close_hid(file_id);
    }
    CP_UNLOCK();

    return(ret);

}

static struct darshan_file_runtime* darshan_file_by_name_sethid(const char* name, int hid)
{
    struct darshan_file_runtime* tmp_file;

    tmp_file = darshan_file_by_name_sethandle(name, &hid, sizeof(hid), DARSHAN_HID);
    return(tmp_file);
}

static void darshan_file_close_hid(int hid)
{
    darshan_file_closehandle(&hid, sizeof(hid), DARSHAN_HID);
    return;
}

static struct darshan_file_runtime* darshan_file_by_hid(int hid)
{
    struct darshan_file_runtime* tmp_file;

    tmp_file = darshan_file_by_handle(&hid, sizeof(hid), DARSHAN_HID);
    
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
