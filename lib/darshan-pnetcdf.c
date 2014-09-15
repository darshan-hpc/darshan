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

#define NC_NOERR 0

typedef enum {
        NC_NAT =        0,      /* NAT = 'Not A Type' (c.f. NaN) */
        NC_BYTE =       1,      /* signed 1 byte integer */
        NC_CHAR =       2,      /* ISO/ASCII character */
        NC_SHORT =      3,      /* signed 2 byte integer */
        NC_INT =        4,      /* signed 4 byte integer */
        NC_FLOAT =      5,      /* single precision floating point number */
        NC_DOUBLE =     6       /* double precision floating point number */
} nc_type;

typedef nc_type ncmpi_type;


extern int __real_ncmpi_create(MPI_Comm comm, const char *path, 
    int cmode, MPI_Info info, int *ncidp);
extern int __real_ncmpi_open(MPI_Comm comm, const char *path, 
    int omode, MPI_Info info, int *ncidp);
extern int __real_ncmpi_close(int ncid);

/* nonblocking interfaces */
extern int __real_ncmpi_iput_vara(int ncid, int varid,
                const MPI_Offset *start, const MPI_Offset *count,
                const void *buf, MPI_Offset bufcount, MPI_Datatype datatype,
                int *reqid);

extern int __real_ncmpi_wait_all(int  ncid, int  num_reqs,
		int *req_ids, int *statuses);

/* blocking interfaces */
extern int __real_ncmpi_put_vara_all(int ncid, int varid,
                   const MPI_Offset  start[], const MPI_Offset  count[],
                   const void *buf, MPI_Offset bufcount,
                   MPI_Datatype datatype);

extern int __real_ncmpi_put_vara_double_all(int ncid, int varid,
        const MPI_Offset start[],
        const MPI_Offset count[], const double *op);

extern int __real_ncmpi_put_vara_int_all(int ncid, int varid,
    const MPI_Offset  start[], const MPI_Offset  count[],
    const void *buf);

extern int __real_ncmpi_put_vara_float_all(int ncid, int varid, const MPI_Offset start[],
        const MPI_Offset count[], const float *op);

extern int __real_ncmpi_put_vars_double_all(int ncid, int varid,
        const MPI_Offset start[],
        const MPI_Offset count[], const MPI_Offset stride[], const double *op);

extern int __real_ncmpi_put_vars_float_all(int ncid, int varid, const MPI_Offset start[],
        const MPI_Offset count[], const MPI_Offset stride[], const float *op);

/* define-mode functions */
extern int __real_ncmpi_enddef(int ncid);
extern int __real_ncmpi_def_dim(int ncid, const char *name, MPI_Offset size, int *dimidp);

extern int __real_ncmpi_def_var(int ncid, const char *name, nc_type type,
        int ndims, const int *dimids, int *varidp);

/* inquiry functions */
extern int __real_ncmpi_get_att_double(int ncid, int varid, const char *name, double *tp);
extern int __real_ncmpi_get_att_int(int ncid, int varid, const char *name, int *tp);
extern int __real_ncmpi_get_att_text(int ncid, int varid, const char *name, char *str);

static int nr_writes=0;
static int nr_reads=0;

static struct darshan_file_runtime* darshan_file_by_ncid(int ncid);


#define CP_RECORD_PNETCDF_META(__ret, __ncid, __tim1, __tim2) do {\
    struct darshan_file_runtime *file;\
    if (__ret != NC_NOERR) break;\
    file = darshan_file_by_ncid(__ncid);\
    CP_F_INC(file, CP_F_NC_META_TIME, (__tim2-__tim1));\
} while (0)

#define CP_RECORD_PNETCDF_WRITE(__ret, __ncid, __tim1, __tim2) do {\
	struct darshan_file_runtime*file;\
	if (__ret != NC_NOERR) break;\
	file = darshan_file_by_ncid(__ncid);\
	CP_F_INC(file, CP_F_NC_WRITE_TIME, (__tim2-__tim1));\
} while (0)

#define CP_RECORD_PNETCDF_READ(__ret, __ncid, __tim1, __tim2) do {\
	struct darshan_file_runtime*file;\
	if (__ret != NC_NOERR) break;\
	file = darshan_file_by_ncid(__ncid);\
	CP_F_INC(file, CP_F_NC_READ_TIME, (__tim2-__tim1));\
} while (0)



int __wrap_ncmpi_create(MPI_Comm comm, const char *path, 
    int cmode, MPI_Info info, int *ncidp)
{
    int ret;
    struct darshan_file_runtime* file;
    char* tmp;
    int comm_size;
    int hash_index;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real_ncmpi_create(comm, path, cmode, info, ncidp);
    tm2 = darshan_wtime();
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

            CP_F_INC(file, CP_F_MPI_META_TIME, (tm2-tm1));

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

            CP_F_INC(file, CP_F_MPI_META_TIME, (tm2-tm1));

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
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real_ncmpi_close(ncid);
    tm2 = darshan_wtime();

    CP_LOCK();
    file = darshan_file_by_ncid(ncid);
    if(file)
    {
        CP_F_INC(file, CP_F_NC_META_TIME, (tm2-tm1));
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

int __wrap_ncmpi_put_vara_all(int ncid, int varid, 
    const MPI_Offset  start[], const MPI_Offset  count[], 
    const void *buf, MPI_Offset bufcount, MPI_Datatype datatype)
{   
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real_ncmpi_put_vara_all(ncid, varid, start, count, buf, bufcount,
                    datatype);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_PNETCDF_WRITE(ret, ncid, tm1, tm2);
    CP_UNLOCK();
    return (ret);
}

int __wrap_ncmpi_put_vara_double_all(int ncid, int varid, const MPI_Offset start[],
        const MPI_Offset count[], const double *op)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real_ncmpi_put_vara_double_all(ncid, varid, start, count, op);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_PNETCDF_WRITE(ret, ncid, tm1, tm2);
    CP_UNLOCK();
    return (ret);
}

int __wrap_ncmpi_put_vara_float_all(int ncid, int varid, const MPI_Offset start[],
        const MPI_Offset count[], const float *op)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real_ncmpi_put_vara_float_all(ncid, varid, start, count, op);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_PNETCDF_WRITE(ret, ncid, tm1, tm2);
    CP_UNLOCK();
    return (ret);
}
int __wrap_ncmpi_put_vars_double_all(int ncid, int varid, const MPI_Offset start[],
        const MPI_Offset count[], const MPI_Offset stride[], const double *op)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real_ncmpi_put_vars_double_all(ncid, varid, start, count, stride, op);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_PNETCDF_WRITE(ret, ncid, tm1, tm2);
    CP_UNLOCK();
    return (ret);
}

int __wrap_ncmpi_put_vars_float_all(int ncid, int varid, const MPI_Offset start[],
        const MPI_Offset count[], const MPI_Offset stride[], const float *op)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real_ncmpi_put_vars_float_all(ncid, varid, start, count, stride, op);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_PNETCDF_WRITE(ret, ncid, tm1, tm2);
    CP_UNLOCK();
    return (ret);
}



int __wrap_ncmpi_put_vara_int_all(int ncid, int varid, 
    const MPI_Offset  start[], const MPI_Offset  count[], 
    const void *buf)
{   
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real_ncmpi_put_vara_int_all(ncid, varid, start, count, buf);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_PNETCDF_WRITE(ret, ncid, tm1, tm2);
    CP_UNLOCK();
    return (ret);
}

int __wrap_ncmpi_get_vara_all(int ncid, int varid,
    const MPI_Offset  start[], const MPI_Offset  count[],
    const void *buf, MPI_Offset bufcount, MPI_Datatype datatype)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real_ncmpi_get_vara_all(ncid, varid, start, count, buf, bufcount,
                    datatype);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_PNETCDF_READ(ret, ncid, tm1, tm2);
    CP_UNLOCK();
    return (ret);
}

int __wrap_ncmpi_get_vara_double_all(int ncid, int varid,
    const MPI_Offset  start[], const MPI_Offset  count[], double *ip)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real_ncmpi_get_vara_double_all(ncid, varid, start, count, ip);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_PNETCDF_READ(ret, ncid, tm1, tm2);
    CP_UNLOCK();
    return (ret);
}
int __wrap_ncmpi_get_vara_int_all(int ncid, int varid,
    const MPI_Offset  start[], const MPI_Offset  count[], double *ip)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real_ncmpi_get_vara_double_all(ncid, varid, start, count, ip);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_PNETCDF_READ(ret, ncid, tm1, tm2);
    CP_UNLOCK();
    return (ret);
}

int __wrap_ncmpi_iput_vara(int ncid, int varid,
                const MPI_Offset *start, const MPI_Offset *count,
                const void *buf, MPI_Offset bufcount, MPI_Datatype datatype,
                int *reqid)
{   
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real_ncmpi_iput_vara(ncid, varid, start, count, buf, bufcount, datatype, reqid);
    tm2 = darshan_wtime();
    CP_LOCK();
    nr_writes++;
    CP_RECORD_PNETCDF_WRITE(ret, ncid, tm1, tm2);
    CP_UNLOCK();
    return (ret);
}

int __wrap_ncmpi_wait_all(int  ncid,
		int  num_reqs, 
		int *req_ids,  
		int *statuses) 
{
    int ret;
    double tm1, tm2;
    tm1 = darshan_wtime();
    ret = __real_ncmpi_wait_all(ncid, num_reqs, req_ids, statuses);
    tm2 = darshan_wtime();

    CP_LOCK();
    /* TODO: problem: ncmpi_wait{,_all} take both read and write operations.
     * Need a good way to sort out how much of this wait is read and how much
     * is write.  This is good enough for now... */

    /* fraction of writes: nr_writes/(nr_writes + nr_reads) */
    /* fraction of reads:  1- fraction_of_writes */
    /* don't need to do any of that if all the reqests are of one kind */

    if (nr_reads == 0) {
	    CP_RECORD_PNETCDF_WRITE(ret, ncid, tm1, tm2);
    } else if (nr_writes == 0)  {
	    CP_RECORD_PNETCDF_READ(ret, ncid, tm1, tm2);
    } else {
	    double delta = tm2-tm1;	
	    double write_time, read_time;
	    write_time = delta*(nr_writes/(nr_writes+nr_reads));
	    read_time = delta*(1-write_time);
	    /* second_timestamp - first_timestamp == total_time:
	     * 	so, (tm1+read_time) - tm1 = read_time
	     * 	and tm2 - (tm2 - write_time) = write_time */
	    CP_RECORD_PNETCDF_READ(ret, ncid, tm1, tm1+read_time);
	    CP_RECORD_PNETCDF_WRITE(ret, ncid, tm2-write_time, tm2);
    }
    nr_writes=0;
    nr_reads=0;
    CP_UNLOCK();

    return ret;
}
int __wrap_ncmpi_def_dim(int ncid, const char *name, MPI_Offset size, int *dimidp)
{
    double tm1, tm2;
    int ret;
    tm1 = darshan_wtime();
    ret = __real_ncmpi_def_dim(ncid, name, size, dimidp);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_PNETCDF_META(ret, ncid, tm1, tm2);
    CP_UNLOCK();

    return (ret);
}
int __wrap_ncmpi_def_var(int ncid, const char *name, nc_type type,
        int ndims, const int *dimids, int *varidp)
{
    double tm1, tm2;
    int ret;
    tm1 = darshan_wtime();
    ret = __real_ncmpi_def_var(ncid, name, type, ndims, dimids, varidp);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_PNETCDF_META(ret, ncid, tm1, tm2);
    CP_UNLOCK();

    return (ret);
}
int __wrap_ncmpi_enddef(int ncid)
{
    double tm1, tm2;
    int ret;
    tm1 = darshan_wtime();
    ret = __real_ncmpi_enddef(ncid);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_PNETCDF_META(ret, ncid, tm1, tm2);
    CP_UNLOCK();

    return (ret);
}

int __wrap_ncmpi_get_att_double(int ncid, int varid, const char *name, double *tp)
{
    double tm1, tm2;
    int ret;
    tm1 = darshan_wtime();
    ret = __real_ncmpi_get_att_double(ncid, varid, name, tp);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_PNETCDF_META(ret, ncid, tm1, tm2);
    CP_UNLOCK();

    return (ret);
}
int __wrap_ncmpi_get_att_int(int ncid, int varid, const char *name, int *tp)
{
    double tm1, tm2;
    int ret;
    tm1 = darshan_wtime();
    ret = __real_ncmpi_get_att_int(ncid, varid, name, tp);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_PNETCDF_META(ret, ncid, tm1, tm2);
    CP_UNLOCK();

    return (ret);
}
int __wrap_ncmpi_get_att_text(int ncid, int varid, const char *name, char *str)
{
    double tm1, tm2;
    int ret;
    tm1 = darshan_wtime();
    ret = __real_ncmpi_get_att_text(ncid, varid, name, str);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_PNETCDF_META(ret, ncid, tm1, tm2);
    CP_UNLOCK();

    return (ret);
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
