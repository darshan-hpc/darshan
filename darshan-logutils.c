/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include "darshan-logutils.h"
#include "darshan-config.h"

/* isn't there a clever c way to avoid this? */
char *darshan_names[] = {
    "CP_INDEP_OPENS",
    "CP_COLL_OPENS",               /* count of MPI collective opens */
    "CP_INDEP_READS",              /* count of independent MPI reads */
    "CP_INDEP_WRITES",             /* count of independent MPI writes */
    "CP_COLL_READS",               /* count of collective MPI reads */
    "CP_COLL_WRITES",              /* count of collective MPI writes */
    "CP_SPLIT_READS",              /* count of split collective MPI reads */
    "CP_SPLIT_WRITES",             /* count of split collective MPI writes */
    "CP_NB_READS",                 /* count of nonblocking MPI reads */
    "CP_NB_WRITES",                /* count of nonblocking MPI writes */
    "CP_SYNCS",                    /* count of MPI_File_sync */
    "CP_POSIX_READS",              /* count of posix reads */
    "CP_POSIX_WRITES",             /* count of posix writes */
    "CP_POSIX_OPENS",              /* count of posix opens */
    "CP_POSIX_SEEKS",              /* count of posix seeks */
    "CP_POSIX_STATS",              /* count of posix stat/lstat/fstats */
    "CP_POSIX_MMAPS",              /* count of posix mmaps */
    "CP_POSIX_FREADS",
    "CP_POSIX_FWRITES",
    "CP_POSIX_FOPENS",
    "CP_POSIX_FSEEKS",
    "CP_POSIX_FSYNCS",
    "CP_POSIX_FDSYNCS",
    "CP_INDEP_NC_OPENS",
    "CP_COLL_NC_OPENS",
    "CP_HDF5_OPENS",
    "CP_COMBINER_NAMED",           /* count of each MPI datatype category */
    "CP_COMBINER_DUP",
    "CP_COMBINER_CONTIGUOUS",
    "CP_COMBINER_VECTOR",
    "CP_COMBINER_HVECTOR_INTEGER",
    "CP_COMBINER_HVECTOR",
    "CP_COMBINER_INDEXED",
    "CP_COMBINER_HINDEXED_INTEGER",
    "CP_COMBINER_HINDEXED",
    "CP_COMBINER_INDEXED_BLOCK",
    "CP_COMBINER_STRUCT_INTEGER",
    "CP_COMBINER_STRUCT",
    "CP_COMBINER_SUBARRAY",
    "CP_COMBINER_DARRAY",
    "CP_COMBINER_F90_REAL",
    "CP_COMBINER_F90_COMPLEX",
    "CP_COMBINER_F90_INTEGER",
    "CP_COMBINER_RESIZED",
    "CP_HINTS",                     /* count of MPI hints used */
    "CP_VIEWS",                     /* count of MPI set view calls */
    "CP_MODE",                      /* mode of file */
    "CP_BYTES_READ",                /* total bytes read */
    "CP_BYTES_WRITTEN",             /* total bytes written */
    "CP_MAX_BYTE_READ",             /* highest offset byte read */
    "CP_MAX_BYTE_WRITTEN",          /* highest offset byte written */
    "CP_CONSEC_READS",              /* count of consecutive reads */
    "CP_CONSEC_WRITES",             /* count of consecutive writes */
    "CP_SEQ_READS",                 /* count of sequential reads */
    "CP_SEQ_WRITES",                /* count of sequential writes */
    "CP_RW_SWITCHES",
    "CP_MEM_NOT_ALIGNED",           /* count of accesses not mem aligned */
    "CP_MEM_ALIGNMENT",             /* mem alignment in bytes */
    "CP_FILE_NOT_ALIGNED",          /* count of accesses not file aligned */
    "CP_FILE_ALIGNMENT",            /* file alignment in bytes */
    "CP_MAX_READ_TIME_SIZE",
    "CP_MAX_WRITE_TIME_SIZE",
    "CP_SIZE_READ_0_100",           /* count of posix read size ranges */
    "CP_SIZE_READ_100_1K",
    "CP_SIZE_READ_1K_10K",
    "CP_SIZE_READ_10K_100K",
    "CP_SIZE_READ_100K_1M",
    "CP_SIZE_READ_1M_4M",
    "CP_SIZE_READ_4M_10M",
    "CP_SIZE_READ_10M_100M",
    "CP_SIZE_READ_100M_1G",
    "CP_SIZE_READ_1G_PLUS",
    "CP_SIZE_WRITE_0_100",          /* count of posix write size ranges */
    "CP_SIZE_WRITE_100_1K",
    "CP_SIZE_WRITE_1K_10K",
    "CP_SIZE_WRITE_10K_100K",
    "CP_SIZE_WRITE_100K_1M",
    "CP_SIZE_WRITE_1M_4M",
    "CP_SIZE_WRITE_4M_10M",
    "CP_SIZE_WRITE_10M_100M",
    "CP_SIZE_WRITE_100M_1G",
    "CP_SIZE_WRITE_1G_PLUS",
    "CP_SIZE_READ_AGG_0_100",       /* count of MPI read size ranges */
    "CP_SIZE_READ_AGG_100_1K",
    "CP_SIZE_READ_AGG_1K_10K",
    "CP_SIZE_READ_AGG_10K_100K",
    "CP_SIZE_READ_AGG_100K_1M",
    "CP_SIZE_READ_AGG_1M_4M",
    "CP_SIZE_READ_AGG_4M_10M",
    "CP_SIZE_READ_AGG_10M_100M",
    "CP_SIZE_READ_AGG_100M_1G",
    "CP_SIZE_READ_AGG_1G_PLUS",
    "CP_SIZE_WRITE_AGG_0_100",      /* count of MPI write size ranges */
    "CP_SIZE_WRITE_AGG_100_1K",
    "CP_SIZE_WRITE_AGG_1K_10K",
    "CP_SIZE_WRITE_AGG_10K_100K",
    "CP_SIZE_WRITE_AGG_100K_1M",
    "CP_SIZE_WRITE_AGG_1M_4M",
    "CP_SIZE_WRITE_AGG_4M_10M",
    "CP_SIZE_WRITE_AGG_10M_100M",
    "CP_SIZE_WRITE_AGG_100M_1G",
    "CP_SIZE_WRITE_AGG_1G_PLUS",
    "CP_EXTENT_READ_0_100",          /* count of MPI read extent ranges */
    "CP_EXTENT_READ_100_1K",
    "CP_EXTENT_READ_1K_10K",
    "CP_EXTENT_READ_10K_100K",
    "CP_EXTENT_READ_100K_1M",
    "CP_EXTENT_READ_1M_4M",
    "CP_EXTENT_READ_4M_10M",
    "CP_EXTENT_READ_10M_100M",
    "CP_EXTENT_READ_100M_1G",
    "CP_EXTENT_READ_1G_PLUS",
    "CP_EXTENT_WRITE_0_100",         /* count of MPI write extent ranges */
    "CP_EXTENT_WRITE_100_1K",
    "CP_EXTENT_WRITE_1K_10K",
    "CP_EXTENT_WRITE_10K_100K",
    "CP_EXTENT_WRITE_100K_1M",
    "CP_EXTENT_WRITE_1M_4M",
    "CP_EXTENT_WRITE_4M_10M",
    "CP_EXTENT_WRITE_10M_100M",
    "CP_EXTENT_WRITE_100M_1G",
    "CP_EXTENT_WRITE_1G_PLUS",
    "CP_STRIDE1_STRIDE",             /* the four most frequently appearing strides */
    "CP_STRIDE2_STRIDE",
    "CP_STRIDE3_STRIDE",
    "CP_STRIDE4_STRIDE",
    "CP_STRIDE1_COUNT",              /* count of each of the most frequent strides */
    "CP_STRIDE2_COUNT",
    "CP_STRIDE3_COUNT",
    "CP_STRIDE4_COUNT",
    "CP_ACCESS1_ACCESS",
    "CP_ACCESS2_ACCESS",
    "CP_ACCESS3_ACCESS",
    "CP_ACCESS4_ACCESS",
    "CP_ACCESS1_COUNT",
    "CP_ACCESS2_COUNT",
    "CP_ACCESS3_COUNT",
    "CP_ACCESS4_COUNT",
    "CP_DEVICE",
    "CP_SIZE_AT_OPEN",
    "CP_FASTEST_RANK",
    "CP_FASTEST_RANK_BYTES",
    "CP_SLOWEST_RANK",
    "CP_SLOWEST_RANK_BYTES",

    "CP_NUM_INDICES"
};

/* isn't there a clever c way to avoid this? */
char *darshan_f_names[] = {
    "CP_F_OPEN_TIMESTAMP",        /* timestamp of first open */
    "CP_F_READ_START_TIMESTAMP",  /* timestamp of first read */
    "CP_F_WRITE_START_TIMESTAMP", /* timestamp of first write */
    "CP_F_CLOSE_TIMESTAMP",       /* timestamp of last close */
    "CP_F_READ_END_TIMESTAMP",    /* timestamp of last read */
    "CP_F_WRITE_END_TIMESTAMP",   /* timestamp of last write */
    "CP_F_POSIX_READ_TIME",       /* cumulative posix read time */
    "CP_F_POSIX_WRITE_TIME",      /* cumulative posix write time */
    "CP_F_POSIX_META_TIME",       /* cumulative posix meta time */
    "CP_F_MPI_META_TIME",         /* cumulative mpi-io metadata time */
    "CP_F_MPI_READ_TIME",         /* cumulative mpi-io read time */
    "CP_F_MPI_WRITE_TIME",        /* cumulative mpi-io write time */
    "CP_F_MAX_READ_TIME",
    "CP_F_MAX_WRITE_TIME",
    "CP_F_FASTEST_RANK_TIME",
    "CP_F_SLOWEST_RANK_TIME",
    "CP_F_VARIANCE_RANK_TIME",
    "CP_F_VARIANCE_RANK_BYTES",

    "CP_F_NUM_INDICES"
};

/* function pointers so that we can switch functions depending on what file
 * version is detected
 */
int (*getjob_internal)(darshan_fd file, struct darshan_job *job);
int (*getfile_internal)(darshan_fd fd, 
    struct darshan_job *job, 
    struct darshan_file *file);
#define JOB_SIZE_124 28

/* internal routines for parsing different file versions */
static int getjob_internal_200(darshan_fd file, struct darshan_job *job);
static int getfile_internal_200(darshan_fd fd, struct darshan_job *job, 
    struct darshan_file *file);
static int getjob_internal_124(darshan_fd file, struct darshan_job *job);
static int getfile_internal_124(darshan_fd fd, struct darshan_job *job, 
    struct darshan_file *file);
static int getfile_internal_122(darshan_fd fd, struct darshan_job *job, 
    struct darshan_file *file);
static int getfile_internal_121(darshan_fd fd, struct darshan_job *job, 
    struct darshan_file *file);
static int getfile_internal_1x(darshan_fd fd, struct darshan_job *job, 
    struct darshan_file *file, int n_counters, int n_fcounters);
static void shift_missing_1_24(struct darshan_file* file);
static void shift_missing_1_22(struct darshan_file* file);
static void shift_missing_1_21(struct darshan_file* file);

/* a rather crude API for accessing raw binary darshan files */
darshan_fd darshan_log_open(const char *name)
{
    darshan_fd tmp_fd = malloc(sizeof(*tmp_fd));
    if(!tmp_fd)
        return(NULL);

    memset(tmp_fd, 0, sizeof(*tmp_fd));

    tmp_fd->gzf = gzopen(name, "r");
    if(!tmp_fd->gzf)
    {
        free(tmp_fd);
        tmp_fd = NULL;
    }
    return tmp_fd;
}

/* darshan_log_getjob()
 *
 * returns 0 on success, -1 on failure
 */
int darshan_log_getjob(darshan_fd file, struct darshan_job *job)
{
    int ret;

    gzseek(file->gzf, 0, SEEK_SET);

    /* read version number first so we know how to digest the rest of the
     * file
     */
    ret = gzread(file->gzf, file->version, 10);
    if(ret < 10)
    {
        fprintf(stderr, "Error: invalid log file (failed to read version).\n");
        return(-1);
    }

    if(strcmp(file->version, "2.00") == 0)
    {
        getjob_internal = getjob_internal_200;
        getfile_internal = getfile_internal_200;
        file->job_struct_size = sizeof(*job);
    }
    else if(strcmp(file->version, "1.24") == 0)
    {
        getjob_internal = getjob_internal_124;
        getfile_internal = getfile_internal_124;
        file->job_struct_size = JOB_SIZE_124;
    }
    else if(strcmp(file->version, "1.23") == 0)
    {
        /* same as 1.24, except that mnt points may be incorrect */
        getjob_internal = getjob_internal_124;
        getfile_internal = getfile_internal_124;
        file->job_struct_size = JOB_SIZE_124;
    }
    else if(strcmp(file->version, "1.22") == 0)
    {
        getjob_internal = getjob_internal_124;
        getfile_internal = getfile_internal_122;
        file->job_struct_size = JOB_SIZE_124;
    }
    else if(strcmp(file->version, "1.21") == 0)
    {
        getjob_internal = getjob_internal_124;
        getfile_internal = getfile_internal_121;
        file->job_struct_size = JOB_SIZE_124;
    }
    else
    {
        fprintf(stderr, "Error: incompatible darshan file.\n");
        fprintf(stderr, "Error: expected version %s, but got %s\n", 
                CP_VERSION, file->version);
        return(-1);
    }

    ret = getjob_internal(file, job);
    return(ret);
}

/* darshan_log_getfile()
 *
 * return 1 if file record found, 0 on eof, and -1 on error
 */
int darshan_log_getfile(darshan_fd fd, struct darshan_job *job, struct darshan_file *file)
{
    int ret;

    ret = getfile_internal(fd, job, file);

    return(ret);
}

/* darshan_log_getmounts()
 * 
 * retrieves mount table information from the log.  Note that devs, mnt_pts,
 * and fs_types are arrays that will be allocated by the function and must
 * be freed by the caller.  count will indicate the size of the arrays
 */
int darshan_log_getmounts(darshan_fd fd, int64_t** devs, char*** mnt_pts, char***
    fs_types, int* count, int *flag)
{
    int ret;
    char* pos;
    int array_index = 0;
    char buf[CP_EXE_LEN+1];

    gzseek(fd->gzf, fd->job_struct_size, SEEK_SET);

    ret = gzread(fd->gzf, buf, (CP_EXE_LEN + 1));
    if (ret < (CP_EXE_LEN + 1))
    {
        perror("gzread");
        return(-1);
    }
    if (gzeof(fd->gzf))
        *flag = 1;
    else
        *flag = 0;

    /* count entries */
    *count = 0;
    pos = buf;
    while((pos = strchr(pos, '\n')) != NULL)
    {
        pos++;
        (*count)++;
    }

    if(*count == 0)
    {
        /* no mount entries present */
        return(0);
    }

    /* allocate output arrays */
    *devs = malloc((*count)*sizeof(int64_t));
    assert(*devs);
    *mnt_pts = malloc((*count)*sizeof(char*));
    assert(*mnt_pts);
    *fs_types = malloc((*count)*sizeof(char*));
    assert(*fs_types);
    
    /* work backwards through the table and parse each line (except for
     * first, which holds command line information)
     */
    while((pos = strrchr(buf, '\n')) != NULL)
    {
        /* overestimate string lengths */
        (*mnt_pts)[array_index] = malloc(CP_EXE_LEN);
        assert((*mnt_pts)[array_index]);
        (*fs_types)[array_index] = malloc(CP_EXE_LEN);
        assert((*fs_types)[array_index]);
        
#if SIZEOF_LONG_INT == 4
        ret = sscanf(++pos, "%lld\t%s\t%s", &(*devs)[array_index],
            (*fs_types)[array_index], (*mnt_pts)[array_index]);
#elif SIZEOF_LONG_INT == 8
        ret = sscanf(++pos, "%ld\t%s\t%s", &(*devs)[array_index],
            (*fs_types)[array_index], (*mnt_pts)[array_index]);
#else
#  error Unexpected sizeof(long int)
#endif

        if(ret != 3)
        {
            fprintf(stderr, "Error: poorly formatted mount table in log file.\n");
            return(-1);
        }
        pos--;
        *pos = '\0';
        array_index++;
    }
   
    return (0);
}


int darshan_log_getexe(darshan_fd fd, char *buf, int *flag)
{
    int ret;
    char* newline;

    gzseek(fd->gzf, fd->job_struct_size, SEEK_SET);

    ret = gzread(fd->gzf, buf, (CP_EXE_LEN + 1));
    if (ret < (CP_EXE_LEN + 1))
    {
        perror("gzread");
        return(-1);
    }
    if (gzeof(fd->gzf))
        *flag = 1;
    else
        *flag = 0;

    /* this call is only supposed to return the exe string, but starting in
     * log format 1.23 there could be a table of mount entry information
     * after the exe.  Look for newline character and truncate there.
     */
    newline = strchr(buf, '\n');
    if(newline)
        *newline = '\0';

    return (0);
}

void darshan_log_close(darshan_fd file)
{
    gzclose(file->gzf);
    free(file);
}

/* darshan_log_print_version_warnings()
 *
 * Print summary of any problems with the detected log format
 */
void darshan_log_print_version_warnings(struct darshan_job *job)
{
    if(strcmp(job->version_string, "2.00") == 0)
    {
        /* current version */
        return;
    }
 
    if(strcmp(job->version_string, "1.24") == 0)
    {
        printf("# WARNING: version 1.24 log format does not store the job id in the log file.\n");
        return;
    }
    
    if(strcmp(job->version_string, "1.23") == 0)
    {
        printf("# WARNING: version 1.23 log format may have incorrect mount point mappings for files with rank > 0\n");
        printf("# It also does not store the job id in the log file.\n");
        return;
    }

    if(strcmp(job->version_string, "1.22") == 0)
    {
        printf("# WARNING: version 1.22 log format does not support the following parameters:\n");
        printf("#   CP_DEVICE\n");
        printf("#   CP_SIZE_AT_OPEN\n");
        printf("# It does not record mounted file systems, mount points, or fs types.\n");
        printf("# It attributes syncs to cumulative metadata time, rather than cumulative write time.\n");
        printf("# It also does not store the job id in the log file.\n");
        return;
    }

    if(strcmp(job->version_string, "1.21") == 0)
    {
        printf("# WARNING: version 1.21 log format does not support the following parameters:\n");
        printf("#   CP_INDEP_NC_OPENS\n");
        printf("#   CP_COLL_NC_OPENS\n");
        printf("#   CP_HDF5_OPENS\n");
        printf("#   CP_MAX_READ_TIME_SIZE\n");
        printf("#   CP_MAX_WRITE_TIME_SIZE\n");
        printf("#   CP_DEVICE\n");
        printf("#   CP_SIZE_AT_OPEN\n");
        printf("#   CP_F_MAX_READ_TIME\n");
        printf("#   CP_F_MAX_WRITE_TIME\n");
        printf("# It does not record mounted file systems, mount points, or fs types.\n");
        printf("# It attributes syncs to cumulative metadata time, rather than cumulative write time.\n");
        printf("#\n");
        printf("# It also does not store the job id in the file.\n");
        return;
    }

    fprintf(stderr, "Error: version %s not supported by parser.\n",
        job->version_string);
    return;
}

/* shift_missing_1_21()
 *
 * translates indices to account for counters that weren't present in log
 * format 1.21
 */
/*******************************
 * version 1.21 to 2.00 differences 
 * - added:
 *   - CP_INDEP_NC_OPENS
 *   - CP_COLL_NC_OPENS
 *   - CP_HDF5_OPENS
 *   - CP_MAX_READ_TIME_SIZE
 *   - CP_MAX_WRITE_TIME_SIZE
 *   - CP_DEVICE
 *   - CP_SIZE_AT_OPEN
 *   - CP_FASTEST_RANK
 *   - CP_FASTEST_RANK_BYTES
 *   - CP_SLOWEST_RANK
 *   - CP_SLOWEST_RANK_BYTES
 *   - CP_F_MAX_READ_TIME
 *   - CP_F_MAX_WRITE_TIME
 *   - CP_F_FASTEST_RANK_TIME
 *   - CP_F_SLOWEST_RANK_TIME
 *   - CP_F_VARIANCE_RANK_TIME
 *   - CP_F_VARIANCE_RANK_BYTES
 * - changed params:
 *   - CP_FILE_RECORD_SIZE: 1184 to 1328
 *   - CP_NUM_INDICES: 133 to 144
 *   - CP_F_NUM_INDICES: 12 to 18
 */
static void shift_missing_1_21(struct darshan_file* file)
{
    int c_index = 0;
    int missing_counters[] = {
        CP_INDEP_NC_OPENS,
        CP_COLL_NC_OPENS,
        CP_HDF5_OPENS,
        CP_MAX_READ_TIME_SIZE,
        CP_MAX_WRITE_TIME_SIZE,
        CP_DEVICE,
        CP_SIZE_AT_OPEN,
        CP_FASTEST_RANK,
        CP_FASTEST_RANK_BYTES,
        CP_SLOWEST_RANK,
        CP_SLOWEST_RANK_BYTES,
        -1};
    int missing_f_counters[] = {
        CP_F_MAX_READ_TIME,
        CP_F_MAX_WRITE_TIME,
        CP_F_FASTEST_RANK_TIME,
        CP_F_SLOWEST_RANK_TIME,
        CP_F_VARIANCE_RANK_TIME,
        CP_F_VARIANCE_RANK_BYTES,
        -1};

    c_index = 0;
    while(missing_counters[c_index] != -1)
    {
        int missing_counter = missing_counters[c_index];
        c_index++;
        if(missing_counter < (CP_NUM_INDICES - 1))
        {
            /* shift down */
            memmove(&file->counters[missing_counter+1],
                &file->counters[missing_counter],
                (CP_NUM_INDICES-missing_counter-1)*sizeof(int64_t));
        }
        /* zero out missing counter */
        file->counters[missing_counter] = 0;
    }

    c_index = 0;
    while(missing_f_counters[c_index] != -1)
    {
        int missing_counter = missing_f_counters[c_index];
        c_index++;
        if(missing_counter < (CP_F_NUM_INDICES - 1))
        {
            /* shift down */
            memmove(&file->fcounters[missing_counter+1],
                &file->fcounters[missing_counter],
                (CP_F_NUM_INDICES-missing_counter-1)*sizeof(double));
        }
        /* zero out missing counter */
        file->fcounters[missing_counter] = 0;
    }

    return;
}

/* shift_missing_1_22()
 *
 * translates indices to account for counters that weren't present in log
 * format 1.22
 */
/*******************************
 * version 1.22 to 2.00 differences
 *
 * - added:
 *   - CP_DEVICE
 *   - CP_SIZE_AT_OPEN
 *   - CP_FASTEST_RANK
 *   - CP_FASTEST_RANK_BYTES
 *   - CP_SLOWEST_RANK
 *   - CP_SLOWEST_RANK_BYTES
 *   - CP_F_FASTEST_RANK_TIME
 *   - CP_F_SLOWEST_RANK_TIME
 *   - CP_F_VARIANCE_RANK_TIME
 *   - CP_F_VARIANCE_RANK_BYTES
 * - changed params:
 *   - CP_FILE_RECORD_SIZE: 1240 to 1328
 *   - CP_NUM_INDICES: 138 to 144
 *   - CP_F_NUM_INDICES: 14 to 18
 */
static void shift_missing_1_22(struct darshan_file* file)
{
    int c_index = 0;
    int missing_counters[] = {
        CP_DEVICE,
        CP_SIZE_AT_OPEN,
        CP_FASTEST_RANK,
        CP_FASTEST_RANK_BYTES,
        CP_SLOWEST_RANK,
        CP_SLOWEST_RANK_BYTES,
        -1};
    int missing_f_counters[] = {
        CP_F_FASTEST_RANK_TIME,
        CP_F_SLOWEST_RANK_TIME,
        CP_F_VARIANCE_RANK_TIME,
        CP_F_VARIANCE_RANK_BYTES,
        -1};

    c_index = 0;
    while(missing_counters[c_index] != -1)
    {
        int missing_counter = missing_counters[c_index];
        c_index++;
        if(missing_counter < (CP_NUM_INDICES - 1))
        {
            /* shift down */
            memmove(&file->counters[missing_counter+1],
                &file->counters[missing_counter],
                (CP_NUM_INDICES-missing_counter-1)*sizeof(int64_t));
        }
        /* zero out missing counter */
        file->counters[missing_counter] = 0;
    }

    c_index = 0;
    while(missing_f_counters[c_index] != -1)
    {
        int missing_counter = missing_f_counters[c_index];
        c_index++;
        if(missing_counter < (CP_F_NUM_INDICES - 1))
        {
            /* shift down */
            memmove(&file->fcounters[missing_counter+1],
                &file->fcounters[missing_counter],
                (CP_F_NUM_INDICES-missing_counter-1)*sizeof(double));
        }
        /* zero out missing counter */
        file->fcounters[missing_counter] = 0;
    }

    return;
}

/* shift_missing_1_24()
 *
 * translates indices to account for counters that weren't present in log
 * format 1.24
 */
/*******************************
 * version 1.24 to 2.00 differences
 *
 * - added:
 *   - CP_F_FASTEST_RANK_TIME
 *   - CP_F_SLOWEST_RANK_TIME
 *   - CP_F_VARIANCE_RANK_TIME
 *   - CP_F_VARIANCE_RANK_BYTES
 * - changed params:
 *   - CP_FILE_RECORD_SIZE: ? to 1328
 *   - CP_NUM_INDICES: 140 to 144
 *   - CP_F_NUM_INDICES: 14 to 18
 */
static void shift_missing_1_24(struct darshan_file* file)
{
    int c_index = 0;
    int missing_counters[] = {
        CP_FASTEST_RANK,
        CP_FASTEST_RANK_BYTES,
        CP_SLOWEST_RANK,
        CP_SLOWEST_RANK_BYTES,
        -1};
    int missing_f_counters[] = {
        CP_F_FASTEST_RANK_TIME,
        CP_F_SLOWEST_RANK_TIME,
        CP_F_VARIANCE_RANK_TIME,
        CP_F_VARIANCE_RANK_BYTES,
        -1};

    c_index = 0;
    while(missing_counters[c_index] != -1)
    {
        int missing_counter = missing_counters[c_index];
        c_index++;
        if(missing_counter < (CP_NUM_INDICES - 1))
        {
            /* shift down */
            memmove(&file->counters[missing_counter+1],
                &file->counters[missing_counter],
                (CP_NUM_INDICES-missing_counter-1)*sizeof(int64_t));
        }
        /* zero out missing counter */
        file->counters[missing_counter] = 0;
    }

    c_index = 0;
    while(missing_f_counters[c_index] != -1)
    {
        int missing_counter = missing_f_counters[c_index];
        c_index++;
        if(missing_counter < (CP_F_NUM_INDICES - 1))
        {
            /* shift down */
            memmove(&file->fcounters[missing_counter+1],
                &file->fcounters[missing_counter],
                (CP_F_NUM_INDICES-missing_counter-1)*sizeof(double));
        }
        /* zero out missing counter */
        file->fcounters[missing_counter] = 0;
    }

    return;
}


static int getjob_internal_200(darshan_fd file, struct darshan_job *job)
{
    int ret;

    gzseek(file->gzf, 0, SEEK_SET);

    ret = gzread(file->gzf, job, sizeof(*job));
    if (ret < sizeof(*job))
    {
        if(gzeof(file->gzf))
        {
            fprintf(stderr, "Error: invalid log file (too short).\n");
        }
        perror("darshan_job_init");
        return(-1);
    }

    if(job->magic_nr == CP_MAGIC_NR)
    {
        /* no byte swapping needed, this file is in host format already */
        file->swap_flag = 0;
        return(0);
    }

    /* try byte swapping */
    DARSHAN_BSWAP64(&job->magic_nr);
    if(job->magic_nr == CP_MAGIC_NR)
    {
        file->swap_flag = 1;
        DARSHAN_BSWAP64(&job->uid);
        DARSHAN_BSWAP64(&job->start_time);
        DARSHAN_BSWAP64(&job->end_time);
        DARSHAN_BSWAP64(&job->nprocs);
        DARSHAN_BSWAP64(&job->jobid);
        return(0);
    }

    /* otherwise this file is just broken */
    fprintf(stderr, "Error: bad magic number in darshan file.\n");
    return(-1);
}

static int getfile_internal_200(darshan_fd fd, struct darshan_job *job, 
    struct darshan_file *file)
{
    int ret;
    const char* err_string;
    int i;
    
    if(gztell(fd->gzf) < CP_JOB_RECORD_SIZE)
        gzseek(fd->gzf, CP_JOB_RECORD_SIZE, SEEK_SET);

    /* reset file record, so that diff compares against a zero'd out record
     * if file is missing
     */
    memset(file, 0, sizeof(&file));

    ret = gzread(fd->gzf, file, sizeof(*file));
    if(ret == sizeof(*file))
    {
        /* got exactly one, correct size record */
        if(fd->swap_flag)
        {
            /* swap bytes if necessary */
            DARSHAN_BSWAP64(&file->hash);
            DARSHAN_BSWAP64(&file->rank);
            for(i=0; i<CP_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->counters[i]);
            for(i=0; i<CP_F_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->fcounters[i]);
        }
        return(1);
    }


    if(ret > 0)
    {
        /* got a short read */
        fprintf(stderr, "Error: invalid file record (too small)\n");
        return(-1);
    }

    if(ret == 0 && gzeof(fd->gzf))
    {
        /* hit end of file */
        return(0);
    }

    /* all other errors */
    err_string = gzerror(fd->gzf, &ret);
    fprintf(stderr, "Error: %s\n", err_string);
    return(-1);
}

/* If we see version 1.24, assume that it is stored in big endian 32 bit
 * format.  Convert up to current format.
 */
static int getjob_internal_124(darshan_fd fd, struct darshan_job *job)
{
    char* buffer;
    int ret;
    uint32_t uid;
    int32_t start_time;
    int32_t end_time;
    int32_t nprocs;

#ifdef WORDS_BIGENDIAN
    fd->swap_flag = 0;
#else
    fd->swap_flag = 1;
#endif

    memset(job, 0, sizeof(*job));

    buffer = (char*)malloc(JOB_SIZE_124);
    if(!buffer)
    {
        return(-1);
    }

    gzseek(fd->gzf, 0, SEEK_SET);

    ret = gzread(fd->gzf, buffer, JOB_SIZE_124);
    if (ret < JOB_SIZE_124)
    {
        fprintf(stderr, "Error: invalid log file (could not read file record).\n");
        free(buffer);
        return(-1);
    }

    /* pull job header information out of specific bytes in case struct
     * padding is off
     */
    strncpy(job->version_string, buffer, 8);
    uid = *((uint32_t*)&buffer[12]);
    start_time = *((int32_t*)&buffer[16]);
    end_time = *((int32_t*)&buffer[20]);
    nprocs = *((int32_t*)&buffer[24]);

    free(buffer);

    if(fd->swap_flag)
    {
        /* byte swap */
        DARSHAN_BSWAP32(&uid);
        DARSHAN_BSWAP32(&start_time);
        DARSHAN_BSWAP32(&end_time);
        DARSHAN_BSWAP32(&nprocs);
    }

    job->uid += uid;
    job->start_time += start_time;
    job->end_time += end_time;
    job->nprocs += nprocs;
    job->jobid = 0; /* old log versions did not have this field */
    
    /* set magic number */
    job->magic_nr = CP_MAGIC_NR;

    return(0);
}

static int getfile_internal_124(darshan_fd fd, struct darshan_job *job, 
    struct darshan_file *file)
{
    int ret;

    ret = getfile_internal_1x(fd, job, file, 138, 14);
    if(ret <= 0)
        return(ret);

    shift_missing_1_24(file);

    return(1);
}

static int getfile_internal_122(darshan_fd fd, struct darshan_job *job, 
    struct darshan_file *file)
{
    int ret;

    ret = getfile_internal_1x(fd, job, file, 138, 14);
    if(ret <= 0)
        return(ret);

    shift_missing_1_22(file);

    return(1);
}

static int getfile_internal_121(darshan_fd fd, struct darshan_job *job, 
    struct darshan_file *file)
{
    int ret;

    ret = getfile_internal_1x(fd, job, file, 133, 12);
    if(ret <= 0)
        return(ret);

    shift_missing_1_21(file);

    return(1);
}

static int getfile_internal_1x(darshan_fd fd, struct darshan_job *job, 
    struct darshan_file *file, int n_counters, int n_fcounters)
{
    char* buffer;
    int ret;
    const char* err_string;
    int i;
    uint64_t hash;
    int32_t rank;
    int64_t* counters;
    double* fcounters;
    char* name_suffix;
    int FILE_SIZE_1x = (32 + n_counters*8 + n_fcounters*8);

    memset(file, 0, sizeof(*file));

    /* set file pointer if this is the first file record; otherwise pick up
     * where we left off last time
     */
    if(gztell(fd->gzf) < CP_JOB_RECORD_SIZE)
        gzseek(fd->gzf, CP_JOB_RECORD_SIZE, SEEK_SET);

    /* space for file struct, int64 array, and double array */
    buffer = (char*)malloc(FILE_SIZE_1x);
    if(!buffer)
    {
        return(-1);
    }

    ret = gzread(fd->gzf, buffer, FILE_SIZE_1x);

    if(ret > 0 && ret < FILE_SIZE_1x)
    {
        /* got a short read */
        fprintf(stderr, "Error: invalid file record (too small)\n");
        free(buffer);
        return(-1);
    }
    else if(ret == 0 && gzeof(fd->gzf))
    {
        /* hit end of file */
        free(buffer);
        return(0);
    }
    else if(ret <= 0)
    {
        /* all other errors */
        err_string = gzerror(fd->gzf, &ret);
        fprintf(stderr, "Error: %s\n", err_string);
        free(buffer);
        return(-1);
    }

    /* got exactly one, correct size record */
    hash = *((int64_t*)&buffer[0]);
    rank = *((int32_t*)&buffer[8]);
    counters = ((int64_t*)&buffer[16]);
    fcounters = ((double*)&buffer[16 + n_counters*8]);
    name_suffix = &buffer[16 + n_counters*8 + n_fcounters*8];


    if(fd->swap_flag)
    {
        /* swap bytes if necessary */
        DARSHAN_BSWAP64(&hash);
        DARSHAN_BSWAP32(&rank);
        for(i=0; i<n_counters; i++)
            DARSHAN_BSWAP64(&counters[i]);
        for(i=0; i<n_fcounters; i++)
            DARSHAN_BSWAP64(&fcounters[i]);
    }

    /* assign into new format */
    file->hash = hash;
    file->rank += rank;
    memcpy(file->counters, counters, n_counters*8);
    memcpy(file->fcounters, fcounters, n_fcounters*8);
    memcpy(file->name_suffix, name_suffix, 12);

    free(buffer);
    return(1);
}



