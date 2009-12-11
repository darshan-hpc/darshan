/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include "darshan-logutils.h"

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
    "CP_F_NUM_INDICES",
};

/*******************************
 * version 1.22 to 1.23 differences
 *
 * - added:
 *   - CP_DEVICE
 * - changed params:
 *   - CP_FILE_RECORD_SIZE: 1240 to 1244
 *   - CP_NUM_INDICES: 138 to 139
 */
#define CP_NUM_INDICES_1_22 138
struct darshan_file_1_22
{
    uint64_t hash;
    int rank;
    int64_t counters[CP_NUM_INDICES_1_22];
    double fcounters[CP_F_NUM_INDICES];
    char name_suffix[CP_NAME_SUFFIX_LEN+1];
};

static void shift_missing_1_22(struct darshan_file* file);


/*******************************
 * version 1.21 to 1.23 differences 
 * - added:
 *   - CP_INDEP_NC_OPENS
 *   - CP_COLL_NC_OPENS
 *   - CP_HDF5_OPENS
 *   - CP_MAX_READ_TIME_SIZE
 *   - CP_MAX_WRITE_TIME_SIZE
 *   - CP_DEVICE
 *   - CP_F_MAX_READ_TIME
 *   - CP_F_MAX_WRITE_TIME
 * - changed params:
 *   - CP_FILE_RECORD_SIZE: 1184 to 1244
 *   - CP_NUM_INDICES: 133 to 139
 *   - CP_F_NUM_INDICES: 12 to 14
 * - so 60 bytes worth of new indices are the only difference
 */
#define CP_NUM_INDICES_1_21 133
#define CP_F_NUM_INDICES_1_21 12
struct darshan_file_1_21
{
    uint64_t hash;
    int rank;
    int64_t counters[CP_NUM_INDICES_1_21];
    double fcounters[CP_F_NUM_INDICES_1_21];
    char name_suffix[CP_NAME_SUFFIX_LEN+1];
};

static void shift_missing_1_21(struct darshan_file* file);

/* a rather crude API for accessing raw binary darshan files */
darshan_fd darshan_log_open(const char *name)
{
    return gzopen(name, "r");
}

/* darshan_log_getjob()
 *
 * returns 0 on success, -1 on failure
 */
int darshan_log_getjob(darshan_fd file, struct darshan_job *job)
{
    int ret;

    gzseek(file, 0, SEEK_SET);

    ret = gzread(file, job, sizeof(*job));
    if (ret < sizeof(*job))
    {
        if(gzeof(file))
        {
            fprintf(stderr, "Error: invalid log file (too short).\n");
        }
        perror("darshan_job_init");
        return(-1);
    }
    if(strcmp(job->version_string, "1.21"))
        return(0);
    if(strcmp(job->version_string, "1.22"))
        return(0);

    fprintf(stderr, "Error: incompatible darshan file.\n");
    fprintf(stderr, "Error: expected version %s, but got %s\n", 
            CP_VERSION, job->version_string);
    return(-1);
}

/* darshan_log_getfile()
 *
 * return 1 if file record found, 0 on eof, and -1 on error
 */
int darshan_log_getfile(darshan_fd fd, struct darshan_job *job, struct darshan_file *file)
{
    int ret;
    const char* err_string;
    struct darshan_file_1_21 file_1_21;
    struct darshan_file_1_22 file_1_22;
    
    if(gztell(file) < CP_JOB_RECORD_SIZE)
        gzseek(file, CP_JOB_RECORD_SIZE, SEEK_SET);

    gzseek(file, sizeof(struct darshan_job), SEEK_SET);

    /* reset file record, so that diff compares against a zero'd out record
     * if file is missing
     */
    memset(file, 0, sizeof(&file));

    if(strcmp(job->version_string, "1.21") == 0)
    {
        ret = gzread(fd, &file_1_21, sizeof(file_1_21));
        if(ret == sizeof(file_1_21))
        {
            /* convert to new file record structure */
            file->hash = file_1_21.hash;
            file->rank = file_1_21.rank;
            strcpy(file->name_suffix, file_1_21.name_suffix);
            memcpy(file->counters, file_1_21.counters,
                (CP_NUM_INDICES_1_21*sizeof(int64_t)));
            memcpy(file->fcounters, file_1_21.fcounters,
                (CP_F_NUM_INDICES_1_21*sizeof(double)));
            shift_missing_1_21(file);

            /* got exactly one, correct size record */
            return(1);
        }
    }
    else if(strcmp(job->version_string, "1.22") == 0)
    {
        ret = gzread(fd, &file_1_22, sizeof(file_1_22));
        if(ret == sizeof(file_1_22))
        {
            /* convert to new file record structure */
            file->hash = file_1_22.hash;
            file->rank = file_1_22.rank;
            strcpy(file->name_suffix, file_1_22.name_suffix);
            memcpy(file->counters, file_1_22.counters,
                (CP_NUM_INDICES_1_22*sizeof(int64_t)));
            memcpy(file->fcounters, file_1_22.fcounters,
                (CP_F_NUM_INDICES*sizeof(double)));
            shift_missing_1_22(file);

            /* got exactly one, correct size record */
            return(1);
        }
    }
    else if(strcmp(job->version_string, "1.23") == 0)
    {
        /* make sure this is the current version */
        assert(strcmp("1.23", CP_VERSION) == 0);

        ret = gzread(fd, file, sizeof(*file));
        if(ret == sizeof(*file))
        {
            /* got exactly one, correct size record */
            return(1);
        }
    }
    else
    {
        fprintf(stderr, "Error: invalid log file version.\n");
        return(-1);
    }

    if(ret > 0)
    {
        /* got a short read */
        fprintf(stderr, "Error: invalid file record (too small)\n");
        return(-1);
    }

    if(ret == 0 && gzeof(fd))
    {
        /* hit end of file */
        return(0);
    }

    /* all other errors */
    err_string = gzerror(fd, &ret);
    fprintf(stderr, "Error: %s\n", err_string);
    return(-1);
}

/* darshan_log_getmounts()
 * 
 * retrieves mount table information from the log.  Note that devs, mnt_pts,
 * and fs_types are arrays that will be allocated by the function and must
 * be freed by the caller.  count will indicate the size of the arrays
 */
int darshan_log_getmounts(darshan_fd fd, int** devs, char*** mnt_pts, char***
    fs_types, int* count, int *flag)
{
    int ret;
    char* pos;
    int array_index = 0;
    char buf[CP_EXE_LEN+1];

    gzseek(fd, sizeof(struct darshan_job), SEEK_SET);

    ret = gzread(fd, buf, (CP_EXE_LEN + 1));
    if (ret < (CP_EXE_LEN + 1))
    {
        perror("gzread");
        return(-1);
    }
    if (gzeof(fd))
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
    *devs = malloc((*count)*sizeof(int));
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
        
        ret = sscanf(++pos, "%d\t%s\t%s", &(*devs)[array_index],
            (*fs_types)[array_index], (*mnt_pts)[array_index]);
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

    gzseek(fd, sizeof(struct darshan_job), SEEK_SET);

    ret = gzread(fd, buf, (CP_EXE_LEN + 1));
    if (ret < (CP_EXE_LEN + 1))
    {
        perror("gzread");
        return(-1);
    }
    if (gzeof(fd))
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
    gzclose(file);
}

/* darshan_log_print_version_warnings()
 *
 * Print summary of any problems with the detected log format
 */
void darshan_log_print_version_warnings(struct darshan_job *job)
{
    if(strcmp(job->version_string, "1.23") == 0)
    {
        /* nothing to do, this is the current version */
        return;
    }
    
    if(strcmp(job->version_string, "1.22") == 0)
    {
        printf("# WARNING: version 1.22 log format does not support the following parameters:\n");
        printf("#   CP_DEVICE\n");
        printf("# It also does not record mounted file systems, mount points, or fs types.\n");
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
        printf("#   CP_F_MAX_READ_TIME\n");
        printf("#   CP_F_MAX_WRITE_TIME\n");
        printf("# It also does not record mounted file systems, mount points, or fs types.\n");
        printf("#\n");
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
        -1};
    int missing_f_counters[] = {
        CP_F_MAX_READ_TIME,
        CP_F_MAX_WRITE_TIME,
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
static void shift_missing_1_22(struct darshan_file* file)
{
    int c_index = 0;
    int missing_counters[] = {
        CP_DEVICE,
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

    return;
}
