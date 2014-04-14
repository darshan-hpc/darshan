/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#define _GNU_SOURCE
#include "darshan-util-config.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <zlib.h>
#ifdef HAVE_LIBBZ2
#include <bzlib.h>
#endif

#include "darshan-logutils.h"

struct darshan_fd_s
{
    gzFile gzf;
#ifdef HAVE_LIBBZ2
    BZFILE* bzf;
#endif
    int64_t pos;
    char mode[2];
    int swap_flag;
    char version[10];
    int job_struct_size;
    char* name;
    int COMPAT_CP_EXE_LEN;
};

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
#define JOB_SIZE_200 56
#define JOB_SIZE_201 120
#define CP_JOB_RECORD_SIZE_200 1024
#define CP_JOB_RECORD_SIZE_1x 1024

/* internal routines for parsing different file versions */
static int getjob_internal_204(darshan_fd file, struct darshan_job *job);
static int getjob_internal_201(darshan_fd file, struct darshan_job *job);
static int getjob_internal_200(darshan_fd file, struct darshan_job *job);
static int getfile_internal_204(darshan_fd fd, struct darshan_job *job, 
    struct darshan_file *file);
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

static int darshan_log_seek(darshan_fd fd, int64_t offset);
static int darshan_log_read(darshan_fd fd, void* buf, int len);
static int darshan_log_write(darshan_fd fd, void* buf, int len);
static const char* darshan_log_error(darshan_fd fd, int* errnum);

/* a rather crude API for accessing raw binary darshan files */
darshan_fd darshan_log_open(const char *name, const char* mode)
{
    int test_fd;
    uint8_t magic[2];
    int ret;
    int try_bz2 = 1;
    int len = strlen(name);

    /* we only allows "w" or "r" modes, nothing fancy */
    assert(strlen(mode) == 1);
    assert(mode[0] == 'r' || mode[0] == 'w');

    darshan_fd tmp_fd = malloc(sizeof(*tmp_fd));
    if(!tmp_fd)
        return(NULL);
    memset(tmp_fd, 0, sizeof(*tmp_fd));

    tmp_fd->mode[0] = mode[0];
    tmp_fd->mode[1] = mode[1];
    tmp_fd->name  = strdup(name);
    if(!tmp_fd->name)
    {
        free(tmp_fd);
        return(NULL);
    }

    if(strcmp(mode, "r") == 0)
    {
        /* Try to detect if existing file is a bzip2 file or not.  Both 
         * libbz2 and libz will fall back to normal I/O (without compression) 
         * automatically, so we need to do some detection manually up front 
         * in order to get a chance to try both compression formats.
         */
        test_fd = open(name, O_RDONLY);
        if(test_fd < 0)
        {
            perror("open");
            free(tmp_fd->name);
            free(tmp_fd);
            return(NULL);
        }
        ret = read(test_fd, &magic, 2);
        if(ret != 2)
        {
            fprintf(stderr, "Error: failed to read any data from %s.\n", 
                name);
            free(tmp_fd->name);
            free(tmp_fd);
            close(test_fd);
            return(NULL);
        }
        /* header magic for bz2 */
        if(magic[0] != 0x42 && magic[1] != 0x5A)
        {
            try_bz2 = 0;
        }
        close(test_fd);
    }

    if(strcmp(mode, "w") == 0)
    {
        /* TODO: is this the behavior that we want? */
        /* if we are writing a new file, go by the file extension to tell
         * whether to use bz2 or not?
         */
        if(len >= 3 && name[len-3] == 'b' && name[len-2] == 'z' && name[len-1] == '2')
            try_bz2 = 1;
        else
            try_bz2 = 0;
    }

#ifdef HAVE_LIBBZ2
    if(try_bz2)
    {
        tmp_fd->bzf = BZ2_bzopen(name, mode);
        if(!tmp_fd->bzf)
        {
            free(tmp_fd->name);
            free(tmp_fd);
            return(NULL);
        }
        return(tmp_fd);
    }
#else
    if(try_bz2)
    {
        fprintf(stderr, "Error: this Darshan build does not support bz2 files.\n");
        fprintf(stderr, "Error: please install libbz2-dev and reconfigure.\n");
        return(NULL);
    }
#endif

    tmp_fd->gzf = gzopen(name, mode);
    if(!tmp_fd->gzf)
    {
        free(tmp_fd->name);
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
    char buffer[DARSHAN_JOB_METADATA_LEN];

    ret = darshan_log_seek(file, 0);
    if(ret < 0)
        return(ret);

    /* read version number first so we know how to digest the rest of the
     * file
     */
    ret = darshan_log_read(file, file->version, 10);
    if(ret < 10)
    {
        fprintf(stderr, "Error: invalid log file (failed to read version).\n");
        return(-1);
    }

    if(strcmp(file->version, "2.05") == 0)
    {
        getjob_internal = getjob_internal_204;
        getfile_internal = getfile_internal_204;
        file->job_struct_size = sizeof(*job);
        file->COMPAT_CP_EXE_LEN = CP_EXE_LEN;
    }
    else if(strcmp(file->version, "2.04") == 0)
    {
        getjob_internal = getjob_internal_204;
        getfile_internal = getfile_internal_204;
        file->job_struct_size = sizeof(*job);
        file->COMPAT_CP_EXE_LEN = CP_EXE_LEN;
    }
    else if(strcmp(file->version, "2.03") == 0)
    {
        getjob_internal = getjob_internal_201;
        getfile_internal = getfile_internal_200;
        file->job_struct_size = JOB_SIZE_201;
        file->COMPAT_CP_EXE_LEN = CP_JOB_RECORD_SIZE_200-file->job_struct_size-1;
    }
    else if(strcmp(file->version, "2.02") == 0)
    {
        getjob_internal = getjob_internal_201;
        getfile_internal = getfile_internal_200;
        file->job_struct_size = JOB_SIZE_201;
        file->COMPAT_CP_EXE_LEN = CP_JOB_RECORD_SIZE_200-file->job_struct_size-1;
    }
    else if(strcmp(file->version, "2.01") == 0)
    {
        getjob_internal = getjob_internal_201;
        getfile_internal = getfile_internal_200;
        file->job_struct_size = JOB_SIZE_201;
        file->COMPAT_CP_EXE_LEN = CP_JOB_RECORD_SIZE_200-file->job_struct_size-1;
    }
    else if(strcmp(file->version, "2.00") == 0)
    {
        getjob_internal = getjob_internal_200;
        getfile_internal = getfile_internal_200;
        file->job_struct_size = JOB_SIZE_200;
        file->COMPAT_CP_EXE_LEN = CP_JOB_RECORD_SIZE_200-file->job_struct_size-1;
    }
    else if(strcmp(file->version, "1.24") == 0)
    {
        getjob_internal = getjob_internal_124;
        getfile_internal = getfile_internal_124;
        file->job_struct_size = JOB_SIZE_124;
        file->COMPAT_CP_EXE_LEN = CP_JOB_RECORD_SIZE_1x-file->job_struct_size-1;
    }
    else if(strcmp(file->version, "1.23") == 0)
    {
        /* same as 1.24, except that mnt points may be incorrect */
        getjob_internal = getjob_internal_124;
        getfile_internal = getfile_internal_124;
        file->job_struct_size = JOB_SIZE_124;
        file->COMPAT_CP_EXE_LEN = CP_JOB_RECORD_SIZE_1x-file->job_struct_size-1;
    }
    else if(strcmp(file->version, "1.22") == 0)
    {
        getjob_internal = getjob_internal_124;
        getfile_internal = getfile_internal_122;
        file->job_struct_size = JOB_SIZE_124;
        file->COMPAT_CP_EXE_LEN = CP_JOB_RECORD_SIZE_1x-file->job_struct_size-1;
    }
    else if(strcmp(file->version, "1.21") == 0)
    {
        getjob_internal = getjob_internal_124;
        getfile_internal = getfile_internal_121;
        file->job_struct_size = JOB_SIZE_124;
        file->COMPAT_CP_EXE_LEN = CP_JOB_RECORD_SIZE_1x-file->job_struct_size-1;
    }
    else
    {
        fprintf(stderr, "Error: incompatible darshan file.\n");
        fprintf(stderr, "Error: expected version %s, but got %s\n", 
                CP_VERSION, file->version);
        return(-1);
    }

    ret = getjob_internal(file, job);

    if (ret == 0)
    {
#ifdef HAVE_STRNDUP
        char *metadata = strndup(job->metadata, sizeof(job->metadata));
#else
        char *metadata = strdup(job->metadata);
#endif
        char *kv;
        char *key;
        char *value;
        char *save;

        for(kv=strtok_r(metadata, "\n", &save);
            kv != NULL;
            kv=strtok_r(NULL, "\n", &save))
        {
            /* NOTE: we intentionally only split on the first = character.
             * There may be additional = characters in the value portion
             * (for example, when storing mpi-io hints).
             */
            strcpy(buffer, kv);
            key = buffer;
            value = index(buffer, '=');
            if(!value)
                continue;
            /* convert = to a null terminator to split key and value */
            value[0] = '\0';
            value++;
            if (strcmp(key, "prev_ver") == 0)
            {
                strncpy(job->version_string, value, sizeof(job->version_string));
            }
        }
        free(metadata);
    }

    return(ret);
}

/* darshan_putjob()
 * write job header in gzfile
 *
 * return 0 on success, -1 on failure.
 */
int darshan_log_putjob(darshan_fd file, struct darshan_job *job)
{
    struct darshan_job job_copy;
    char    pv_str[64];
    int     ret;
    int len;

    ret = darshan_log_seek(file, 0);
    if(ret < 0)
        return(ret);

    memset(&job_copy, 0, sizeof(job_copy));
    memcpy(&job_copy, job, sizeof(job_copy));
    /* check for newline in existing metadata, add if needed */
    len = strlen(job_copy.metadata);
    if(len > 0 && len < DARSHAN_JOB_METADATA_LEN)
    {
        if(job_copy.metadata[len-1] != '\n')
        {
            job_copy.metadata[len] = '\n';
            job_copy.metadata[len+1] = '\0';
        }
    }

    sprintf(pv_str, "prev_ver=%s\n", job->version_string);
    sprintf(job_copy.version_string, "%s", CP_VERSION);
    if(strlen(job_copy.metadata) + strlen(pv_str) < DARSHAN_JOB_METADATA_LEN)
        strncat(job_copy.metadata, pv_str, strlen(pv_str));
    else
        sprintf(job_copy.metadata, "%s", pv_str);
    job_copy.magic_nr = CP_MAGIC_NR;

    ret = darshan_log_write(file, &job_copy, sizeof(job_copy));
    if (ret != sizeof(job_copy))
    {
        fprintf(stderr, "Error: failed to write job header: %d\n", ret);
        return(-1);
    }

    return(0);
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

/* darshan_log_putfile()
 *
 * return 0 if file record written, -1 on error.
 */
int darshan_log_putfile(darshan_fd fd, struct darshan_job *job, struct darshan_file *file)
{
    int     ret;

    if(fd->pos < CP_JOB_RECORD_SIZE)
    {
        ret = darshan_log_seek(fd, CP_JOB_RECORD_SIZE);
        if(ret < 0)
            return(ret);
    }

    ret = darshan_log_write(fd, file, sizeof(*file));
    if (ret != sizeof(*file))
    {
        fprintf(stderr, "Error: writing file record failed: %d\n", ret);
        return(-1);
    }

    return(0);
}

/* darshan_log_getmounts()
 * 
 * retrieves mount table information from the log.  Note that devs, mnt_pts,
 * and fs_types are arrays that will be allocated by the function and must
 * be freed by the caller.  count will indicate the size of the arrays
 */
int darshan_log_getmounts(darshan_fd fd, int64_t** devs, char*** mnt_pts, char***
    fs_types, int* count)
{
    int ret;
    char* pos;
    int array_index = 0;
    char buf[fd->COMPAT_CP_EXE_LEN+1];

    ret = darshan_log_seek(fd, fd->job_struct_size);
    if(ret < 0)
        return(ret);

    ret = darshan_log_read(fd, buf, (fd->COMPAT_CP_EXE_LEN + 1));
    if (ret < (fd->COMPAT_CP_EXE_LEN + 1))
    {
        perror("darshan_log_read");
        return(-1);
    }

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
        (*mnt_pts)[array_index] = malloc(fd->COMPAT_CP_EXE_LEN);
        assert((*mnt_pts)[array_index]);
        (*fs_types)[array_index] = malloc(fd->COMPAT_CP_EXE_LEN);
        assert((*fs_types)[array_index]);
        
        ret = sscanf(++pos, "%" PRId64 "\t%s\t%s", &(*devs)[array_index],
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

/* darshan_log_putmounts
 *
 * encode mount information back into mtab format.
 *
 * returns 0 on success, -1 on failure.
 */
int darshan_log_putmounts(darshan_fd fd, int64_t* devs, char** mnt_pts, char** fs_types, int count)
{
    int     ret;
    char    line[1024];
    int     i;

    for(i=count-1; i>=0; i--)
    {
        sprintf(line, "\n%" PRId64 "\t%s\t%s",
                devs[i], fs_types[i], mnt_pts[i]);
        ret = darshan_log_write(fd, line, strlen(line));
        if (ret != strlen(line))
        {
            fprintf(stderr, "Error: failed to write mount entry: %d\n", ret);
            return(-1);
        }
    }

    /* seek ahead to end of exe region, will be zero filled */
    ret = darshan_log_seek(fd, CP_JOB_RECORD_SIZE);
    if (ret)
    {
        fprintf(stderr, "Error: forward seek failed: %d\n", CP_JOB_RECORD_SIZE);
    }

    return(0);
}

int darshan_log_getexe(darshan_fd fd, char *buf)
{
    int ret;
    char* newline;

    ret = darshan_log_seek(fd, fd->job_struct_size);
    if(ret < 0)
        return(ret);

    ret = darshan_log_read(fd, buf, (fd->COMPAT_CP_EXE_LEN + 1));
    if (ret < (fd->COMPAT_CP_EXE_LEN + 1))
    {
        perror("darshan_log_read");
        return(-1);
    }

    /* this call is only supposed to return the exe string, but starting in
     * log format 1.23 there could be a table of mount entry information
     * after the exe.  Look for newline character and truncate there.
     */
    newline = strchr(buf, '\n');
    if(newline)
        *newline = '\0';

    return (0);
}

/* darshan_log_putexe()
 *
 * Write the exe string to the log.
 *
 * return 0 on success, -1 on failure.
 */
int darshan_log_putexe(darshan_fd fd, char *buf)
{
    int     ret;
    int     len;

    ret = darshan_log_seek(fd, sizeof(struct darshan_job));
    if(ret < 0)
        return(ret);

    len = strlen(buf);

    ret = darshan_log_write(fd, buf, len);
    if (ret != len)
    {
        fprintf(stderr, "Error: failed to write exe info: %d\n", ret);
        ret = -1;
    }

    return(ret);
}

void darshan_log_close(darshan_fd file)
{
#ifdef HAVE_LIBBZ2
    if(file->bzf)
        BZ2_bzclose(file->bzf);
#endif

    if(file->gzf)
        gzclose(file->gzf);

    free(file->name);
    free(file);
}

/* darshan_log_print_version_warnings()
 *
 * Print summary of any problems with the detected log format
 */
void darshan_log_print_version_warnings(struct darshan_job *job)
{
    if(strcmp(job->version_string, "2.05") == 0)
    {
        /* current version */
        return;
    }

    if(strcmp(job->version_string, "2.04") == 0)
    {
        printf("# WARNING: version 2.04 log format has the following limitations:\n");
        printf("# - CP_F_SLOWEST_RANK_TIME and CP_F_FASTEST_RANK_TIME only report elapsed time at the POSIX level.\n");
        return;
    }

    if(strcmp(job->version_string, "2.03") == 0)
    {
        /* no meaningful change to interpretation of log file, 2.03 just
         * increased the header space available for annotations.
         */
        printf("# WARNING: version 2.03 log format has the following limitations:\n");
        printf("# - CP_F_SLOWEST_RANK_TIME and CP_F_FASTEST_RANK_TIME only report elapsed time at the POSIX level.\n");
        return;
    }

    if(strcmp(job->version_string, "2.02") == 0)
    {
        printf("# WARNING: version 2.01 log format has the following limitations:\n");
        printf("# - *_TIMESTAMP fields are not normalized relative to MPI_Init() time.\n");
        printf("# - CP_F_SLOWEST_RANK_TIME and CP_F_FASTEST_RANK_TIME only report elapsed time at the POSIX level.\n");
        return;
    }

    if(strcmp(job->version_string, "2.01") == 0)
    {
        printf("# WARNING: version 2.01 log format has the following limitations:\n");
        printf("# - *_TIMESTAMP fields are not normalized relative to MPI_Init() time.\n");
        printf("# - inaccurate statistics in some multi-threaded cases.\n");
        printf("# - CP_F_SLOWEST_RANK_TIME and CP_F_FASTEST_RANK_TIME only report elapsed time at the POSIX level.\n");
        return;
    }

    if(strcmp(job->version_string, "2.00") == 0)
    {
        printf("# WARNING: version 2.00 log format has the following limitations:\n");
        printf("# - *_TIMESTAMP fields are not normalized relative to MPI_Init() time.\n");
        printf("# - inaccurate statistics in some multi-threaded cases.\n");
        printf("# - CP_F_SLOWEST_RANK_TIME and CP_F_FASTEST_RANK_TIME only report elapsed time at the POSIX level.\n");
        return;
    }
 
    if(strcmp(job->version_string, "1.24") == 0)
    {
        printf("# WARNING: version 1.24 log format does not support the following parameters:\n");
        printf("#   CP_FASTEST_RANK\n");
        printf("#   CP_FASTEST_RANK_BYTES\n");
        printf("#   CP_SLOWEST_RANK\n");
        printf("#   CP_SLOWEST_RANK_BYTES\n");
        printf("#   CP_F_FASTEST_RANK_TIME\n");
        printf("#   CP_F_SLOWEST_RANK_TIME\n");
        printf("#   CP_F_VARIANCE_RANK_TIME\n");
        printf("#   CP_F_VARIANCE_RANK_BYTES\n");
        printf("# WARNING: version 1.24 log format has the following limitations:\n");
        printf("# - *_TIMESTAMP fields are not normalized relative to MPI_Init() time.\n");
        printf("# - does not store the job id in the file.\n");
        printf("# - inaccurate statistics in some multi-threaded cases.\n");
        printf("# - CP_F_SLOWEST_RANK_TIME and CP_F_FASTEST_RANK_TIME only report elapsed time at the POSIX level.\n");
        return;
    }
    
    if(strcmp(job->version_string, "1.23") == 0)
    {
        printf("# WARNING: version 1.23 log format does not support the following parameters:\n");
        printf("#   CP_FASTEST_RANK\n");
        printf("#   CP_FASTEST_RANK_BYTES\n");
        printf("#   CP_SLOWEST_RANK\n");
        printf("#   CP_SLOWEST_RANK_BYTES\n");
        printf("#   CP_F_FASTEST_RANK_TIME\n");
        printf("#   CP_F_SLOWEST_RANK_TIME\n");
        printf("#   CP_F_VARIANCE_RANK_TIME\n");
        printf("#   CP_F_VARIANCE_RANK_BYTES\n");
        printf("# WARNING: version 1.23 log format has the following limitations:\n");
        printf("# - *_TIMESTAMP fields are not normalized relative to MPI_Init() time.\n");
        printf("# - may have incorrect mount point mappings for files with rank > 0.\n");
        printf("# - does not store the job id in the file.\n");
        printf("# - inaccurate statistics in some multi-threaded cases.\n");
        printf("# - CP_F_SLOWEST_RANK_TIME and CP_F_FASTEST_RANK_TIME only report elapsed time at the POSIX level.\n");
        return;
    }

    if(strcmp(job->version_string, "1.22") == 0)
    {
        printf("# WARNING: version 1.22 log format does not support the following parameters:\n");
        printf("#   CP_DEVICE\n");
        printf("#   CP_SIZE_AT_OPEN\n");
        printf("#   CP_FASTEST_RANK\n");
        printf("#   CP_FASTEST_RANK_BYTES\n");
        printf("#   CP_SLOWEST_RANK\n");
        printf("#   CP_SLOWEST_RANK_BYTES\n");
        printf("#   CP_F_FASTEST_RANK_TIME\n");
        printf("#   CP_F_SLOWEST_RANK_TIME\n");
        printf("#   CP_F_VARIANCE_RANK_TIME\n");
        printf("#   CP_F_VARIANCE_RANK_BYTES\n");
        printf("# WARNING: version 1.22 log format has the following limitations:\n");
        printf("# - *_TIMESTAMP fields are not normalized relative to MPI_Init() time.\n");
        printf("# - does not record mounted file systems, mount points, or fs types.\n");
        printf("# - attributes syncs to cumulative metadata time, rather than cumulative write time.\n");
        printf("# - does not store the job id in the file.\n");
        printf("# - inaccurate statistics in some multi-threaded cases.\n");
        printf("# - CP_F_SLOWEST_RANK_TIME and CP_F_FASTEST_RANK_TIME only report elapsed time at the POSIX level.\n");
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
        printf("#   CP_FASTEST_RANK\n");
        printf("#   CP_FASTEST_RANK_BYTES\n");
        printf("#   CP_SLOWEST_RANK\n");
        printf("#   CP_SLOWEST_RANK_BYTES\n");
        printf("#   CP_F_FASTEST_RANK_TIME\n");
        printf("#   CP_F_SLOWEST_RANK_TIME\n");
        printf("#   CP_F_VARIANCE_RANK_TIME\n");
        printf("#   CP_F_VARIANCE_RANK_BYTES\n");
        printf("# WARNING: version 1.21 log format has the following limitations:\n");
        printf("# - *_TIMESTAMP fields are not normalized relative to MPI_Init() time.\n");
        printf("# - does not record mounted file systems, mount points, or fs types.\n");
        printf("# - attributes syncs to cumulative metadata time, rather than cumulative write time.\n");
        printf("# - does not store the job id in the file.\n");
        printf("# - inaccurate statistics in some multi-threaded cases.\n");
        printf("# - CP_F_SLOWEST_RANK_TIME and CP_F_FASTEST_RANK_TIME only report elapsed time at the POSIX level.\n");
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
 *   - CP_FASTEST_RANK
 *   - CP_FASTEST_RANK_BYTES
 *   - CP_SLOWEST_RANK
 *   - CP_SLOWEST_RANK_BYTES
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

static int getjob_internal_204(darshan_fd file, struct darshan_job *job)
{
    int ret;

    ret = darshan_log_seek(file, 0);
    if(ret < 0)
        return(ret);

    ret = darshan_log_read(file, job, sizeof(*job));
    if (ret < sizeof(*job))
    {
        fprintf(stderr, "Error: invalid log file (too short).\n");
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

static int getjob_internal_201(darshan_fd file, struct darshan_job *job)
{
    int ret;
    struct darshan_job_201
    {
        char version_string[8];
        int64_t magic_nr;
        int64_t uid;
        int64_t start_time;
        int64_t end_time;
        int64_t nprocs;
        int64_t jobid;
        char metadata[64];
    } job_201;
    memset(job, 0, sizeof(job_201));
    memset(job, 0, sizeof(*job));

    ret = darshan_log_seek(file, 0);
    if(ret < 0)
        return(ret);

    ret = darshan_log_read(file, &job_201, sizeof(job_201));
    if (ret < sizeof(job_201))
    {
        fprintf(stderr, "Error: invalid log file (too short).\n");
        return(-1);
    }

    memcpy(job->version_string, job_201.version_string, 8);
    job->magic_nr   = job_201.magic_nr;
    job->uid        = job_201.uid;
    job->start_time = job_201.start_time;
    job->end_time   = job_201.end_time;
    job->nprocs     = job_201.nprocs;
    job->jobid      = job_201.jobid;
    strncpy(job->metadata, job_201.metadata, 64);

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


static int getjob_internal_200(darshan_fd file, struct darshan_job *job)
{
    int ret;
    struct darshan_job_200
    {
        char version_string[8];
        int64_t magic_nr;
        int64_t uid;
        int64_t start_time;
        int64_t end_time;
        int64_t nprocs;
        int64_t jobid;
    } job_200;

    memset(job, 0, sizeof(job_200));
    memset(job, 0, sizeof(*job));

    ret = darshan_log_seek(file, 0);
    if(ret < 0)
        return(ret);

    ret = darshan_log_read(file, &job_200, sizeof(job_200));
    if (ret < sizeof(job_200))
    {
        fprintf(stderr, "Error: invalid log file (too short).\n");
        return(-1);
    }

    memcpy(job->version_string, job_200.version_string, 8);
    job->magic_nr   = job_200.magic_nr;
    job->uid        = job_200.uid;
    job->start_time = job_200.start_time;
    job->end_time   = job_200.end_time;
    job->nprocs     = job_200.nprocs;
    job->jobid      = job_200.jobid;

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

static int getfile_internal_204(darshan_fd fd, struct darshan_job *job, 
    struct darshan_file *file)
{
    int ret;
    const char* err_string;
    int i;

    if(fd->pos < CP_JOB_RECORD_SIZE)
    {
        ret = darshan_log_seek(fd, CP_JOB_RECORD_SIZE);
        if(ret < 0)
            return(ret);
    }

    /* reset file record, so that diff compares against a zero'd out record
     * if file is missing
     */
    memset(file, 0, sizeof(&file));

    ret = darshan_log_read(fd, file, sizeof(*file));
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

    if(ret == 0)
    {
        /* hit end of file */
        return(0);
    }

    /* all other errors */
    err_string = darshan_log_error(fd, &ret);
    fprintf(stderr, "Error: %s\n", err_string);
    return(-1);
}

static int getfile_internal_200(darshan_fd fd, struct darshan_job *job, 
    struct darshan_file *file)
{
    int ret;
    const char* err_string;
    int i;

    if(fd->pos < CP_JOB_RECORD_SIZE_200)
    {
        ret = darshan_log_seek(fd, CP_JOB_RECORD_SIZE_200);
        if(ret < 0)
            return(ret);
    }

    /* reset file record, so that diff compares against a zero'd out record
     * if file is missing
     */
    memset(file, 0, sizeof(&file));

    ret = darshan_log_read(fd, file, sizeof(*file));
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

    if(ret == 0)
    {
        /* hit end of file */
        return(0);
    }

    /* all other errors */
    err_string = darshan_log_error(fd, &ret);
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

    ret = darshan_log_seek(fd, 0);
    if(ret < 0)
        return(ret);

    ret = darshan_log_read(fd, buffer, JOB_SIZE_124);
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

    ret = getfile_internal_1x(fd, job, file, 140, 14);
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
    if(fd->pos < CP_JOB_RECORD_SIZE_1x)
    {
        ret = darshan_log_seek(fd, CP_JOB_RECORD_SIZE_1x);
        if(ret < 0)
            return(ret);
    }
    
    /* space for file struct, int64 array, and double array */
    buffer = (char*)malloc(FILE_SIZE_1x);
    if(!buffer)
    {
        return(-1);
    }

    ret = darshan_log_read(fd, buffer, FILE_SIZE_1x);

    if(ret > 0 && ret < FILE_SIZE_1x)
    {
        /* got a short read */
        fprintf(stderr, "Error: invalid file record (too small)\n");
        free(buffer);
        return(-1);
    }
    else if(ret == 0)
    {
        /* hit end of file */
        free(buffer);
        return(0);
    }
    else if(ret <= 0)
    {
        /* all other errors */
        err_string = darshan_log_error(fd, &ret);
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

/* return amount written on success, -1 on failure.
 */
static int darshan_log_write(darshan_fd fd, void* buf, int len)
{
    int ret;

    if(fd->gzf)
    {
        ret = gzwrite(fd->gzf, buf, len);
        if(ret > 0)
            fd->pos += ret;
        return(ret);
    }

#ifdef HAVE_LIBBZ2
    if(fd->bzf)
    {
        ret = BZ2_bzwrite(fd->bzf, buf, len);
        if(ret > 0)
            fd->pos += ret;
        return(ret);
    }
#endif

    return(-1);
}


/* return amount read on success, 0 on EOF, -1 on failure.
 */
static int darshan_log_read(darshan_fd fd, void* buf, int len)
{
    int ret;

    if(fd->gzf)
    {
        ret = gzread(fd->gzf, buf, len);
        if(ret > 0)
            fd->pos += ret;
        return(ret);
    }

#ifdef HAVE_LIBBZ2
    if(fd->bzf)
    {
        ret = BZ2_bzread(fd->bzf, buf, len);
        if(ret > 0)
            fd->pos += ret;
        return(ret);
    }
#endif

    return(-1);
}


static const char* darshan_log_error(darshan_fd fd, int* errnum)
{
    if(fd->gzf)
    {
        return(gzerror(fd->gzf, errnum));
    }

#ifdef HAVE_LIBBZ2
    if(fd->bzf)
    {
        return(BZ2_bzerror(fd->bzf, errnum));
    }
#endif

    *errnum = 0;
    return(NULL);
}

/* return 0 on successful seek to offset, -1 on failure.
 */
static int darshan_log_seek(darshan_fd fd, int64_t offset)
{
    z_off_t zoff = 0;
    z_off_t zoff_ret = 0;

    if(fd->pos == offset)
        return(0);

    if(fd->gzf)
    {
        zoff += offset;
        zoff_ret = gzseek(fd->gzf, zoff, SEEK_SET);
        if(zoff_ret == zoff)
        {
            fd->pos = offset;
            return(0);
        }
        return(-1);
    }

#ifdef HAVE_LIBBZ2
    if(fd->bzf)
    {
        int64_t counter;
        char dummy = '\0';
        int ret;

        /* There is no seek in bzip2.  Just close, reopen, and throw away 
         * data until the correct offset.  Very slow, but we don't expect to
         * do this often.
         */
        if(fd->mode[0] == 'r' && offset < fd->pos)
        {
            /* to seek backwards in read-only mode we just close and re-open
             * the file
             */
            BZ2_bzclose(fd->bzf);
            fd->bzf = BZ2_bzopen(fd->name, fd->mode);
            if(!fd->bzf)
                return(-1);

            fd->pos = 0;
        }
        else if(fd->mode[0] == 'w' && offset < fd->pos)
        {
            /* there isn't any convenient way to seek backwards in a
             * write-only bzip2 file, but we shouldn't need that
             * functionality in darshan anyway.
             */
            fprintf(stderr, "Error: seeking backwards in a bzip2 compressed darshan output file is not supported.\n");
            return(-1);
        }

        for(counter=0; counter<(offset-fd->pos); counter++)
        {
            if(fd->mode[0] == 'r')
            {
                ret = BZ2_bzread(fd->bzf, &dummy, 1);
            }
            else
            {
                ret = BZ2_bzwrite(fd->bzf, &dummy, 1);
            }
            if(ret != 1)
                return(-1);
        }
        fd->pos += counter;
        return(0);
    }
#endif

    return(-1);
}

