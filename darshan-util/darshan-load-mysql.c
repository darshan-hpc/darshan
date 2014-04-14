#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ftw.h>
#include <libgen.h>
#include <mysql.h>
#include <regex.h>
#include <sys/types.h>
#include <getopt.h>
#include <readline/readline.h>

#include "darshan-logutils.h"

#define MAXSQL (1024*1024)
#define STOPWALK (1)
#define CONTWALK (0)

#define OPT_HOST (1)
#define OPT_USER (2)
#define OPT_PASS (3)
#define OPT_DB   (4)
#define OPT_PATH (5)

const char *insert_job_fmt  = "insert into %s values('%d','%s','%s','%s',\
'%d','%ld','%ld','%d')";
const char *insert_mnt_fmt  = "insert into %s values('%d','%d','%" PRId64 "','%s','%s')";
const char *insert_file_fmt = "insert into %s values('%d','%ld','%" PRId64 "','%d',\
'%s',\
'%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "',\
'%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "',\
'%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "',\
'%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "',\
'%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "',\
'%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "',\
'%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "',\
'%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "',\
'%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "',\
'%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "',\
'%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "',\
'%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "',\
'%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "',\
'%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "','%" PRId64 "',\
'%.16lf','%.16lf','%.16lf','%.16lf','%.16lf',\
'%.16lf','%.16lf','%.16lf','%.16lf','%.16lf',\
'%.16lf','%.16lf','%.16lf','%.16lf')";

MYSQL *mysql = NULL;
int debug = 0;

int tree_walk (const char *fpath, const struct stat *sb, int typeflag)
{
    struct darshan_file file;
    struct darshan_job  job;
    darshan_fd          dfile = NULL;
    int                 ret;
    int                 rc;
    int                 nofiles;
    char                exe[4096];
    char               *base;
    char               *dash;
    char               *username;
    char               *jobid;
    char               *sqlstmt = NULL;
    int                 count;
    int                 i;
    int64_t            *devs;
    char              **mnts;
    char              **fstypes;
    regex_t             regex;
    regmatch_t          match[1];
    char               *filepath = NULL;
    int                 prev_rank;

    rc      = CONTWALK;
    count   = 0;

    /* Only Process Files */
    if (typeflag != FTW_F) return CONTWALK;

    sqlstmt = malloc(MAXSQL);
    if (!sqlstmt)
    {
        perror("malloc");
        rc = STOPWALK;
        goto exit;
    }

    filepath = strdup(fpath);
    if (!filepath)
    {
        perror("strdup");
        rc = STOPWALK;
        goto exit;
    }

    /* Process Log Files */
    dfile = darshan_log_open(fpath, "r");
    if (dfile == NULL)
    {
        fprintf(stderr, "darshan_log_open() failed to open %s\n.", fpath);
        rc = CONTWALK;
        goto exit;
    }

    ret = darshan_log_getjob(dfile, &job);
    if (ret < 0)
    {
        perror("darshan_log_getjob");
        fprintf(stderr, "%s\n", fpath);
        rc = CONTWALK;
        goto exit;
    }

    memset(exe, 0, sizeof(exe));

    ret = darshan_log_getexe(dfile, exe, &nofiles);
    if (ret < 0)
    {
        perror("darshan_log_getexe");
        fprintf(stderr, "%s\n", fpath);
        rc = CONTWALK;
        goto exit;
    }

    base     = basename(filepath);
    username = base;
    dash     = index(base, '_');
    *dash    = '\0';
    jobid    = dash+1;

    /*
     * Find jobid for log file name
     */
    ret = regcomp(&regex, "_id[[:digit:]]+_", REG_EXTENDED);
    if (ret != 0)
    {
        char buf[256];
        regerror(ret, &regex, buf, sizeof(buf));
        fprintf(stderr, "regcomp: %s\n", buf);
        rc = STOPWALK;
        goto exit;
    }

    ret = regexec(&regex, jobid, 1, match, 0);
    if (ret != 0)
    {
        char buf[256];
        regerror(ret, &regex, buf, sizeof(buf));
        fprintf(stderr, "regexec: %s\n", buf);
        rc = STOPWALK;
        goto exit;
    }

    regfree(&regex);

    dash   = jobid;
    jobid += (match[0].rm_so + 3);
    dash  += (match[0].rm_eo - 1);
    *dash  = 0;

    /*
     * Insert Job Record
     */
    snprintf(sqlstmt, MAXSQL, insert_job_fmt, "darshan_job_intrepid",
        atoi(jobid), username, job.version_string, exe, job.uid,
        job.start_time, job.end_time, job.nprocs);

    if (debug) printf("sql: %s\n", sqlstmt);
    ret = mysql_query(mysql, sqlstmt);
    if (ret)
    {
        fprintf(stderr, "log not processed: %s [mysql: %d (%s)] : \
jobid=%d start_time=%ld\n",
            fpath, mysql_errno(mysql), mysql_error(mysql),
            atoi(jobid), job.start_time);
        rc = CONTWALK;
        goto exit;
    }

    /*
     * Insert MountPoint Record (if present)
     */
    ret = darshan_log_getmounts(dfile,&devs,&mnts,&fstypes,&count,&nofiles);
    if (ret < 0)
    {
        perror("darshan_log_getmounts");
        fprintf(stderr, "%s\n", fpath);
        rc = STOPWALK;
        goto exit;
    }

    for (i=0; (i<count); i++)
    {
        snprintf(sqlstmt,MAXSQL,insert_mnt_fmt, "darshan_mountpoints_intrepid",
            atoi(jobid), job.start_time, devs[i], mnts[i], fstypes[i]);

        if (debug) printf("sql: %s\n", sqlstmt);
        ret = mysql_query(mysql, sqlstmt);
        if (ret)
        {
            fprintf(stderr, "mysql: %d (%s)\n", mysql_errno(mysql),
                mysql_error(mysql));
            rc = STOPWALK;
            goto exit;
        }
    }

    /*
     * Insert File Records (if present)
     */
    if (!nofiles)
    {
        while ((ret = darshan_log_getfile(dfile, &job, &file)) == 1)
        {
            /*
             * Work around issue where bogus file data was in older logs.
             * Bogus data was files taht were 'stat'd but not opened.
             */
            if (file.rank != -1 && file.rank < prev_rank)
            {
                continue;
            }
            if (file.rank != -1)
                prev_rank = file.rank;

            snprintf(sqlstmt, MAXSQL, insert_file_fmt, "darshan_file_intrepid",
                atoi(jobid), job.start_time, file.hash, file.rank, file.name_suffix,
                file.counters[CP_INDEP_OPENS],
                file.counters[CP_COLL_OPENS],
                file.counters[CP_INDEP_READS],
                file.counters[CP_INDEP_WRITES],
                file.counters[CP_COLL_READS],
                file.counters[CP_COLL_WRITES],
                file.counters[CP_SPLIT_READS],
                file.counters[CP_SPLIT_WRITES],
                file.counters[CP_NB_READS],
                file.counters[CP_NB_WRITES],
                file.counters[CP_SYNCS],
                file.counters[CP_POSIX_READS],
                file.counters[CP_POSIX_WRITES],
                file.counters[CP_POSIX_OPENS],
                file.counters[CP_POSIX_SEEKS],
                file.counters[CP_POSIX_STATS],
                file.counters[CP_POSIX_MMAPS],
                file.counters[CP_POSIX_FREADS],
                file.counters[CP_POSIX_FWRITES],
                file.counters[CP_POSIX_FOPENS],
                file.counters[CP_POSIX_FSEEKS],
                file.counters[CP_POSIX_FSYNCS],
                file.counters[CP_POSIX_FDSYNCS],
                file.counters[CP_INDEP_NC_OPENS],
                file.counters[CP_COLL_NC_OPENS],
                file.counters[CP_HDF5_OPENS],
                file.counters[CP_COMBINER_NAMED],
                file.counters[CP_COMBINER_DUP],
                file.counters[CP_COMBINER_CONTIGUOUS],
                file.counters[CP_COMBINER_VECTOR],
                file.counters[CP_COMBINER_HVECTOR_INTEGER],
                file.counters[CP_COMBINER_HVECTOR],
                file.counters[CP_COMBINER_INDEXED],
                file.counters[CP_COMBINER_HINDEXED_INTEGER],
                file.counters[CP_COMBINER_HINDEXED],
                file.counters[CP_COMBINER_INDEXED_BLOCK],
                file.counters[CP_COMBINER_STRUCT_INTEGER],
                file.counters[CP_COMBINER_STRUCT],
                file.counters[CP_COMBINER_SUBARRAY],
                file.counters[CP_COMBINER_DARRAY],
                file.counters[CP_COMBINER_F90_REAL],
                file.counters[CP_COMBINER_F90_COMPLEX],
                file.counters[CP_COMBINER_F90_INTEGER],
                file.counters[CP_COMBINER_RESIZED],
                file.counters[CP_HINTS],
                file.counters[CP_VIEWS],
                file.counters[CP_MODE],
                file.counters[CP_BYTES_READ],
                file.counters[CP_BYTES_WRITTEN],
                file.counters[CP_MAX_BYTE_READ],
                file.counters[CP_MAX_BYTE_WRITTEN],
                file.counters[CP_CONSEC_READS],
                file.counters[CP_CONSEC_WRITES],
                file.counters[CP_SEQ_READS],
                file.counters[CP_SEQ_WRITES],
                file.counters[CP_RW_SWITCHES],
                file.counters[CP_MEM_NOT_ALIGNED],
                file.counters[CP_MEM_ALIGNMENT],
                file.counters[CP_FILE_NOT_ALIGNED],
                file.counters[CP_FILE_ALIGNMENT],
                file.counters[CP_MAX_READ_TIME_SIZE],
                file.counters[CP_MAX_WRITE_TIME_SIZE],
                file.counters[CP_SIZE_READ_0_100],
                file.counters[CP_SIZE_READ_100_1K],
                file.counters[CP_SIZE_READ_1K_10K],
                file.counters[CP_SIZE_READ_10K_100K],
                file.counters[CP_SIZE_READ_100K_1M],
                file.counters[CP_SIZE_READ_1M_4M],
                file.counters[CP_SIZE_READ_4M_10M],
                file.counters[CP_SIZE_READ_10M_100M],
                file.counters[CP_SIZE_READ_100M_1G],
                file.counters[CP_SIZE_READ_1G_PLUS],
                file.counters[CP_SIZE_WRITE_0_100],
                file.counters[CP_SIZE_WRITE_100_1K],
                file.counters[CP_SIZE_WRITE_1K_10K],
                file.counters[CP_SIZE_WRITE_10K_100K],
                file.counters[CP_SIZE_WRITE_100K_1M],
                file.counters[CP_SIZE_WRITE_1M_4M],
                file.counters[CP_SIZE_WRITE_4M_10M],
                file.counters[CP_SIZE_WRITE_10M_100M],
                file.counters[CP_SIZE_WRITE_100M_1G],
                file.counters[CP_SIZE_WRITE_1G_PLUS],
                file.counters[CP_SIZE_READ_AGG_0_100],
                file.counters[CP_SIZE_READ_AGG_100_1K],
                file.counters[CP_SIZE_READ_AGG_1K_10K],
                file.counters[CP_SIZE_READ_AGG_10K_100K],
                file.counters[CP_SIZE_READ_AGG_100K_1M],
                file.counters[CP_SIZE_READ_AGG_1M_4M],
                file.counters[CP_SIZE_READ_AGG_4M_10M],
                file.counters[CP_SIZE_READ_AGG_10M_100M],
                file.counters[CP_SIZE_READ_AGG_100M_1G],
                file.counters[CP_SIZE_READ_AGG_1G_PLUS],
                file.counters[CP_SIZE_WRITE_AGG_0_100],
                file.counters[CP_SIZE_WRITE_AGG_100_1K],
                file.counters[CP_SIZE_WRITE_AGG_1K_10K],
                file.counters[CP_SIZE_WRITE_AGG_10K_100K],
                file.counters[CP_SIZE_WRITE_AGG_100K_1M],
                file.counters[CP_SIZE_WRITE_AGG_1M_4M],
                file.counters[CP_SIZE_WRITE_AGG_4M_10M],
                file.counters[CP_SIZE_WRITE_AGG_10M_100M],
                file.counters[CP_SIZE_WRITE_AGG_100M_1G],
                file.counters[CP_SIZE_WRITE_AGG_1G_PLUS],
                file.counters[CP_EXTENT_READ_0_100],
                file.counters[CP_EXTENT_READ_100_1K],
                file.counters[CP_EXTENT_READ_1K_10K], 
                file.counters[CP_EXTENT_READ_10K_100K],
                file.counters[CP_EXTENT_READ_100K_1M],
                file.counters[CP_EXTENT_READ_1M_4M],
                file.counters[CP_EXTENT_READ_4M_10M],
                file.counters[CP_EXTENT_READ_10M_100M],
                file.counters[CP_EXTENT_READ_100M_1G],
                file.counters[CP_EXTENT_READ_1G_PLUS],
                file.counters[CP_EXTENT_WRITE_0_100],
                file.counters[CP_EXTENT_WRITE_100_1K],
                file.counters[CP_EXTENT_WRITE_1K_10K],
                file.counters[CP_EXTENT_WRITE_10K_100K],
                file.counters[CP_EXTENT_WRITE_100K_1M],
                file.counters[CP_EXTENT_WRITE_1M_4M],
                file.counters[CP_EXTENT_WRITE_4M_10M],
                file.counters[CP_EXTENT_WRITE_10M_100M],
                file.counters[CP_EXTENT_WRITE_100M_1G],
                file.counters[CP_EXTENT_WRITE_1G_PLUS],
                file.counters[CP_STRIDE1_STRIDE],
                file.counters[CP_STRIDE2_STRIDE],
                file.counters[CP_STRIDE3_STRIDE],
                file.counters[CP_STRIDE4_STRIDE],
                file.counters[CP_STRIDE1_COUNT],
                file.counters[CP_STRIDE2_COUNT],
                file.counters[CP_STRIDE3_COUNT],
                file.counters[CP_STRIDE4_COUNT],
                file.counters[CP_ACCESS1_ACCESS],
                file.counters[CP_ACCESS2_ACCESS],
                file.counters[CP_ACCESS3_ACCESS],
                file.counters[CP_ACCESS4_ACCESS],
                file.counters[CP_ACCESS1_COUNT],
                file.counters[CP_ACCESS2_COUNT],
                file.counters[CP_ACCESS3_COUNT],
                file.counters[CP_ACCESS4_COUNT],
                file.counters[CP_DEVICE],
                file.counters[CP_SIZE_AT_OPEN],
                file.fcounters[CP_F_OPEN_TIMESTAMP],
                file.fcounters[CP_F_READ_START_TIMESTAMP],
                file.fcounters[CP_F_WRITE_START_TIMESTAMP],
                file.fcounters[CP_F_CLOSE_TIMESTAMP],
                file.fcounters[CP_F_READ_END_TIMESTAMP],
                file.fcounters[CP_F_WRITE_END_TIMESTAMP],
                file.fcounters[CP_F_POSIX_READ_TIME],
                file.fcounters[CP_F_POSIX_WRITE_TIME],
                file.fcounters[CP_F_POSIX_META_TIME],
                file.fcounters[CP_F_MPI_META_TIME],
                file.fcounters[CP_F_MPI_READ_TIME],
                file.fcounters[CP_F_MPI_WRITE_TIME],
                file.fcounters[CP_F_MAX_READ_TIME],
                file.fcounters[CP_F_MAX_WRITE_TIME]);

            if (debug) printf("sql: %s\n", sqlstmt);
            ret = mysql_query(mysql, sqlstmt);
            if (ret)
            {
                fprintf(stderr, "mysql: %d (%s)\n", mysql_errno(mysql),
                    mysql_error(mysql));
                rc = STOPWALK;
                goto exit;
            }
        }
    }

exit:
    if (dfile) darshan_log_close(dfile);

    if (count > 0)
    {
        for(i=0; i<count; i++)
        {
            if (mnts[i]) free(mnts[i]);
            if (fstypes[i]) free(fstypes[i]);
        }
        if (devs) free(devs);
        if (mnts) free(mnts);
        if (fstypes) free(fstypes);
    }

    if (sqlstmt)
    {
        free(sqlstmt);
    }

    if (filepath)
    {
        free(filepath);
    }
    
    return rc;
}

int main (int argc, char **argv)
{
    char base[256] = "";
    char host[256] = "";
    char user[256] = "";
    char pass[256] = "";
    char db[256]   = "";
    int  ret = 0;

    while(1)
    {
        static struct option options[] = {
            {"host", 1, NULL, OPT_HOST},
            {"user", 1, NULL, OPT_USER},
            {"pass", 1, NULL, OPT_PASS},
            {"db",   1, NULL, OPT_DB},
            {"path", 1, NULL, OPT_PATH},
            {NULL,   0, NULL, 0}
        };
        int o = getopt_long(argc, argv, "", options, NULL);

        if (o == -1) break;

        switch(o)
        {
        case OPT_HOST:
            strncpy(host, optarg, sizeof(host));
            break;
        case OPT_USER:
            strncpy(user, optarg, sizeof(user));
            break;
        case OPT_PASS:
            strncpy(pass, optarg, sizeof(pass));
            break;
        case OPT_DB:
            strncpy(db, optarg, sizeof(db));
            break;
        case OPT_PATH:
            strncpy(base, optarg, sizeof(base));
            break;
        }
    }

    if (strcmp(pass, "") == 0)
    {
        char *line = readline(NULL);
        if (line)
        {
            strncpy(pass, line, sizeof(pass));
        }
    }

    mysql = mysql_init(NULL);
    if (mysql == NULL)
    {
        fprintf(stderr, "mysql_init failed");
        exit(-1);
    }

    mysql = mysql_real_connect(mysql, host, user, pass, db, 0, NULL, 0);
    if (mysql == NULL)
    {
        fprintf(stderr, "mysql_real_connect");
        exit(-1);
    }

    /* Turn off auto commits, hopefuly for performance sake */
    (void) mysql_autocommit(mysql, 0);

    ret = ftw(base, tree_walk, 512);
    if(ret != 0)
    {
        fprintf(stderr, "Error: failed to walk path: %s\n", base);
        return(-1);
    }

    ret = mysql_commit(mysql);
    if (ret)
    {
        fprintf(stderr, "mysql: %d (%s)\n", mysql_errno(mysql),
            mysql_error(mysql));
    }

    mysql_close(mysql);

    return 0;
}
