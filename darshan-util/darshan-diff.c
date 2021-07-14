/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */
#ifdef HAVE_CONFIG_H
# include "darshan-util-config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <sys/types.h>
#include <assert.h>

#include "darshan-logutils.h"
#include "uthash-1.9.2/src/uthash.h"

struct darshan_mod_record_ref
{
    int rank;
    char *mod_dat;
    struct darshan_mod_record_ref *prev;
    struct darshan_mod_record_ref *next;
};

struct darshan_file_record_ref
{
    darshan_record_id rec_id;
    struct darshan_mod_record_ref *mod_recs[DARSHAN_MAX_MODS];
    UT_hash_handle hlink;
};

static int darshan_build_global_record_hash(
    darshan_fd fd, struct darshan_file_record_ref **rec_hash);

static void print_str_diff(char *prefix, char *arg1, char *arg2)
{
    printf("- %s %s\n", prefix, arg1);
    printf("+ %s %s\n", prefix, arg2);
    return;
}

static void print_int64_diff(char *prefix, int64_t arg1, int64_t arg2)
{
    printf("- %s %" PRId64 "\n", prefix, arg1);
    printf("+ %s %" PRId64 "\n", prefix, arg2);
    return;
}

int main(int argc, char *argv[])
{
    char *logfile1, *logfile2;
    darshan_fd file1, file2;
    struct darshan_job job1, job2;
    char exe1[4096], exe2[4096];
    struct darshan_name_record_ref *name_hash1 = NULL, *name_hash2 = NULL;
    struct darshan_name_record_ref *name_ref1, *name_ref2;
    struct darshan_file_record_ref *rec_hash1 = NULL, *rec_hash2 = NULL;
    struct darshan_file_record_ref *rec_ref1, *rec_ref2, *rec_tmp;
    struct darshan_mod_record_ref *mod_rec1, *mod_rec2;
    void *mod_buf1, *mod_buf2;
    struct darshan_base_record *base_rec1, *base_rec2;
    char *file_name1, *file_name2;
    int i;
    int ret;

    if(argc != 3)
    {
        fprintf(stderr, "Usage: darshan-diff <logfile1> <logfile2>\n");
        return(-1);
    }

    logfile1 = argv[1];
    logfile2 = argv[2];

    file1 = darshan_log_open(logfile1);
    if(!file1)
    {
        fprintf(stderr, "Error: unable to open darshan log file %s.\n", logfile1);
        return(-1);
    }

    file2 = darshan_log_open(logfile2);
    if(!file2)
    {
        darshan_log_close(file1);
        fprintf(stderr, "Error: unable to open darshan log file %s.\n", logfile2);
        return(-1);
    }

    /* get job data for each log file */
    ret = darshan_log_get_job(file1, &job1);
    if(ret < 0)
    {
        darshan_log_close(file1);
        darshan_log_close(file2);
        fprintf(stderr, "Error: unable to read job info for darshan log file %s.\n", logfile1);
        return(-1);
    }

    ret = darshan_log_get_job(file2, &job2);
    if(ret < 0)
    {
        darshan_log_close(file1);
        darshan_log_close(file2);
        fprintf(stderr, "Error: unable to read job info for darshan log file %s.\n", logfile2);
        return(-1);
    }

    /* get exe string for each log file */
    ret = darshan_log_get_exe(file1, exe1);
    if(ret < 0)
    {
        darshan_log_close(file1);
        darshan_log_close(file2);
        fprintf(stderr, "Error: unable to read exe for darshan log file %s.\n", logfile1);
        return(-1);
    }

    ret = darshan_log_get_exe(file2, exe2);
    if(ret < 0)
    {
        darshan_log_close(file1);
        darshan_log_close(file2);
        fprintf(stderr, "Error: unable to read exe for darshan log file %s.\n", logfile2);
        return(-1);
    }

    /* print diff of exe and job data */
    if (strcmp(exe1, exe2))
        print_str_diff("# exe: ", exe1, exe2);

    if (job1.uid != job2.uid)
        print_int64_diff("# uid:", job1.uid, job2.uid);
    if (job1.start_time != job2.start_time)
        print_int64_diff("# start_time:", job1.start_time, job2.start_time);
    if (job1.end_time != job2.end_time)
        print_int64_diff("# end_time:", job1.end_time, job2.end_time);
    if (job1.nprocs != job2.nprocs)
        print_int64_diff("# nprocs:", job1.nprocs, job2.nprocs);
    if ((job1.end_time-job1.start_time) != (job2.end_time - job2.start_time))
        print_int64_diff("# run time:",
                (int64_t)(job1.end_time - job1.start_time + 1),
                (int64_t)(job2.end_time - job2.start_time + 1));

    /* get hash of record ids to file names for each log */
    ret = darshan_log_get_namehash(file1, &name_hash1);
    if(ret < 0)
    {
        darshan_log_close(file1);
        darshan_log_close(file2);
        fprintf(stderr, "Error: unable to read record hash for darshan log file %s.\n", logfile1);
        return(-1);
    }

    ret = darshan_log_get_namehash(file2, &name_hash2);
    if(ret < 0)
    {
        darshan_log_close(file1);
        darshan_log_close(file2);
        fprintf(stderr, "Error: unable to read record hash for darshan log file %s.\n", logfile2);
        return(-1);
    }

    /* build hash tables of all records opened by all modules for each darshan log file */
    ret = darshan_build_global_record_hash(file1, &rec_hash1);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to build record hash for darshan log file %s.\n", logfile1);
        darshan_log_close(file1);
        darshan_log_close(file2);
        return(-1);
    }

    ret = darshan_build_global_record_hash(file2, &rec_hash2);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to build record hash for darshan log file %s.\n", logfile2);
        darshan_log_close(file1);
        darshan_log_close(file2);
        return(-1);
    }

    /* iterate records for the first log file and correlate/diff with records from
     * the second log file
     */
    HASH_ITER(hlink, rec_hash1, rec_ref1, rec_tmp)
    {
        printf("\n");

        /* search hash2 for this record */
        HASH_FIND(hlink, rec_hash2, &(rec_ref1->rec_id), sizeof(darshan_record_id), rec_ref2);

        for(i = 0; i < DARSHAN_MAX_MODS; i++)
        {
            /* skip the DXT modules -- we won't be diff'ing traces */
            if(i == DXT_POSIX_MOD || i == DXT_MPIIO_MOD)
                continue;

            /* TODO: skip modules that don't have the same format version, for now */
            if(rec_ref1->mod_recs[i] && rec_ref2 && rec_ref2->mod_recs[i] &&
                (file1->mod_ver[i] != file2->mod_ver[i]))
            {
                fprintf(stderr, "Warning: skipping %s module data due to incompatible"
                    "version numbers (file1=%d, file2=%d).\n",
                    darshan_module_names[i], file1->mod_ver[i], file2->mod_ver[i]);
                continue;
            }

            while(1)
            {
                mod_rec1 = rec_ref1->mod_recs[i];
                if(rec_ref2)
                    mod_rec2 = rec_ref2->mod_recs[i];
                else
                    mod_rec2 = NULL;

                if(mod_rec1 == NULL)
                    mod_buf1 = NULL;
                else
                    mod_buf1 = mod_rec1->mod_dat;
                if(mod_rec2 == NULL)
                    mod_buf2 = NULL;
                else
                    mod_buf2 = mod_rec2->mod_dat;

                base_rec1 = (struct darshan_base_record *)mod_buf1;
                base_rec2 = (struct darshan_base_record *)mod_buf2;
                if(!base_rec1 && !base_rec2)
                {
                    /* break out if there are no more records for this module */
                    break;
                }
                else if(base_rec1 && base_rec2)
                {
                    /* make sure if we have module records for this darshan record
                     * from both log files, that we are performing the diff rank-by-rank
                     * (i.e., we diff rank 1 records together, rank 2 records together,
                     * and so on)
                     */
                    if(base_rec1->rank < base_rec2->rank)
                        mod_buf2 = NULL;
                    else if(base_rec1->rank > base_rec2->rank)
                        mod_buf1 = NULL;
                }

                /* get corresponding file name for each record */
                if(mod_buf1)
                {
                    HASH_FIND(hlink, name_hash1, &(base_rec1->id),
                        sizeof(darshan_record_id), name_ref1);
                    assert(name_ref1);
                    file_name1 = name_ref1->name_record->name;
                }
                if(mod_buf2)
                {
                    HASH_FIND(hlink, name_hash2, &(base_rec2->id),
                        sizeof(darshan_record_id), name_ref2);
                    assert(name_ref2);
                    file_name2 = name_ref2->name_record->name;
                }

                mod_logutils[i]->log_print_diff(mod_buf1, file_name1, mod_buf2, file_name2);

                /* remove records which we have diffed already */
                if(mod_buf1)
                {
                    if(mod_rec1->next == mod_rec1)
                    {
                        rec_ref1->mod_recs[i] = NULL;
                    }
                    else
                    {
                        mod_rec1->prev->next = mod_rec1->next;
                        mod_rec1->next->prev = mod_rec1->prev;
                        rec_ref1->mod_recs[i] = mod_rec1->next;
                    }
                    free(mod_rec1->mod_dat);
                    free(mod_rec1);
                }
                if(mod_buf2)
                {
                    if(mod_rec2->next == mod_rec2)
                    {
                        rec_ref2->mod_recs[i] = NULL;
                    }
                    else
                    {
                        mod_rec2->prev->next = mod_rec2->next;
                        mod_rec2->next->prev = mod_rec2->prev;
                        rec_ref2->mod_recs[i] = mod_rec2->next;
                    }
                    free(mod_rec2->mod_dat);
                    free(mod_rec2);
                }
            }
        }

        HASH_DELETE(hlink, rec_hash1, rec_ref1);
        free(rec_ref1);
        if(rec_ref2)
        {
            HASH_DELETE(hlink, rec_hash2, rec_ref2);
            free(rec_ref2);
        }
    }

    /* iterate any remaning records from the 2nd log file and print the diff output --
     * NOTE: that these records do not have corresponding records in the first log file
     */
    HASH_ITER(hlink, rec_hash2, rec_ref2, rec_tmp)
    {
        for(i = 0; i < DARSHAN_MAX_MODS; i++)
        {
            while(rec_ref2->mod_recs[i])
            {
                mod_rec2 = rec_ref2->mod_recs[i];
                base_rec2 = (struct darshan_base_record *)mod_rec2->mod_dat;

                HASH_FIND(hlink, name_hash2, &(base_rec2->id),
                    sizeof(darshan_record_id), name_ref2);
                assert(name_ref2);
                file_name2 = name_ref2->name_record->name;

                mod_logutils[i]->log_print_diff(NULL, NULL, mod_rec2->mod_dat, file_name2);
                
                /* remove the record we just diffed */
                if(mod_rec2->next == mod_rec2)
                {
                    rec_ref2->mod_recs[i] = NULL;
                }
                else
                {
                    mod_rec2->prev->next = mod_rec2->next;
                    mod_rec2->next->prev = mod_rec2->prev;
                    rec_ref2->mod_recs[i] = mod_rec2->next;
                }
                free(mod_rec2->mod_dat);
                free(mod_rec2);
            }
        }

        HASH_DELETE(hlink, rec_hash2, rec_ref2);
        free(rec_ref2);
    }

    HASH_ITER(hlink, name_hash1, name_ref1, name_ref2)
    {
        HASH_DELETE(hlink, name_hash1, name_ref1);
        free(name_ref1->name_record);
        free(name_ref1);
    }
    HASH_ITER(hlink, name_hash2, name_ref2, name_ref1)
    {
        HASH_DELETE(hlink, name_hash2, name_ref2);
        free(name_ref2->name_record);
        free(name_ref2);
    }

    darshan_log_close(file1);
    darshan_log_close(file2);

    return(0);
}

static int darshan_build_global_record_hash(
    darshan_fd fd, struct darshan_file_record_ref **rec_hash)
{
    struct darshan_mod_record_ref *mod_rec;
    struct darshan_file_record_ref *file_rec;
    struct darshan_base_record *base_rec;
    int i;
    int ret;

    /* iterate over all modules in each log file, adding records to the
     * appropriate hash table
     */
    for(i = 0; i < DARSHAN_MAX_MODS; i++)
    {
        if(!mod_logutils[i]) continue;

        while(1)
        {
            mod_rec = malloc(sizeof(struct darshan_mod_record_ref));
            assert(mod_rec);
            memset(mod_rec, 0, sizeof(struct darshan_mod_record_ref));

            ret = mod_logutils[i]->log_get_record(fd, (void **)&(mod_rec->mod_dat));
            if(ret < 0)
            {
                fprintf(stderr, "Error: unable to read module %s data from log file.\n",
                    darshan_module_names[i]);
                free(mod_rec);
                return(-1);
            }
            else if(ret == 0)
            {
                free(mod_rec);
                break;
            }
            else
            {
                base_rec = (struct darshan_base_record *)mod_rec->mod_dat;
                mod_rec->rank = base_rec->rank;

                HASH_FIND(hlink, *rec_hash, &(base_rec->id), sizeof(darshan_record_id), file_rec);
                if(!file_rec)
                {
                    /* there is no entry in the global hash table of darshan records
                     * for this log file, so create one and add it.
                     */
                    file_rec = malloc(sizeof(struct darshan_file_record_ref));
                    assert(file_rec);

                    memset(file_rec, 0, sizeof(struct darshan_file_record_ref));
                    file_rec->rec_id = base_rec->id;
                    HASH_ADD(hlink, *rec_hash, rec_id, sizeof(darshan_record_id), file_rec);

                }

                /* add new record into the linked list of this module's records */
                if(file_rec->mod_recs[i])
                {
                    /* there is already an initialized linked list for this module */

                    /* we start at the end of the list and work backwards to insert this
                     * record (the list is sorted according to increasing ranks, and in
                     * general, darshan log records are sorted according to increasing
                     * ranks, as well)
                     */
                    struct darshan_mod_record_ref *tmp_mod_rec = file_rec->mod_recs[i]->prev;
                    while(1)
                    {
                        if(mod_rec->rank > tmp_mod_rec->rank)
                        {
                            /* insert new module record after this record */
                            mod_rec->prev = tmp_mod_rec;
                            mod_rec->next = tmp_mod_rec->next;
                            tmp_mod_rec->next->prev = mod_rec;
                            tmp_mod_rec->next = mod_rec;
                            break;
                        }
                        else if(mod_rec->rank < tmp_mod_rec->rank)
                        {
                            /* insert new module record before this record */
                            mod_rec->prev = tmp_mod_rec->prev;
                            mod_rec->next = tmp_mod_rec;
                            tmp_mod_rec->prev->next = mod_rec;
                            tmp_mod_rec->prev = mod_rec;
                            if(file_rec->mod_recs[i] == mod_rec->next)
                                file_rec->mod_recs[i] = mod_rec;
                            break;
                        }

                        tmp_mod_rec = tmp_mod_rec->prev;
                        assert(tmp_mod_rec != file_rec->mod_recs[i]);
                    }
                }
                else
                {
                    /* there are currently no records for this module, so just
                     * initialize a new linked list
                     */
                    mod_rec->prev = mod_rec->next = mod_rec;
                    file_rec->mod_recs[i] = mod_rec;
                }
            }
        }
    }

    return(0);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
