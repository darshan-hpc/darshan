typedef union
{
    int nompi_fd;
#ifdef HAVE_MPI
    MPI_File mpi_fh;
#endif
} darshan_log_fh;

#define DARSHAN_WARN(__err_str, ...) do { \
    darshan_core_fprintf(stderr, "darshan_library_warning: " \
        __err_str ".\n", ## __VA_ARGS__);
} while(0)

#ifdef HAVE_MPI

/* MPI variant of darshan logging helpers */
#define DARSHAN_CHECK_ERR(__ret, __err_str, ...) do { \
    if(using_mpi) \
        PMPI_Allreduce(MPI_IN_PLACE, &__ret, 1, MPI_INT, MPI_LOR, MPI_COMM_WORLD); \
    if(__ret != 0) { \
        if(my_rank == 0) { \
            DARSHAN_WARN(__err_str); \
            if(log_created) \
                unlink(logfile_name); \
        } \
        goto exit; \
    } \
} while(0)

#else

/* Non-MPI variant of darshan logging helpers */
#define DARSHAN_CHECK_ERR(__ret, __err_str, ...) do { \
    if(__ret != 0) { \
        DARSHAN_WARN(__err_str); \
        if(log_created) \
            unlink(logfile_name); \
        goto exit; \
    } \
} while(0)

#endif

static void darshan_get_logfile_name(char* logfile_name, int jobid, time_t start_time);
static int darshan_log_open(char *logfile_name, darshan_log_fh *log_fh);
static int darshan_log_write_job_record(darshan_log_fh log_fh,
    struct darshan_core_runtime *core, uint64_t *inout_off);
static int darshan_log_write_name_record_hash(darshan_log_fh log_fh,
    struct darshan_core_runtime *core, uint64_t *inout_off);
static int darshan_log_write_header(darshan_log_fh log_fh,
    struct darshan_core_runtime *core);
static int darshan_log_append(darshan_log_fh log_fh, struct darshan_core_runtime *core,
    void *buf, int count, uint64_t *inout_off);
void darshan_log_close(darshan_log_fh log_fh);
void darshan_log_finalize(char *logfile_name, double start_log_time);




void darshan_core_shutdown()
{
    struct darshan_core_runtime *final_core;
    double start_log_time;
    int internal_timing_flag = 0;
    double open1 = 0, open2 = 0;
    double job1 = 0, job2 = 0;
    double rec1 = 0, rec2 = 0;
    double mod1[DARSHAN_MAX_MODS] = {0};
    double mod2[DARSHAN_MAX_MODS] = {0};
    double header1 = 0, header2 = 0;
    double tm_end;
    int active_mods[DARSHAN_MAX_MODS] = {0};
    uint64_t gz_fp = 0;
    char *logfile_name = NULL;
    darshan_log_fh log_fh;
    int log_created = 0;
    int i;
    int ret;

    /* FIXME darshan_mpi_wtime references */

    /* disable darhan-core while we shutdown */
    DARSHAN_CORE_LOCK();
    if(!darshan_core)
    {
        DARSHAN_CORE_UNLOCK();
        return;
    }
    final_core = darshan_core;
    darshan_core = NULL;
    DARSHAN_CORE_UNLOCK();

    /* grab some initial timing information */
#ifdef HAVE_MPI
    /* if using mpi, sync across procs first */
    if(using_mpi)
        PMPI_Barrier(MPI_COMM_WORLD);
#endif
    start_log_time = darshan_mpi_wtime();
    final_core->log_job_p->end_time = time(NULL);

    if(getenv("DARSHAN_INTERNAL_TIMING"))
        internal_timing_flag = 1;

#ifdef __DARSHAN_ENABLE_MMAP_LOGS
    /* remove the temporary mmap log files */
    /* NOTE: this unlink is not immediate as it must wait for the mapping
     * to no longer be referenced, which in our case happens when the
     * executable exits. If the application terminates mid-shutdown, then
     * there will be no mmap files and no final log file.
     */
    unlink(final_core->mmap_log_name);
#endif

    final_core->comp_buf = malloc(darshan_mod_mem_quota);
    logfile_name = malloc(PATH_MAX);
    if(!final_core->comp_buf || !logfile_name)
        goto cleanup;

    /* set which modules were used locally */
    for(i = 0; i < DARSHAN_MAX_MODS; i++)
    {
        if(final_core->mod_array[i])
            active_mods[i] = 1;
    }

#ifdef HAVE_MPI
    darshan_record_id *shared_recs = NULL;
    darshan_record_id *mod_shared_recs = NULL;
    int shared_rec_cnt = 0;

    if(using_mpi)
    {
        /* allreduce locally active mods to determine globally active mods */
        PMPI_Allreduce(MPI_IN_PLACE, active_mods, DARSHAN_MAX_MODS, MPI_INT,
            MPI_SUM, MPI_COMM_WORLD);

        /* reduce to report first start and last end time across all ranks at rank 0 */
        PMPI_Reduce(MPI_IN_PLACE, &final_core->log_job_p->start_time,
            1, MPI_INT64_T, MPI_MIN, 0, MPI_COMM_WORLD);
        PMPI_Reduce(MPI_IN_PLACE, &final_core->log_job_p->end_time,
            1, MPI_INT64_T, MPI_MAX, 0, MPI_COMM_WORLD);

        /* get a list of records which are shared across all processes */
        darshan_get_shared_records(final_core, &shared_recs, &shared_rec_cnt);

        mod_shared_recs = malloc(shared_rec_cnt * sizeof(darshan_record_id));
        assert(mod_shared_recs);
    }
#endif

    /* get the log file name */
    darshan_get_logfile_name(logfile_name, final_core->log_job_p->jobid,
        final_core->log_job_p->start_time);
    if(strlen(logfile_name) == 0)
    {
        /* failed to generate log file name */
        goto exit;
    }

    if(internal_timing_flag)
        open1 = darshan_mpi_wtime();
    /* open the darshan log file */
    ret = darshan_log_open(logfile_name, &log_fh);
    /* error out if unable to open log file */
    DARSHAN_CHECK_ERR(ret, "unable to create log file %s", logfile_name);
    log_created = 1;
    if(internal_timing_flag)
        open2 = darshan_mpi_wtime();

    if(internal_timing_flag)
        job1 = darshan_mpi_wtime();
    /* write the the compressed darshan job information */
    ret = darshan_log_write_job_record(log_fh, final_core, &gz_fp)
    /* error out if unable to write job information */
    DARSHAN_CHECK_ERR(ret, "unable to write job record to file %s", logfile_name);
    if(internal_timing_flag)
        job2 = darshan_mpi_wtime();

    if(internal_timing_flag)
        rec1 = darshan_mpi_wtime();
    /* write the record name->id hash to the log file */
    final_core->log_hdr_p->name_map.off = gz_fp;
    ret = darshan_log_write_name_record_hash(log_fh, final_core, &gz_fp);
    final_core->log_hdr_p->name_map.len = gz_fp - final_core->log_hdr_p->name_map.off;
    /* error out if unable to write name records */
    DARSHAN_CHECK_ERR(ret, "unable to write name records to log file %s", logfile_name);
    if(internal_timing_flag)
        rec2 = darshan_mpi_wtime();

    /* loop over globally used darshan modules and:
     *      - get final output buffer
     *      - compress (zlib) provided output buffer
     *      - append compressed buffer to log file
     *      - add module map info (file offset/length) to log header
     *      - shutdown the module
     */
    for(i = 0; i < DARSHAN_MAX_MODS; i++)
    {
        struct darshan_core_module* this_mod = final_core->mod_array[i];
        void* mod_buf = NULL;
        int mod_buf_sz = 0;

        if(!active_mods[i])
        {
            final_core->log_hdr_p->mod_map[i].off = 0;
            final_core->log_hdr_p->mod_map[i].len = 0;
            continue;
        }

        if(internal_timing_flag)
            mod1[i] = darshan_mpi_wtime();

#ifdef HAVE_MPI
        struct darshan_core_name_record_ref *ref = NULL;
        int mod_shared_rec_cnt = 0;
        int j;

        if(using_mpi)
        {
            /* set the shared record list for this module */
            for(j = 0; j < shared_rec_cnt; j++)
            {
                HASH_FIND(hlink, final_core->name_hash, &shared_recs[j],
                    sizeof(darshan_record_id), ref);
                assert(ref);
                if(DARSHAN_MOD_FLAG_ISSET(ref->global_mod_flags, i))
                {
                    mod_shared_recs[mod_shared_rec_cnt++] = shared_recs[j];
                }
            }

            /* FIXME reduction op here! */
            this_mod->mod_redux_func(MPI_COMM_WORLD, mod_shared_recs,
                mod_shared_rec_cnt); 
        }
#endif

        /* if module is registered locally, get the corresponding output buffer
         * 
         * NOTE: this function can be used to run collective operations across
         * modules, if there are records shared globally.
         */
        if(this_mod)
        {
            mod_buf = final_core->mod_array[i]->rec_buf_start;
            mod_buf_sz = final_core->mod_array[i]->rec_buf_p - mod_buf;
            this_mod->mod_shutdown_func(&mod_buf, &mod_buf_sz);
        }

        /* append this module's data to the darshan log */
        final_core->log_hdr_p->mod_map[i].off = gz_fp;
        ret = darshan_log_append(log_fh, final_core, mod_buf, mod_buf_sz, &gz_fp);
        final_core->log_hdr_p->mod_map[i].len =
            gz_fp - final_core->log_hdr_p->mod_map[i].off;

        /* XXX: DXT manages its own module memory buffers, so we need to
         * explicitly free them
         */
        if(i == DXT_POSIX_MOD || i == DXT_MPIIO_MOD)
            free(mod_buf);

        /* error out if unable to write module data */
        DARSHAN_CHECK_ERR(ret, "unable to write %s module data to log file %s",
            darshan_module_names[i], logfile_name);

        if(internal_timing_flag)
            mod2[i] = darshan_mpi_wtime();
    }

    if(internal_timing_flag)
        header1 = darshan_mpi_wtime();
    ret = darshan_log_write_header(log_fh, final_core);
    DARSHAN_CHECK_ERR(ret, "unable to write header to file %s", logfile_name);
    if(internal_timing_flag)
        header2 = darshan_mpi_wtime();

    /* done writing data, close the log file */
    darshan_log_close(log_fh);

    /* finalize log file name and permissions */
    darshan_log_finalize();

    if(internal_timing_flag)
    {
        double open_tm;
        double header_tm;
        double job_tm;
        double rec_tm;
        double mod_tm[DARSHAN_MAX_MODS];
        double all_tm;

        tm_end = darshan_mpi_wtime();

        open_tm = open2 - open1;
        header_tm = header2 - header1;
        job_tm = job2 - job1;
        rec_tm = rec2 - rec1;
        all_tm = tm_end - start_log_time;
        for(i = 0; i < DARSHAN_MAX_MODS; i++)
        {
            mod_tm[i] = mod2[i] - mod1[i];
        }

#ifdef HAVE_MPI
        if(using_mpi)
        {
            PMPI_Reduce(MPI_IN_PLACE, &open_tm, 1,
                MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
            PMPI_Reduce(MPI_IN_PLACE, &header_tm, 1,
                MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
            PMPI_Reduce(MPI_IN_PLACE, &job_tm, 1,
                MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
            PMPI_Reduce(MPI_IN_PLACE, &rec_tm, 1,
                MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
            PMPI_Reduce(MPI_IN_PLACE, &all_tm, 1,
                MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
            PMPI_Reduce(MPI_IN_PLACE, mod_tm, DARSHAN_MAX_MODS,
                MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

            /* let rank 0 report the timing info */
            if(my_rank > 0)
                goto exit;
        }
#endif

        darshan_core_fprintf(stderr, "#darshan:<op>\t<nprocs>\t<time>\n");
        darshan_core_fprintf(stderr, "darshan:log_open\t%d\t%f\n", nprocs, open_tm);
        darshan_core_fprintf(stderr, "darshan:job_write\t%d\t%f\n", nprocs, job_tm);
        darshan_core_fprintf(stderr, "darshan:hash_write\t%d\t%f\n", nprocs, rec_tm);
        darshan_core_fprintf(stderr, "darshan:header_write\t%d\t%f\n", nprocs, header_tm);
        for(i = 0; i < DARSHAN_MAX_MODS; i++)
        {
            if(active_mods[i])
                darshan_core_fprintf(stderr, "darshan:%s_shutdown\t%d\t%f\n",
                    darshan_module_names[i], nprocs, mod_tm[i]);
        }
        darshan_core_fprintf(stderr, "darshan:core_shutdown\t%d\t%f\n", nprocs, all_tm);
    }

exit:
#ifdef HAVE_MPI
    free(shared_recs);
    free(mod_shared_recs);
#endif
    free(logfile_name);
    darshan_core_cleanup(final_core);

    return;
}

/* construct the darshan log file name */
static void darshan_get_logfile_name(char* logfile_name, int jobid, time_t start_time)
{
    char* user_logfile_name;
    char* logpath;
    char* logpath_override = NULL;
#ifdef __DARSHAN_LOG_ENV
    char env_check[256];
    char* env_tok;
#endif
    uint64_t hlevel;
    char hname[HOST_NAME_MAX];
    uint64_t logmod;
    char cuser[L_cuserid] = {0};
    struct tm *start_tm;
    int ret;

#ifdef HAVE_MPI
    if (using_mpi && my_rank > 0)
        goto bcast;
#endif

    /* first, check if user specifies a complete logpath to use */
    user_logfile_name = getenv("DARSHAN_LOGFILE");
    if(user_logfile_name)
    {
        if(strlen(user_logfile_name) >= (PATH_MAX-1))
        {
            DARSHAN_WARN("user log file name too long");
            logfile_name[0] = '\0';
        }
        else
        {
            strcpy(logfile_name, user_logfile_name);
        }
    }
    else
    {
        /* otherwise, generate the log path automatically */

        /* Use DARSHAN_LOG_PATH_OVERRIDE for the value or __DARSHAN_LOG_PATH */
        logpath = getenv(DARSHAN_LOG_PATH_OVERRIDE);
        if(!logpath)
        {
#ifdef __DARSHAN_LOG_PATH
            logpath = __DARSHAN_LOG_PATH;
#endif
        }

        darshan_get_user_name(cuser);

        /* generate a random number to help differentiate the log */
        hlevel = darshan_mpi_wtime() * 1000000;
        (void)gethostname(hname, sizeof(hname));
        logmod = darshan_hash((void*)hname,strlen(hname),hlevel);

        /* use human readable start time format in log filename */
        start_tm = localtime(&start_time);

        /* see if darshan was configured using the --with-logpath-by-env
         * argument, which allows the user to specify an absolute path to
         * place logs via an env variable.
         */
#ifdef __DARSHAN_LOG_ENV
        /* just silently skip if the environment variable list is too big */
        if(strlen(__DARSHAN_LOG_ENV) < 256)
        {
            /* copy env variable list to a temporary buffer */
            strcpy(env_check, __DARSHAN_LOG_ENV);
            /* tokenize the comma-separated list */
            env_tok = strtok(env_check, ",");
            if(env_tok)
            {
                do
                {
                    /* check each env variable in order */
                    logpath_override = getenv(env_tok);
                    if(logpath_override)
                    {
                        /* stop as soon as we find a match */
                        break;
                    }
                }while((env_tok = strtok(NULL, ",")));
            }
        }
#endif

        if(logpath_override)
        {
            ret = snprintf(logfile_name, PATH_MAX,
                "%s/%s_%s_id%d_%d-%d-%d-%" PRIu64 ".darshan_partial",
                logpath_override,
                cuser, __progname, jobid,
                (start_tm->tm_mon+1),
                start_tm->tm_mday,
                (start_tm->tm_hour*60*60 + start_tm->tm_min*60 + start_tm->tm_sec),
                logmod);
            if(ret == (PATH_MAX-1))
            {
                /* file name was too big; squish it down */
                snprintf(logfile_name, PATH_MAX,
                    "%s/id%d.darshan_partial",
                    logpath_override, jobid);
            }
        }
        else if(logpath)
        {
            ret = snprintf(logfile_name, PATH_MAX,
                "%s/%d/%d/%d/%s_%s_id%d_%d-%d-%d-%" PRIu64 ".darshan_partial",
                logpath, (start_tm->tm_year+1900),
                (start_tm->tm_mon+1), start_tm->tm_mday,
                cuser, __progname, jobid,
                (start_tm->tm_mon+1),
                start_tm->tm_mday,
                (start_tm->tm_hour*60*60 + start_tm->tm_min*60 + start_tm->tm_sec),
                logmod);
            if(ret == (PATH_MAX-1))
            {
                /* file name was too big; squish it down */
                snprintf(logfile_name, PATH_MAX,
                    "%s/id%d.darshan_partial",
                    logpath, jobid);
            }
        }
        else
        {
            logfile_name[0] = '\0';
        }
    }

#ifdef HAVE_MPI
bcast:
    if(using_mpi)
    {
        PMPI_Bcast(logfile_name, PATH_MAX, MPI_CHAR, 0, MPI_COMM_WORLD);
        if(my_rank > 0)
            return;
    }
#endif

    if(strlen(logfile_name) == 0)
        DARSHAN_WARN("unable to determine log file path");

    return;
}

static int darshan_log_open(char *logfile_name, darshan_log_fh *log_fh)
{
    int ret;
#ifdef HAVE_MPI
    char *hints;
    char *tok_str;
    char *orig_tok_str;
    char *key;
    char *value;
    char *saveptr = NULL;
    MPI_Info info;

    if(using_mpi)
    {
        /* check environment variable to see if the default MPI file hints have
         * been overridden
         */
        MPI_Info_create(&info);

        hints = getenv(DARSHAN_LOG_HINTS_OVERRIDE);
        if(!hints)
        {
            hints = __DARSHAN_LOG_HINTS;
        }

        if(hints && strlen(hints) > 0)
        {
            tok_str = strdup(hints);
            if(tok_str)
            {
                orig_tok_str = tok_str;
                do
                {
                    /* split string on semicolon */
                    key = strtok_r(tok_str, ";", &saveptr);
                    if(key)
                    {
                        tok_str = NULL;
                        /* look for = sign splitting key/value pairs */
                        value = index(key, '=');
                        if(value)
                        {
                            /* break key and value into separate null terminated strings */
                            value[0] = '\0';
                            value++;
                            if(strlen(key) > 0)
                                MPI_Info_set(info, key, value);
                        }
                    }
                }while(key != NULL);
                free(orig_tok_str);
            }
        }

        /* open the darshan log file for writing using MPI */
        ret = MPI_File_open(MPI_COMM_WORLD, logfile_name,
            MPI_MODE_CREATE | MPI_MODE_WRONLY | MPI_MODE_EXCL, info, &log_fh->mpi_fh);
        MPI_Info_free(&info);
        if(ret != MPI_SUCCESS)
            return(-1);
        return(0);
    }
#endif

    /* open the darshan log file for writing */
    log_fh->nonmpi_fd = open(logfile_name, O_CREAT | O_WRONLY | O_EXCL, S_IRUSR);
    if(log_fh->nonmpi_fd < 0)
        return(-1);
    return(0);
}

static int darshan_log_write_job_record(darshan_log_fh log_fh,
    struct darshan_core_runtime *core, uint64_t *inout_off)
{
    void *pointers[2] = {core->log_job_p, core->log_exemnt_p};
    int lengths[2] = {sizeof(struct darshan_job), strlen(core->log_exemnt_p)};
    int comp_buf_sz = 0;
    int ret;

#ifdef HAVE_MPI
    /* only rank 0 writes the job record */
    if (using_mpi && my_rank > 0)
        return(0);
#endif

    /* compress the job info and the trailing mount/exe data */
    ret = darshan_deflate_buffer(pointers, lengths, 2,
        core->comp_buf, &comp_buf_sz);
    if(ret)
    {
        DARSHAN_WARN("error compressing job record");
    }
    else
    {
        /* write the job information, preallocing space for the log header */
        *inout_off += sizeof(struct darshan_header);

#ifdef HAVE_MPI
        MPI_Status status;
        if(using_mpi)
        {
            ret = PMPI_File_write_at(log_fh.mpi_fh, *inout_off, core->comp_buf,
                comp_buf_sz, MPI_BYTE, &status);
            if(ret != MPI_SUCCESS)
            {
                DARSHAN_WARN("error writing job record");
                ret = -1;
            }
            else
            {
                *inout_off += comp_buf_sz;
                ret = 0;
            }

            return(ret);
        }
#endif

        ret = pwrite(log_fh.nompi_fd, core->comp_buf, comp_buf_sz, *inout_off);
        if(ret != comp_buf_sz)
        {
            DARSHAN_WARN("error writing job record");
            ret = -1;
        }
        else
        {
            *inout_off += comp_buf_sz;
            ret = 0;
        }
    }

    return(ret);
}

static int darshan_log_write_name_record_hash(darshan_log_fh log_fh,
    struct darshan_core_runtime *core, uint64_t *inout_off)
{
    struct darshan_core_name_record_ref *ref;
    struct darshan_name_record *name_rec;
    char *my_buf, *shared_buf;
    char *tmp_p;
    int rec_len;
    int shared_buf_len;
    int name_rec_buf_len;
    int ret;

    name_rec_buf_len = core->name_mem_used;
#ifdef HAVE_MPI
    if(using_mpi && (my_rank > 0))
    {
        /* remove globally shared name records from non-zero ranks */

        name_rec = core->log_name_p;
        my_buf = core->log_name_p;
        shared_buf = core->comp_buf;
        shared_buf_len = 0;
        while(name_rec_buf_len > 0)
        {
            HASH_FIND(hlink, core->name_hash, &(name_rec->id),
                sizeof(darshan_record_id), ref);
            assert(ref);
            rec_len = sizeof(darshan_record_id) + strlen(name_rec->name) + 1;

            if(ref->global_mod_flags)
            {   
                /* this record is shared globally, move to the temporary
                 * shared record buffer and update hash references
                 */
                HASH_DELETE(hlink, core->name_hash, ref);
                memcpy(shared_buf, name_rec, rec_len);
                ref->name_record = (struct darshan_name_record *)shared_buf;
                HASH_ADD(hlink, core->name_hash, name_record->id,
                    sizeof(darshan_record_id), ref);

                shared_buf += rec_len;
                shared_buf_len += rec_len;
            }
            else
            {   
                /* this record is not shared, but we still may need to
                 * move it forward in our buffer and update hash references
                 */
                if(my_buf != (char *)name_rec)
                {
                    HASH_DELETE(hlink, core->name_hash, ref);
                    memcpy(my_buf, name_rec, rec_len);
                    ref->name_record =(struct darshan_name_record *)my_buf;
                    HASH_ADD(hlink, core->name_hash, name_record->id,
                        sizeof(darshan_record_id), ref);
                }
                my_buf += rec_len;
            }

            tmp_p = (char *)name_rec + rec_len;
            name_rec = (struct darshan_name_record *)tmp_p;
            name_rec_buf_len -= rec_len;
        }
        name_rec_buf_len = core->name_mem_used - shared_buf_len;

        /* append the shared records back to the end of the name record
         * buffer and update hash table references so we can still
         * reference these records as modules shutdown
         */
        name_rec = (struct darshan_name_record *)core->comp_buf;
        while(shared_buf_len > 0)
        {
            HASH_FIND(hlink, core->name_hash, &(name_rec->id),
                sizeof(darshan_record_id), ref);
            assert(ref);
            rec_len = sizeof(darshan_record_id) + strlen(name_rec->name) + 1;

            HASH_DELETE(hlink, core->name_hash, ref);
            memcpy(my_buf, name_rec, rec_len);
            ref->name_record = (struct darshan_name_record *)my_buf;
            HASH_ADD(hlink, core->name_hash, name_record->id,
                sizeof(darshan_record_id), ref);

            tmp_p = (char *)name_rec + rec_len;
            name_rec = (struct darshan_name_record *)tmp_p;
            my_buf += rec_len;
            shared_buf_len -= rec_len;
        }
    }
#endif

    /* collectively write out the record hash to the darshan log */
    ret = darshan_log_append(log_fh, core, core->log_name_p,
        name_rec_buf_len, inout_off);
    return(ret);
}

static int darshan_log_write_header(darshan_log_fh log_fh,
    struct darshan_core_runtime *core)
{
    int ret;

    final_core->log_hdr_p->comp_type = DARSHAN_ZLIB_COMP;

#ifdef HAVE_MPI
    MPI_Status status;
    if (using_mpi)
    {
        /* write out log header, after running 2 reductions on header variables:
         *  1) reduce 'partial_flag' variable to determine which modules ran out
         *     of memory for storing data
         *  2) reduce 'mod_ver' array to determine which log format version each
         *     module used for this output log
         */
        PMPI_Reduce(
            MPI_IN_PLACE, &(core->log_hdr_p->partial_flag),
            1, MPI_UINT32_T, MPI_BOR, 0, MPI_COMM_WORLD);
        PMPI_Reduce(
            MPI_IN_PLACE, &(core->log_hdr_p->mod_ver),
            DARSHAN_MAX_MODS, MPI_UINT32_T, MPI_MAX, 0, MPI_COMM_WORLD);

        /* only rank 0 writes the header */
        if(my_rank > 0)
            return(0);

        /* write the header using MPI */
        ret = PMPI_File_write_at(log_fh.mpi_fh, 0, core->log_hdr_p,
            sizeof(struct darshan_header), MPI_BYTE, &status);
        if(ret != MPI_SUCCESS)
        {   
            DARSHAN_WARN("error writing darshan log header");
            ret = -1;
        }
        else
        {   
            ret = 0;
        }
            
        return(ret);
    }
#endif

    /* write log header */
    ret = pwrite(log_fh.nompi_fd, core->log_hdr_p, sizeof(struct darshan_header), 0);
    if(ret != sizeof(struct darshan_header))
    {
        DARSHAN_WARN("error writing darshan log header");
        ret = -1;
    }
    else
    {
        ret = 0;
    }

    return(ret);
}

/* NOTE: inout_off contains the starting offset of this append at the beginning
 *       of the call, and contains the ending offset at the end of the call.
 *       This variable is only valid on the root rank (rank 0).
 */
static int darshan_log_append(darshan_log_fh log_fh, struct darshan_core_runtime *core,
    void *buf, int count, uint64_t *inout_off)
{
    int comp_buf_sz = 0;
    int ret;

    /* compress the input buffer */
    ret = darshan_deflate_buffer((void **)&buf, &count, 1,
        core->comp_buf, &comp_buf_sz);
    if(ret < 0)
        comp_buf_sz = 0;

#ifdef HAVE_MPI
    MPI_Offset send_off, my_off;
    MPI_Status status;

    if(using_mpi)
    {
        /* figure out where everyone is writing using scan */
        send_off = comp_buf_sz;
        if(my_rank == 0)
        {
            send_off += *inout_off; /* rank 0 knows the beginning offset */
        }

        PMPI_Scan(&send_off, &my_off, 1, MPI_OFFSET, MPI_SUM, MPI_COMM_WORLD);
        /* scan is inclusive; subtract local size back out */
        my_off -= comp_buf_sz;

        if(ret == 0)
        {
            /* no compression errors, proceed with the collective write */
            ret = PMPI_File_write_at_all(log_fh.mpi_fh, my_off,
                core->comp_buf, comp_buf_sz, MPI_BYTE, &status);
            if(ret != MPI_SUCCESS)
                ret = -1;
        }
        else
        {
            /* error during compression. preserve and return error to caller,
             * but participate in collective write to avoid deadlock.
             */
            (void)PMPI_File_write_at_all(log_fh.mpi_fh, my_off,
                core->comp_buf, comp_buf_sz, MPI_BYTE, &status);
        }

        if(nprocs > 1)
        {
            /* send the ending offset from rank (n-1) to rank 0 */
            if(my_rank == (nprocs-1))
            {
                my_off += comp_buf_sz;
                PMPI_Send(&my_off, 1, MPI_OFFSET, 0, 0,
                    MPI_COMM_WORLD);
            }
            if(my_rank == 0)
            {
                PMPI_Recv(&my_off, 1, MPI_OFFSET, (nprocs-1), 0,
                    MPI_COMM_WORLD, &status);

                *inout_off = my_off;
            }
        }
        else
        {
            *inout_off = my_off + comp_buf_sz;
        }

        return(ret);
    }
#endif

    ret = pwrite(log_fh.nompi_fd, core->comp_buf, comp_buf_sz, *inout_off);
    if(ret != comp_buf_sz)  
        return(-1);
    *inout_off += comp_buf_sz;
    return(0);
}

void darshan_log_close(darshan_log_fh log_fh)
{
#ifdef HAVE_MPI
    if(using_mpi)
    {
        PMPI_File_close(&log_fh.mpi_fh);
        return;
    }
#endif

    close(log_fh.nompi_fd);
    return;
}

void darshan_log_finalize(char *logfile_name, double start_log_time)
{
#ifdef HAVE_MPI
    if(using_mpi && (my_rank > 0))
        return;
#endif

    /* finalize the darshan log file by renaming from *.darshan_partial
     * to *-<logwritetime>.darshan, indicating that this log file is complete
     * and ready for analysis
     */
        mode_t chmod_mode = S_IRUSR;
#ifdef __DARSHAN_GROUP_READABLE_LOGS
        chmod_mode |= S_IRGRP;
#endif

    if(getenv("DARSHAN_LOGFILE"))
    {
        chmod(logfile_name, chmod_mode);
    }
    else
    {
        char* tmp_index;
        double end_log_time;
        char* new_logfile_name;

        new_logfile_name = malloc(PATH_MAX);
        if(new_logfile_name)
        {
            new_logfile_name[0] = '\0';
            end_log_time = PMPI_Wtime();
            strcat(new_logfile_name, logfile_name);
            tmp_index = strstr(new_logfile_name, ".darshan_partial");
            sprintf(tmp_index, "_%d.darshan", (int)(end_log_time-start_log_time+1));
            rename(logfile_name, new_logfile_name);
            /* set permissions on log file */
            chmod(new_logfile_name, chmod_mode);
            free(new_logfile_name);
        }
    }

    return;
}
