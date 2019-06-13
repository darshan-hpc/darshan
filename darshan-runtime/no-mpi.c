void darshan_core_shutdown_nompi()
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
    int active_mods[DARSHAN_MAX_MODS] = {0};
    uint64_t gz_fp = 0;
    char *logfile_name;
    int i;
    int ret;

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
    if(!(final_core->comp_buf))
    {
        darshan_core_cleanup(final_core);
        return;
    }

    logfile_name = malloc(PATH_MAX);
    if(!logfile_name)
    {
        darshan_core_cleanup(final_core);
        return;
    }

#if 0
    /* set which modules were used locally */
    for(i = 0; i < DARSHAN_MAX_MODS; i++)
    {
        if(final_core->mod_array[i])
            active_mods[i] = 1;
    }
#endif

    /* get the log file name */
    darshan_get_logfile_name(logfile_name, final_core->log_job_p->jobid,
        final_core->log_job_p->start_time);
    if(strlen(logfile_name) == 0)
    {
        /* failed to generate log file name */
        fprintf(stderr, "darshan library warning: unable to determine log file path\n");
        free(logfile_name);
        darshan_core_cleanup(final_core);
        return;
    }

    if(internal_timing_flag)
        open1 = darshan_mpi_wtime();
    /* collectively open the darshan log file */
    ret = darshan_log_open_nompi(logfile_name, &log_fh);
    if(internal_timing_flag)
        open2 = darshan_mpi_wtime();

    /* error out if unable to open log file */
    if(ret != 0)
    {
        if(my_rank == 0)
        {
            fprintf(stderr, "darshan library warning: unable to create log file %s\n",
                logfile_name);
        }
        free(logfile_name);
        darshan_core_cleanup(final_core);
        return;
    }

    if(internal_timing_flag)
        job1 = darshan_mpi_wtime();
    if(internal_timing_flag)
        job2 = darshan_mpi_wtime();

    return;
}
