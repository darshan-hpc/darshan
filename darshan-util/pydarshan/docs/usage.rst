=====
Usage
=====

To use pydarshan in a project::

    import darshan
    log = darshan.log_open("example.darshan")

    # Access various job information
    darshan.log_get_job(log)
    # Example Return:
    # {'jobid': 4478544,
    # 'uid': 69615,
    # 'start_time': 1490000867,
    # 'end_time': 1490000983,
    # 'metadata': {'lib_ver': '3.1.3', 'h': 'romio_no_indep_rw=true;cb_nodes=4'}}


    # Access available modules and modules
    darshan.log_get_modules(log)
    # Example Return:
    # {'POSIX': {'len': 186, 'ver': 3, 'idx': 1},
    #  'MPI-IO': {'len': 154, 'ver': 2, 'idx': 2},
    #  'LUSTRE': {'len': 87, 'ver': 1, 'idx': 6},
    #  'STDIO': {'len': 3234, 'ver': 1, 'idx': 7}}


    # Access different record types as numpy arrays, with integer and float counters seperated
    # Example Return: {'counters': array([...], dtype=uint64), 'fcounters': array([...])}
    posix_record = darshan.log_get_posix_record(log)
    mpiio_record = darshan.log_get_mpiio_record(log)
    stdio_record = darshan.log_get_stdio_record(log)
    # ...


    darshan.log_close(log)
