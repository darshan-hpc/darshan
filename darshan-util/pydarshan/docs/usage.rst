=====
Usage
=====


Darshan Report Object Interface
-------------------------------

To use pydarshan in a project::

	import darshan

	report = darshan.DarshanReport(filename)

	# read metadata, log records and name records
	report.read_all_generic_records()


    # Python aggregations are still experimental and have to be activated:
    # calculate or update aggregate statistics for currently loaded records
    darshan.enable_experimental()
	report.summarize()


	print(report.report)
	



Directly interfacing through the CFFI Interface Wrappers
--------------------------------------------------------

Generally, it is more convienient to access a darshan log from Python using the default report object interface which also caches already fetched information such as log records on a per module basis.
If this seems like an unwanted overhead the CFFI interface can be used which allows fine grained control on which information are loaded.


To use pydarshan.cffi_parser in a project::

    import darshan.backends.cffi_backend as darshanll

    log = darshanll.log_open("example.darshan")

    # Access various job information
    darshanll.log_get_job(log)
    # Example Return:
    # {'jobid': 4478544,
    # 'uid': 69615,
    # 'start_time': 1490000867,
    # 'end_time': 1490000983,
    # 'metadata': {'lib_ver': '3.1.3', 'h': 'romio_no_indep_rw=true;cb_nodes=4'}}


    # Access available modules and modules
    darshanll.log_get_modules(log)
    # Example Return:
    # {'POSIX': {'len': 186, 'ver': 3, 'idx': 1},
    #  'MPI-IO': {'len': 154, 'ver': 2, 'idx': 2},
    #  'LUSTRE': {'len': 87, 'ver': 1, 'idx': 6},
    #  'STDIO': {'len': 3234, 'ver': 1, 'idx': 7}}


    # Access different record types as numpy arrays, with integer and float counters seperated
    # Example Return: {'counters': array([...], dtype=uint64), 'fcounters': array([...])}
    posix_record = darshanll.log_get_posix_record(log)
    mpiio_record = darshanll.log_get_mpiio_record(log)
    stdio_record = darshanll.log_get_stdio_record(log)
    # ...


    darshanll.log_close(log)
