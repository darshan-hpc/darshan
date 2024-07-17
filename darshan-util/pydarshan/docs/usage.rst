.. _usage:

=====
Usage
=====

Darshan job summary tool
------------------------

As a starting point, users can use PyDarshan to generate detailed
summary HTML reports of I/O activity for a given Darshan log. An
example job summary report can be viewed `HERE <https://www.mcs.anl.gov/research/projects/darshan/docs/example_report.html>`_.

Usage of this job summary tool is described below. ::

    usage: darshan summary [-h] [--output OUTPUT] [--enable_dxt_heatmap] log_path

    Generates a Darshan Summary Report

    positional arguments:
      log_path              Specify path to darshan log.

    optional arguments:
      -h, --help            show this help message and exit
      --output OUTPUT       Specify output filename.
      --enable_dxt_heatmap  Enable DXT-based versions of I/O activity heatmaps.

For example, the following command would generate an HTML job summary report
for a Darshan log file named `example.darshan`.

.. code-block:: console

    $ python -m darshan summary example.darshan

If ``--output`` option is not specified, the output HTML report will be based
on the input log file name (i.e., the above command would generate an HTML
report named `example_report.html`).

Darshan Report interface
------------------------

Users can use the Darshan `Report` interface to help develop custom log analysis tools.
The example below demonstrates how to use this interface to open a Darshan log file,
read in log metadata and instrumentation records, and export record data to a pandas
DataFrame. ::

    import darshan

    # open a Darshan log file and read all data stored in it
    with darshan.DarshanReport(filename, read_all=True) as report:

        # print the metadata dict for this log
        print("metadata: ", report.metadata)
        # print job runtime and nprocs
        print("run_time: ", report.metadata['job']['run_time'])
        print("nprocs: ", report.metadata['job']['nprocs'])

        # print modules contained in the report
        print("modules: ", list(report.modules.keys()))

        # export POSIX module records to DataFrame and print
        posix_df = report.records['POSIX'].to_df()
        print("POSIX df: ", posix_df)


Darshan CFFI backend interface
------------------------------

Generally, it is more convenient to access a Darshan log from Python using the `Report`
interface, which also caches already fetched information such as log records on a
per-module basis.
If this seems like an unwanted overhead, the CFFI interface can be used directly to gain
fine-grained control over what log data is being loaded.

The example below demonstrates some usage of the CFFI backend for opening a
log file and accessing different types of log data::

    import darshan.backend.cffi_backend as darshanll

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

    # Access different record types as numpy arrays, with integer and float counters separated
    # Example Return: {'counters': array([...], dtype=uint64), 'fcounters': array([...])}
    posix_record = darshanll.log_get_record(log, "POSIX")
    mpiio_record = darshanll.log_get_record(log, "MPI-IO")
    stdio_record = darshanll.log_get_record(log, "STDIO")
    # ...

    darshanll.log_close(log)
