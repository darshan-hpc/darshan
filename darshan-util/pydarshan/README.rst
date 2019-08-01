=========
pydarshan
=========

Python utilities to interact with darshan log records of HPC applications.
pydarshan requires that you have darshan-util 

Features
--------

* CFFI bindings to access darshan log files
* Plots typically found in the darshan reports (matplotlib)
* Auto-discover darshan-util.so (via darshan-parser in $PATH)


Usage
-----

For examples and a jupyter notebook to get started with pydarshan make sure
to check out the `examples` subdirectory.

A brief examples showing some of the basic functionality is the following::

    import darshan
    log = darshan.log_open("example.darshan")

    # Access Job Information
    darshan.log_get_job(log)
    # Example Return:
    # {'jobid': 4478544,
    # 'uid': 69615,
    # 'start_time': 1490000867,
    # 'end_time': 1490000983,
    # 'metadata': {'lib_ver': '3.1.3', 'h': 'romio_no_indep_rw=true;cb_nodes=4'}}


    # Access available modules and modules
    darshan.log_get_modules(log)
    # Returns:
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


Installation
------------

To install use either::

    make install

Or::

	python setup.py install


Testing
-------

Targets for various tests are included in the makefile. To run the normal 
test suite use::

    make test

Or to test against different version of Python using Tox::

    make test-all

Coverage tests can be performed using::

    make coverage

Conformance to PEPs can be tested using flake8 via::

    make lint



Documentation
-------------

Documentation for the python bindings is generated seperatedly from the 
darshan-utils C library in the interest of using Sphinx. After installing the
developement requirements using `pip install -r requirements_dev.txt` the
documentation can be build using make as follows::

    pip install -r requirements_dev.txt
    make docs
