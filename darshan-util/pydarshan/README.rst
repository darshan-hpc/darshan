=======================
PyDarshan Documentation
=======================

Python utilities to interact with Darshan log records of HPC applications.
pydarshan requires darshan-utils (3.2.2+) to be installed.

Features
--------

* Darshan Report Object Wrapper
* CFFI bindings to access darshan log files
* Plots typically found in the darshan reports (matplotlib)
* Auto-discover darshan-util.so (via darshan-parser in $PATH)


Usage
-----

For examples and Jupyter notebooks to get started with pydarshan make sure
to check out the `examples` subdirectory.

A brief examples showing some of the basic functionality is the following::

    import darshan

    # Open darshan log
    report = darshan.DarshanReport('example.darshan')

    # Load some report data
    report.mod_read_all_records('POSIX')
    report.mod_read_all_records('MPI-IO')
    # or fetch all
    report.read_all_generic_records()

    # ...    
    # Generate summaries for currently loaded data
    # Note: aggregations are still experimental and have to be activated:
    darshan.enable_experimental()
    report.summarize()



Installation
------------

To install in most cases the following will work::

    pip install darshan

For alternative installation instructions and installation from source refer to <docs/install.rst>


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
