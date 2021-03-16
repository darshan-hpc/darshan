=======================
PyDarshan Documentation
=======================

Python utilities to interact with Darshan log records of HPC applications.
PyDarshan requires darshan-utils version 3.3 or higher to be installed.

Features
--------

* Darshan Report Object for common interactive analysis tasks
* Low-level CFFI bindings for efficient access to darshan log files
* Plots typically found in the darshan reports (matplotlib)
* Bundled with darshan-utils while allowing site's darshan-utils to take precedence


Usage
-----

For examples and Jupyter notebooks to get started with pydarshan make sure
to check out the `examples` subdirectory.

A brief examples showing some of the basic functionality is the following::

    import darshan

    # Open darshan log
    report = darshan.DarshanReport('example.darshan', read_all=False)

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

    pip install --user darshan

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

File List
---------

* darshan::
    core darshan python module code
* devel::
    scripts for building python wheel
* docs::
    markdown documentation used by sphinx to auto-generate HTML RTD style doc
* examples::
    Jupyter notebooks showing pydarshan usage with log files
* tests::
    pydarshan specific test cases
* requirements.txt::
    pip requirement file for minimum set of depednencies
* requirements_dev.txt::
    pip requirement file for depednencies needed to run development tools
* setup.py::
    python file for building/generating pydarshan package
* setup.cfg::
    input for setup.py
* MANIFEST.in::
    input files for setup.py package
* tox.ini::
    input for tox which runs the automated testing
