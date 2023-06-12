=======================
PyDarshan Documentation
=======================

Python utilities to interact with `Darshan <https://www.mcs.anl.gov/research/projects/darshan/>`__
log records of HPC applications.

Features
--------

* Job summary tool for generating HTML reports of I/O activity in a Darshan log
* Darshan Report interface for common interactive analysis tasks
* Low-level CFFI bindings for efficient access to Darshan log files
* Generic plotting capabilities (matplotlib) for Darshan log data


Usage
-----

For examples and Jupyter notebooks to get started with PyDarshan make sure
to check out the `examples` subdirectory.


Installation
------------

To install in most cases the following will work::

    pip install darshan


Testing
-------

Targets for various tests are included in the makefile. To run the normal 
test suite use::

    make test

Or to test against a different version of Python using Tox::

    make test-all

Coverage tests can be performed using::

    make coverage

Conformance to PEPs can be tested using flake8 via::

    make lint


Documentation
-------------

Documentation for the Python bindings is generated separately from the
darshan-util C library in the interest of using Sphinx. After installing the
development requirements the documentation can be built using make as follows::

    pip install -r requirements_dev.txt
    make docs

File List
---------

* darshan::
    core darshan python module code
* devel::
    scripts for building Python wheel
* docs::
    markdown documentation used by sphinx to auto-generate HTML RTD style doc
* examples::
    Jupyter notebooks showing PyDarshan usage with log files
* tests::
    PyDarshan-specific test cases
* requirements.txt::
    pip requirement file for minimum set of dependencies
* requirements_dev.txt::
    pip requirement file for dependencies needed to run development tools
* setup.py::
    python file for building/generating PyDarshan package
* setup.cfg::
    input for setup.py
* MANIFEST.in::
    input files for setup.py package
* tox.ini::
    input for tox which runs the automated testing
