Welcome to Darshan's Documentation
===================================

**NOTE: please check with your facility for site-specific documentation first if you are using a version of Darshan provided by your organization.**

The primary Darshan documentation is divided into three parts:

* :ref:`darshan-runtime <TOC Darshan Runtime>`: The portion of Darshan that
  is used to instrument applications.
* :ref:`PyDarshan <pydarshantoc>`: A Python package that provides APIs and
  command-line tools for analyzing Darshan logs.
* :ref:`darshan-util <TOC Darshan Utilities>`: The underlying C
  library and command-line tools for managing Darshan logs.

While both PyDarshan and darshan-util provide analysis capabilities, we
strongly recommend that you use PyDarshan for the most up to date features
and functionality.

In addition, :ref:`Modularized I/O characterization using Darshan 3.x <TOC
Modularization>` is a reference for developers who wish to better understand
the Darshan architecture or develop new instrumentation modules.

.. toctree::
   :maxdepth: 2
   :caption: Darshan Runtime
   :name: TOC Darshan Runtime

   darshan-runtime/doc/darshan-runtime

.. toctree::
   :maxdepth: 2
   :caption: PyDarshan
   :name: pydarshantoc

   darshan-util/pydarshan/docs/readme
   darshan-util/pydarshan/docs/install
   darshan-util/pydarshan/docs/usage
   darshan-util/pydarshan/docs/api/pydarshan/modules

.. toctree::
   :maxdepth: 2
   :caption: Darshan Utilities
   :name: TOC Darshan Utilities

   darshan-util/doc/darshan-util

.. toctree::
   :maxdepth: 2
   :caption: Modularized I/O characterization
   :name: TOC Modularization

   doc/darshan-modularization.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

