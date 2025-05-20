Welcome to Darshan's Documentation
===================================

The Darshan source tree is divided into two parts:

* ``darshan-runtime``: to be installed on systems where you intend to
  instrument MPI applications.
* ``darshan-util``: to be installed on systems where you intend to analyze log
  files produced by darshan-runtime.

  + ``darshan-util/pydarshan``: a Python package providing interfaces to
    Darshan log file.

:ref:`Modularized I/O characterization using Darshan 3.x <TOC Modularization>`
gives details on the design of the new modularized version of Darshan (3.x)
and how new instrumentation modules may be developed within Darshan.

Site-specific documentation for facilities that deploy Darshan in production:

* Argonne Leadership Computing Facility (ALCF):
  `Theta <https://www.alcf.anl.gov/support-center/theta/darshan-theta>`_,
  `Cooley <https://www.alcf.anl.gov/support-center/cooley/darshan-cooley>`_.
* National Energy Research Scientific Computing Center
  (`NERSC <https://docs.nersc.gov/environment/#darshan-and-altd>`_)
* National Center for Supercomputing Applications
  (`NCSA <https://bluewaters.ncsa.illinois.edu/darshan>`_)
* Oak Ridge Leadership Computing Facility (OLCF):
  `darshan-runtime <https://www.olcf.ornl.gov/software_package/darshan-runtime/>`_,
  `darshan-util <https://www.olcf.ornl.gov/software_package/darshan-util/>`_.
* King Abdullah University of Science and Technology
  (`KAUST <https://www.hpc.kaust.edu.sa/sites/default/files/files/public/Parallel_IO_bh.pdf>`_)
* European Centre for Medium-Range Weather Forecasts
  (`ECMWF <https://software.ecmwf.int/wiki/display/UDOC/How+to+use+Darshan+to+profile+IO>`_)
* Ohio Supercomputer Center
  (`OSC <https://www.osc.edu/resources/available_software/software_list/darshan>`_)
* Julich Supercomputing Centre
  (`JSC <https://apps.fz-juelich.de/unite/files/DebugAndPerformanceTools-latest.pdf>`_)

.. toctree::
   :maxdepth: 2
   :caption: Darshan Runtime
   :name: TOC Darshan Runtime

   darshan-runtime/doc/darshan-runtime

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

.. toctree::
   :maxdepth: 2
   :caption: PyDarshan
   :name: pydarshantoc

   darshan-util/pydarshan/docs/readme
   darshan-util/pydarshan/docs/install
   darshan-util/pydarshan/docs/usage
   darshan-util/pydarshan/docs/api/pydarshan/modules

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

