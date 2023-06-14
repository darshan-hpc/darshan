.. highlight:: shell

.. _installation:

============
Installation
============


Stable release (PyPI)
---------------------

The preferred method for installing PyDarshan is to use `pip`_ to install stable
releases directly from PyPI:

.. code-block:: console

    $ pip install darshan

PyDarshan releases on PyPI include manylinux and macOS binary wheels that simplify
distribution of PyDarshan and its dependencies for most users.
Note that these binary wheels are currently only available for x86 architectures.

.. _pip: https://pip.pypa.io


From Spack
----------

PyDarshan is also available within the `Spack`_ package manager.

.. code-block:: console

    $ spack install py-darshan

.. _Spack: https://spack.io/


From sources
------------

The sources for PyDarshan can be obtained as part of the `Darshan Github repo`_, as shown below.

.. code-block:: console

    $ git clone https://github.com/darshan-hpc/darshan.git
    $ cd darshan/darshan-util/pydarshan

Users can then use ``pip`` to install the PyDarshan package from source.

.. code-block:: console

    $ pip install .

When building PyDarshan from sources, users need to make a copy of the darshan-util shared
library available.
On Linux systems, this is typically accomplished using `LD_LIBRARY_PATH`.

.. code-block:: console

    $ export LD_LIBRARY_PATH=/path/to/darshan/install/lib:$LD_LIBRARY_PATH

On macOS systems, `DYLD_FALLBACK_LIBRARY_PATH` should be used instead.

.. code-block:: console

    $ export DYLD_FALLBACK_LIBRARY_PATH=/path/to/darshan/install/lib:$DYLD_FALLBACK_LIBRARY_PATH

Refer to the `darshan-util docs`_ for details on how to install the shared library.
PyPI- and Spack-based installs typically do not have to worry about this step on platforms
for which we provide binary wheels.
Note that PyDarshan requires a compatible darshan-util version (e.g., 3.4.2.x versions of
PyDarshan requires a darshan-util version of 3.4.2).

.. _Darshan Github repo: https://github.com/darshan-hpc/darshan.git
.. _darshan-util docs: https://www.mcs.anl.gov/research/projects/darshan/docs/darshan-util.html
