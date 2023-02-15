.. highlight:: shell

============
Installation
============


Stable release
--------------

To install PyDarshan, run this command in your terminal:

.. code-block:: console

    $ pip install darshan

This is the preferred method to install PyDarshan, as it will always install the most recent stable release.
If you don't have `pip`_ installed, this `Python installation guide`_ can guide
you through the process.

.. _pip: https://pip.pypa.io
.. _Python installation guide: https://www.mcs.anl.gov/research/projects/darshan/


PyDarshan assumes that a recent 'darshan-utils' is installed as a shared
library. If darshan-util is not installed consult with the darshan
documentation or consider using `Spack`_ to install::

    spack install darshan-util


.. _Spack: https://spack.io/


From sources
------------

The sources for PyDarshan can be downloaded from the `Github repo`_.

You can either clone the public repository:

.. code-block:: console

    $ git clone https://github.com/darshan-hpc/darshan.git
    $ cd darshan/darshan-util/pydarshan


.. code-block:: console

    $ python setup.py install


.. _Github repo: https://github.com/darshan-hpc/darshan.git
