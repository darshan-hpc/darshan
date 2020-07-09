.. highlight:: shell

============
Installation
============


Stable release
--------------

To install pydarshan, run this command in your terminal:

.. code-block:: console

    $ pip install pydarshan

This is the preferred method to install pydarshan, as it will always install the most recent stable release.
If you don't have `pip`_ installed, this `Python installation guide`_ can guide
you through the process.

.. _pip: https://pip.pypa.io
.. _Python installation guide: https://www.mcs.anl.gov/research/projects/darshan/


Pydarshan assumes that a recent 'darshan-utils' is installed as a shared 
library. If darshan-util is not installed consult with the darshan
documentation or consider using `Spack`_ to install::

    spack install darshan-util


.. _Spack: https://spack.io/


From sources
------------

The sources for pydarshan can be downloaded from the `Github repo`_.

You can either clone the public repository:

.. code-block:: console

    $ git clone https://xgitlab.cels.anl.gov/darshan/darshan.git
    $ cd darshan/darshan-util/pydarshan


.. code-block:: console

    $ python setup.py install


.. _Github repo: https://xgitlab.cels.anl.gov/darshan/darshan
