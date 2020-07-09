# Copyright 2013-2020 Lawrence Livermore National Security, LLC and other
# Spack Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: (Apache-2.0 OR MIT)

from spack import *


class PyDarshan(PythonPackage):
    """Python wrapper -- and more -- for Aaron Quinlan's BEDTools"""

    homepage = "http://www.mcs.anl.gov/research/projects/darshan/"   
    url      = "https://pypi.io/packages/source/p/darshan/darshan-0.0.1.tar.gz"

    version('0.0.1', sha256='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')

    depends_on('py-cffi', type=('build', 'run'))
    depends_on('py-numpy', type=('build', 'run'))
    depends_on('py-pandas', type=('build', 'run'))
    depends_on('py-setuptools', type='build')
