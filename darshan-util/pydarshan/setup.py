#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages, Extension
import sys


with open('README.rst') as readme_file:
    readme = readme_file.read()


requirements = ['cffi', 'numpy', 'pandas', 'matplotlib']
setup_requirements = ['pytest-runner', ]
test_requirements = ['pytest']


ext_modules = []

if '--with-extension' in sys.argv:
    ext_modules.append(Extension(
        'darshan.extension',
        #optional=True,
        sources=['darshan/extension.c'],
        include_dirs=['/usr/include'],
        libraries=['darshan-util']
        ))
    sys.argv.remove('--with-extension')


setup(
    author='',
    author_email='',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9'
    ],
    description="Python tools to interact with darshan log records of HPC applications.",
    long_description=readme,
    ext_modules = ext_modules,  
    install_requires=requirements,
    include_package_data=True,
    keywords='darshan',
    name='darshan',
    packages=find_packages(include=['darshan*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://www.mcs.anl.gov/research/projects/darshan/',
    version='0.0.5',
    zip_safe=False,
)
