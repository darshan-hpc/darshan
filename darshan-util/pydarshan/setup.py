#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages, Extension
import sys
import os


with open("README.rst") as readme_file:
    readme = readme_file.read()


requirements = ["cffi", "numpy", "pandas", "matplotlib", "seaborn"]
setup_requirements = [
    "pytest-runner",
]
test_requirements = ["pytest"]


# NOTE: The Python C extension is currently only used to automate
# the build process of binary wheels for distribution via PyPi.
#
# If you are building darshan yourself and make libdarshan-util.so
# discoverable in the environment by means of LD_LIBRARY_PATH or
# pkg-config there is no need to build the extension.
ext_modules = []
if "--with-extension" in sys.argv:
    ext_modules.append(
        Extension(
            "darshan.extension",
            # optional=True,
            sources=["darshan/extension.c"],
            include_dirs=["/usr/include"],
            libraries=["darshan-util"],
        )
    )
    sys.argv.remove("--with-extension")

#
# Find backend python files in modules and copy them into lib
#
for root, dirs, files in os.walk("../../modules"):
    for f in files:
        if f.endswith("-backend.py"):
            fname = f.replace("-backend", "")
            try:
                os.symlink("../../" + os.path.join(root, f), f"darshan/backend/{fname}")
            except:
                pass
            print("Adding {0} to backends.".format(os.path.join(root, f)))


setup(
    author="",
    author_email="",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    description="Python tools to interact with darshan log records of HPC applications.",
    long_description=readme,
    ext_modules=ext_modules,
    install_requires=requirements,
    include_package_data=True,
    keywords="darshan",
    name="darshan",
    packages=find_packages(include=["darshan*", "examples*", "tests"]),
    setup_requires=setup_requirements,
    test_suite="tests",
    tests_require=test_requirements,
    url='https://www.mcs.anl.gov/research/projects/darshan/',
    version='3.3.1.0',
    zip_safe=False,
    package_data={"": ["*.darshan"]},
)
