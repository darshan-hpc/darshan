#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, Extension
import sys
import os

if sys.version_info[:2] < (3, 7):
    raise RuntimeError("Python version >= 3.7 required.")



# NOTE: The Python C extension is currently only used to automate
# the build process of binary wheels for distribution via PyPi.
#
# If you are building darshan yourself and make libdarshan-util.so
# discoverable in the environment by means of LD_LIBRARY_PATH or
# pkg-config there is no need to build the extension.
ext_modules = []
if "PYDARSHAN_BUILD_EXT" in os.environ:
    ext_modules.append(
        Extension(
            "darshan.extension",
            # optional=True,
            sources=["darshan/extension.c"],
            include_dirs=["/usr/include"],
            libraries=["darshan-util"],
        )
    )

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
    ext_modules=ext_modules,
)
