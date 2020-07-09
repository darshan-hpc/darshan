#!/bin/bash

# Generates a CFFI compatible header from an existing darshan-utils installations.
# Note: The python bindings utilize a number of newer functions in darshan-util.


# discover darshan
darshan_install=$(cd $(dirname $(which darshan-parser)); cd ..; pwd -L)


cp -R $darshan_install/include include
# TODO: removing all non darshan header inlcudes works farily well except for structures depending on UT_hash


# Remove unecessary includes to keep resulting header small
patch -b --verbose include/darshan-logutils.h patch-darshan-logutils.patch
patch -b --verbose include/darshan-log-format.h patch-darshan-log-format.patch

# Execute preprocessor to expand macros etc.
gcc -E include/darshan-logutils.h > generated.h

# Make header CFFI compatible by patching runtime-dependent structures (e.g., sizeof)
patch -b --verbose generated.h patch-cffi-incompatible.patch
