#!/bin/bash

diff -cB include.org/darshan-logutils.h include/darshan-logutils.h > patch-darshan-logutils.patch
diff -cB include.org/darshan-log-format.h include/darshan-log-format.h > patch-darshan-log-format.patch
