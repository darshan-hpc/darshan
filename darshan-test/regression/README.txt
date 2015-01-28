This directory contains regression tests for both the runtime and util components of Darshan,
assuming that Darshan is already compiled and installed in a known path.

The master script must be executed with three arguments:

1) path to darshan installation
2) path to temporary directory (for building executables, collecting logs, etc. during test)
3) platform type; options include:
   - ws (for a standard workstation)

The platform type should map to a subdirectory containing scripts that describe how to
perform platform-specific tasks (like loading or generating darshan wrappers and executing
jobs).

TODO:
---------
- tie this in with the "automated" directory in the tree, which already performs automated
  build tests (but no runtime tests) in Jenkins
