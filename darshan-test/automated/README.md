This build script can be used by automated systems (e.g., Github workflows,
Jenkins, other CI systems) to build/test the Darshan runtime library and
associated tools.

The following environment variables can be set to influence the behavior of
the script:
 - DARSHAN\_INSTALL\_PREFIX - path to install Darshan
 - DARSHAN\_LOG\_PATH - formatted path to store Darshan logs
 - DARSHAN\_RUNTIME\_CONFIG\_ARGS - darshan-runtime config args
 - DARSHAN\_UTIL\_CONFIG\_ARGS - darshan-util config args
 - DARSHAN\_RUNTIME\_SKIP - skip darshan-runtime build if set
