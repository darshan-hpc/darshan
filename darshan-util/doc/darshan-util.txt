Darshan-util installation and usage
===================================

== Introduction

This document describes darshan-util, a collection of tools for
parsing and summarizing log files produced by Darshan instrumentation.
The darshan-util package can be installed and used on any system
regardless of where the logs were originally generated.  Darshan log
files are platform-independent.

More information about Darshan can be found at the 
http://www.mcs.anl.gov/darshan[Darshan web site].

== Requirements

Darshan-util has only been tested in Linux environments, but will likely
work in other Unix-like environments as well.  

.Hard requirements
* C compiler
* zlib development headers and library (zlib-dev or similar)

.Optional requirements
* libbz2 development headers and library (libbz2-dev or similar)
* Perl
* pdflatex
* gnuplot 4.2 or later
* epstopdf

== Compilation and installation

.Configure and build example
----
tar -xvzf darshan-<version-number>.tar.gz
cd darshan-<version-number>/
./prepare.sh
cd darshan-util/
./configure
make
make install
----

The darshan-util package is intended to be used on a login node or
workstation.  For most use cases this means that you should
either leave `CC` to its default setting or specify a local compiler.  This is
in contrast to the darshan-runtime documentation, which suggests setting `CC`
to mpicc because the runtime library will be used in the compute node
environment.

You can specify `--prefix` to install darshan-util in a specific location
(such as in your home directory for non-root installations).  See
`./configure --help` for additional optional arguments, including how to
specify alternative paths for zlib and libbz2 development libraries.
darshan-util also supports VPATH or "out-of-tree" builds if you prefer that
method of compilation.

The `--enable-shared` argument to configure can be used to enable
compilation of a shared version of the darshan-util library.

The `--enable-apmpi-mod` and `--enable-apxc-mod` configure
arguments must be specified to build darshan-util with support for AutoPerf
APMPI and APXC modules, respectively.

[NOTE]
====
AutoPerf log analysis code is provided as Git submodules to Darshan's main repository, so if building Darshan source that has been cloned from Git, it is neccessary to first retrieve the AutoPerf submodules by running the following command:

----
git submodule update --init
----
====

== Analyzing log files

Each time a darshan-instrumented application is executed, it will generate a
single log file summarizing the I/O activity from that application.  See the
darshan-runtime documentation for more details, but the log file for a given
application will likely be found in a centralized directory, with the path
and log file name in the following format:

----
<YEAR>/<MONTH>/<DAY>/<USERNAME>_<BINARY_NAME>_<JOB_ID>_<DATE>_<UNIQUE_ID>_<TIMING>.darshan
----

This is a binary format file that summarizes I/O activity. As of version
2.0.0 of Darshan, this file is portable and does not have to be analyzed on
the same system that executed the job. Also, note that Darshan logs generated
with Darshan versions preceding version 3.0 will have the extension `darshan.gz`
(or `darshan.bz2` if compressed using bzip2 format). These logs are not compatible
with Darshan 3.0 utilities, and thus must be analyzed using an appropriate version
(2.x) of the darshan-util package.

=== darshan-job-summary.pl

You can generate a graphical summary
of the I/O activity for a job by using the `darshan-job-summary.pl` graphical summary
tool as in the following example:

----
darshan-job-summary.pl carns_my-app_id114525_7-27-58921_19.darshan.gz
----

This utility requires Perl, pdflatex, epstopdf, and gnuplot in order to
generate its summary.  By default, the output is written to a multi-page
pdf file based on the name of the input file (in this case it would
produce a `carns_my-app_id114525_7-27-58921_19.pdf` output file).
You can also manually specify the name of the output file using the
`--output` argument.

An example of the output produced by darshan-job-summary.pl can be found
link:http://www.mcs.anl.gov/research/projects/darshan/docs/ssnyder_ior-hdf5_id3655016_9-23-29011-12333993518351519212_1.darshan.pdf[HERE].

*NOTE*: The darshan-job-summary tool depends on a few LaTeX packages that may not
be availalble by default on all systems, including: lastpage, subfigure, and
threeparttable. These packages can be found and installed using your system's package
manager. For instance, the packages can be installed on Debian or Ubuntu systems as
follows: `apt-get install texlive-latex-extra`

=== darshan-summary-per-file.sh

This utility is similar to darshan-job-summary.pl, except that it produces a
separate pdf summary for every file accessed by an application.  It can be
executed as follows:

----
darshan-summary-per-file.sh carns_my-app_id114525_7-27-58921_19.darshan.gz output-dir
----

The second argument is the name of a directory (to be created) that will
contain the collection of pdf files.  Note that this utility probably
is not appropriate if your application opens a large number of files.

You can produce a summary for a specific file of interest with the following
commands:

----
darshan-convert --file HASH carns_my-app_id114525_7-27-58921_19.darshan.gz interesting_file.darshan.gz
darshan-job-summary.pl interesting_file.darshan.gz
----

The "HASH" argument is the hash of a file name as reported by
darshan-parser.  The +interesting_file.darshan.gz+ file produced by
darshan-convert is like a normal Darshan log file, but it will only contain
instrumentation for the specified file.

=== darshan-parser

You can use the `darshan-parser` command line utility to obtain a
complete, human-readable, text-format dump of all information contained
in a log file.   The following example converts the contents of the
log file into a fully expanded text file:

----
darshan-parser carns_my-app_id114525_7-27-58921_19.darshan.gz > ~/job-characterization.txt
----

The format of this output is described in the following section.

=== Guide to darshan-parser output

The beginning of the output from darshan-parser displays a summary of
overall information about the job. Additional job-level summary information
can also be produced using the `--perf`, `--file`, or `--total` command line
options.  See the <<addsummary,Additional summary output>> section for more
information about those options.

The following table defines the meaning
of each line in the default header section of the output:

[cols="25%,75%",options="header"]
|====
|output line | description
| "# darshan log version" | internal version number of the Darshan log file
| "# exe" | name of the executable that generated the log file
| "# uid" | user id that the job ran as
| "# jobid" | job id from the scheduler
| "# start_time" | start time of the job, in seconds since the epoch
| "# start_time_asci" | start time of the job, in human readable format
| "# end_time" | end time of the job, in seconds since the epoch
| "# end_time_asci" | end time of the job, in human readable format
| "# nprocs" | number of MPI processes
| "# run time" | run time of the job in seconds
|====

==== Log file region sizes

The next portion of the parser output displays the size of each region
contained within the given log file. Each log file will contain the
following regions:

* header - constant-sized uncompressed header providing data on how to properly access the log
* job data - job-level metadata (e.g., start/end time and exe name) for the log
* record table - a table mapping Darshan record identifiers to full file name paths
* module data - each module (e.g., POSIX, MPI-IO, etc.) stores their I/O characterization data in distinct regions of the log

All regions of the log file are compressed (in libz or bzip2 format), except the header.

==== Table of mounted file systems

The next portion of the output shows a table of all general purpose file
systems that were mounted while the job was running. Each line uses the
following format:

----
<mount point> <fs type>
----

==== Format of I/O characterization fields

The remainder of the output will show characteristics for each file that was
opened by the application. Each line uses the following format:

----
<module> <rank> <record id> <counter name> <counter value> <file name> <mount point> <fs type>
----

The `<module>` column specifies the module responsible for recording this piece
of I/O characterization data. The `<rank>` column indicates the rank of the process
that opened the file. A rank value of -1 indicates that all processes opened the
same file. In that case, the value of the counter represents an aggregate across all
processes. The `<record id>` is a 64 bit hash of the file path/name that was opened.
It is used as a way to uniquely differentiate each file. The `<counter name>` is
the name of the statistic that the line is reporting, while the `<counter value>` is
the value of that statistic. A value of -1 indicates that Darshan was unable to
collect statistics for that particular counter, and the value should be ignored.
The `<file name>` field shows the complete file name the record corresponds to. The
`<mount point>` is the mount point of the file system that this file belongs to and
`<fs type>` is the type of that file system.

==== I/O characterization fields

The following tables show a list of integer statistics that are available for each of
Darshan's current instrumentation modules, along with a description of each. Unless
otherwise noted, counters include all variants of the call in question, such as
`read()`, `pread()`, and `readv()` for POSIX_READS.

.POSIX module
[cols="40%,60%",options="header"]
|====
| counter name | description
| POSIX_OPENS | Count of how many times the file was opened (INCLUDING `fileno` and `dup` operations)
| POSIX_FILENOS| Count of POSIX fileno operations
| POSIX_DUPS| Count of POSIX dup operations
| POSIX_READS | Count of POSIX read operations
| POSIX_WRITES | Count of POSIX write operations
| POSIX_SEEKS | Count of POSIX seek operations
| POSIX_STATS | Count of POSIX stat operations
| POSIX_MMAPS | Count of POSIX mmap operations
| POSIX_FSYNCS | Count of POSIX fsync operations
| POSIX_FDSYNCS | Count of POSIX fdatasync operations
| POSIX_RENAME_SOURCES| Number of times this file was the source of a rename operation
| POSIX_RENAME_TARGETS| Number of times this file was the target of a rename operation
| POSIX_RENAMED_FROM | If this file was a rename target, the Darshan record ID of the first rename source
| POSIX_MODE | Mode that the file was last opened in
| POSIX_BYTES_READ | Total number of bytes that were read from the file
| POSIX_BYTES_WRITTEN | Total number of bytes written to the file
| POSIX_MAX_BYTE_READ | Highest offset in the file that was read
| POSIX_MAX_BYTE_WRITTEN | Highest offset in the file that was written
| POSIX_CONSEC_READS | Number of consecutive reads (that were immediately adjacent to the previous access)
| POSIX_CONSEC_WRITES | Number of consecutive writes (that were immediately adjacent to the previous access)
| POSIX_SEQ_READS | Number of sequential reads (at a higher offset than where the previous access left off)
| POSIX_SEQ_WRITES | Number of sequential writes (at a higher offset than where the previous access left off)
| POSIX_RW_SWITCHES | Number of times that access toggled between read and write in consecutive operations
| POSIX_MEM_NOT_ALIGNED | Number of times that a read or write was not aligned in memory
| POSIX_MEM_ALIGNMENT | Memory alignment value (chosen at compile time)
| POSIX_FILE_NOT_ALIGNED | Number of times that a read or write was not aligned in file
| POSIX_FILE_ALIGNMENT | File alignment value.  This value is detected at
runtime on most file systems.  On Lustre, however, Darshan assumes a default
value of 1 MiB for optimal file alignment.
| POSIX_MAX_READ_TIME_SIZE | Size of the slowest POSIX read operation
| POSIX_MAX_WRITE_TIME_SIZE | Size of the slowest POSIX write operation
| POSIX_SIZE_READ_* | Histogram of read access sizes at POSIX level
| POSIX_SIZE_WRITE_* | Histogram of write access sizes at POSIX level
| POSIX_STRIDE[1-4]_STRIDE | Size of 4 most common stride patterns
| POSIX_STRIDE[1-4]_COUNT | Count of 4 most common stride patterns
| POSIX_ACCESS[1-4]_ACCESS | 4 most common POSIX access sizes
| POSIX_ACCESS[1-4]_COUNT | Count of 4 most common POSIX access sizes
| POSIX_FASTEST_RANK | The MPI rank with smallest time spent in POSIX I/O (cumulative read, write, and meta times)
| POSIX_FASTEST_RANK_BYTES | The number of bytes transferred by the rank with smallest time spent in POSIX I/O (cumulative read, write, and meta times)
| POSIX_SLOWEST_RANK | The MPI rank with largest time spent in POSIX I/O (cumulative read, write, and meta times)
| POSIX_SLOWEST_RANK_BYTES | The number of bytes transferred by the rank with the largest time spent in POSIX I/O (cumulative read, write, and meta times)
| POSIX_F_*_START_TIMESTAMP | Timestamp that the first POSIX file open/read/write/close operation began
| POSIX_F_*_END_TIMESTAMP | Timestamp that the last POSIX file open/read/write/close operation ended
| POSIX_F_READ_TIME | Cumulative time spent reading at the POSIX level
| POSIX_F_WRITE_TIME | Cumulative time spent in write, fsync, and fdatasync at the POSIX level
| POSIX_F_META_TIME | Cumulative time spent in open, close, stat, and seek at the POSIX level
| POSIX_F_MAX_READ_TIME | Duration of the slowest individual POSIX read operation
| POSIX_F_MAX_WRITE_TIME | Duration of the slowest individual POSIX write operation
| POSIX_F_FASTEST_RANK_TIME | The time of the rank which had the smallest amount of time spent in POSIX I/O (cumulative read, write, and meta times)
| POSIX_F_SLOWEST_RANK_TIME | The time of the rank which had the largest amount of time spent in POSIX I/O (cumulative read, write, and meta times)
| POSIX_F_VARIANCE_RANK_TIME | The population variance for POSIX I/O time of all the ranks
| POSIX_F_VARIANCE_RANK_BYTES | The population variance for bytes transferred of all the ranks
|====

.MPI-IO module
[cols="40%,60%",options="header"]
|====
| counter name | description
| MPIIO_INDEP_OPENS | Count of non-collective MPI opens
| MPIIO_COLL_OPENS | Count of collective MPI opens
| MPIIO_INDEP_READS | Count of non-collective MPI reads
| MPIIO_INDEP_WRITES | Count of non-collective MPI writes
| MPIIO_COLL_READS | Count of collective MPI reads
| MPIIO_COLL_WRITES | Count of collective MPI writes
| MPIIO_SPLIT_READS | Count of MPI split collective reads
| MPIIO_SPLIT_WRITES | Count of MPI split collective writes
| MPIIO_NB_READS | Count of MPI non-blocking reads
| MPIIO_NB_WRITES | Count of MPI non-blocking writes
| MPIIO_SYNCS | Count of MPI file syncs
| MPIIO_HINTS | Count of MPI file hints used
| MPIIO_VIEWS | Count of MPI file views used
| MPIIO_MODE | MPI mode that the file was last opened in
| MPIIO_BYTES_READ | Total number of bytes that were read from the file at MPI level
| MPIIO_BYTES_WRITTEN | Total number of bytes written to the file at MPI level
| MPIIO_RW_SWITCHES | Number of times that access toggled between read and write in consecutive MPI operations
| MPIIO_MAX_READ_TIME_SIZE | Size of the slowest MPI read operation
| MPIIO_MAX_WRITE_TIME_SIZE | Size of the slowest MPI write operation
| MPIIO_SIZE_READ_AGG_* | Histogram of total size of read accesses at MPI level, even if access is noncontiguous
| MPIIO_SIZE_WRITE_AGG_* | Histogram of total size of write accesses at MPI level, even if access is noncontiguous
| MPIIO_ACCESS[1-4]_ACCESS | 4 most common MPI aggregate access sizes
| MPIIO_ACCESS[1-4]_COUNT | Count of 4 most common MPI aggregate access sizes
| MPIIO_FASTEST_RANK | The MPI rank with smallest time spent in MPI I/O (cumulative read, write, and meta times)
| MPIIO_FASTEST_RANK_BYTES | The number of bytes transferred by the rank with smallest time spent in MPI I/O (cumulative read, write, and meta times)
| MPIIO_SLOWEST_RANK | The MPI rank with largest time spent in MPI I/O (cumulative read, write, and meta times)
| MPIIO_SLOWEST_RANK_BYTES | The number of bytes transferred by the rank with the largest time spent in MPI I/O (cumulative read, write, and meta times)
| MPIIO_F_*_START_TIMESTAMP | Timestamp that the first MPIIO file open/read/write/close operation began
| MPIIO_F_*_END_TIMESTAMP | Timestamp that the last MPIIO file open/read/write/close operation ended
| MPIIO_F_READ_TIME | Cumulative time spent reading at MPI level
| MPIIO_F_WRITE_TIME | Cumulative time spent write and sync at MPI level
| MPIIO_F_META_TIME | Cumulative time spent in open and close at MPI level
| MPIIO_F_MAX_READ_TIME | Duration of the slowest individual MPI read operation
| MPIIO_F_MAX_WRITE_TIME | Duration of the slowest individual MPI write operation
| MPIIO_F_FASTEST_RANK_TIME | The time of the rank which had the smallest amount of time spent in MPI I/O (cumulative read, write, and meta times)
| MPIIO_F_SLOWEST_RANK_TIME | The time of the rank which had the largest amount of time spent in MPI I/O (cumulative read, write, and meta times)
| MPIIO_F_VARIANCE_RANK_TIME | The population variance for MPI I/O time of all the ranks
| MPIIO_F_VARIANCE_RANK_BYTES | The population variance for bytes transferred of all the ranks at MPI level
|====


.STDIO module
[cols="40%,60%",options="header"]
|====
| counter name | description
| STDIO_OPENS | Count of stdio file open operations (INCLUDING `fdopen` operations)
| STDIO_FDOPENS| Count of stdio fdopen operations
| STDIO_READS | Count of stdio read operations
| STDIO_WRITES | Count of stdio write operations
| STDIO_SEEKS | Count of stdio seek operations
| STDIO_FLUSHES | Count of stdio flush operations
| STDIO_BYTES_WRITTEN | Total number of bytes written to the file using stdio operations
| STDIO_BYTES_READ | Total number of bytes read from the file using stdio operations
| STDIO_MAX_BYTE_READ | Highest offset in the file that was read
| STDIO_MAX_BYTE_WRITTEN | Highest offset in the file that was written
| STDIO_FASTEST_RANK | The MPI rank with the smallest time spent in stdio operations (cumulative read, write, and meta times)
| STDIO_FASTEST_RANK_BYTES | The number of bytes transferred by the rank with the smallest time spent in stdio operations (cumulative read, write, and meta times)
| STDIO_SLOWEST_RANK | The MPI rank with the largest time spent in stdio operations (cumulative read, write, and meta times)
| STDIO_SLOWEST_RANK_BYTES | The number of bytes transferred by the rank with the largest time spent in stdio operations (cumulative read, write, and meta times)
| STDIO_F_META_TIME | Cumulative time spent in stdio open/close/seek operations
| STDIO_F_WRITE_TIME | Cumulative time spent in stdio write operations
| STDIO_F_READ_TIME | Cumulative time spent in stdio read operations
| STDIO_F_*_START_TIMESTAMP | Timestamp that the first stdio file open/read/write/close operation began
| STDIO_F_*_END_TIMESTAMP | Timestamp that the last stdio file open/read/write/close operation ended
| STDIO_F_FASTEST_RANK_TIME | The time of the rank which had the smallest time spent in stdio I/O (cumulative read, write, and meta times)
| STDIO_F_SLOWEST_RANK_TIME | The time of the rank which had the largest time spent in stdio I/O (cumulative read, write, and meta times)
| STDIO_F_VARIANCE_RANK_TIME | The population variance for stdio I/O time of all the ranks
| STDIO_F_VARIANCE_RANK_BYTES | The population variance for bytes transferred of all the ranks
|====

.H5F module
[cols="40%,60%",options="header"]
|====
| counter name | description
| H5F_OPENS | Count of H5F opens
| H5F_FLUSHES | Count of H5F flushes
| H5F_USE_MPIIO | Flag indicating whether MPI-IO is used for accessing the file
| H5F_F_*_START_TIMESTAMP | Timestamp that the first H5F open/close operation began
| H5F_F_*_END_TIMESTAMP | Timestamp that the last H5F open/close operation ended
| H5F_F_META_TIME | Cumulative time spent in H5F open/close/flush operations
|====

.H5D module
[cols="40%,60%",options="header"]
|====
| counter name | description
| H5D_OPENS | Count of H5D opens
| H5D_READS | Count of H5D reads
| H5D_WRITES | Count of H5D writes
| H5D_FLUSHES | Count of H5D flushes
| H5D_BYTES_READ | Total number of bytes read from the dataset using H5D
| H5D_BYTES_WRITTEN | Total number of bytes written to the dataset using H5D
| H5D_RW_SWITCHES | Number of times that access toggled between read and write in consecutive H5D operations
| H5D_REGULAR_HYPERSLAB_SELECTS | Number of H5D read/write ops with regular hyperslab selections
| H5D_IRREGULAR_HYPERSLAB_SELECTS | Number of H5D read/write ops with irregular hyperslab selections
| H5D_POINT_SELECTS | Number of read/write ops with point selections
| H5D_MAX_READ_TIME_SIZE | Size of the slowest H5D read operation
| H5D_MAX_WRITE_TIME_SIZE | Size of the slowest H5D write operation
| H5D_SIZE_READ_AGG_* | Histogram of total size of read accesses at H5D level
| H5D_SIZE_WRITE_AGG_* | Histogram of total size of write accesses at H5D level
| H5D_ACCESS[1-4]_ACCESS | Sizes of 4 most common H5D accesses
| H5D_ACCESS[1-4]_LENGTH_D[1-5] | Access lengths along last 5 dimensions (D5 is fastest changing) of 4 most common H5D accesses
| H5D_ACCESS[1-4]_STRIDE_D[1-5] | Access strides along last 5 dimensions (D5 is fastest changing) of 4 most common H5D accesses
| H5D_ACCESS[1-4]_COUNT | Count of 4 most common H5D aggregate access sizes
| H5D_DATASPACE_NDIMS | Number of dimensions in dataset's dataspace
| H5D_DATASPACE_NPOINTS | Number of points in dataset's dataspace
| H5D_DATATYPE_SIZE | Total size of dataset elements in bytes
| H5D_CHUNK_SIZE_D[1-5] | Chunk sizes in the last 5 dimensions of the dataset (D5 is the fastest changing dimension)
| H5D_USE_MPIIO_COLLECTIVE | Flag indicating use of MPI-IO collectives
| H5D_USE_DEPRECATED | Flag indicating whether deprecated create/open calls were used
| H5D_FASTEST_RANK | The MPI rank with smallest time spent in H5D I/O (cumulative read, write, and meta times)
| H5D_FASTEST_RANK_BYTES | The number of bytes transferred by the rank with smallest time spent in H5D I/O (cumulative read, write, and meta times)
| H5D_SLOWEST_RANK | The MPI rank with largest time spent in H5D I/O (cumulative read, write, and meta times)
| H5D_SLOWEST_RANK_BYTES | The number of bytes transferred by the rank with the largest time spent in H5D I/O (cumulative read, write, and meta times)
| H5D_F_*_START_TIMESTAMP | Timestamp that the first H5D open/read/write/close operation began
| H5D_F_*_END_TIMESTAMP | Timestamp that the last H5D open/read/write/close operation ended
| H5D_F_READ_TIME | Cumulative time spent reading at H5D level
| H5D_F_WRITE_TIME | Cumulative time spent writing at H5D level
| H5D_F_META_TIME | Cumulative time spent in open/close/flush at H5D level
| H5D_F_MAX_READ_TIME | Duration of the slowest individual H5D read operation
| H5D_F_MAX_WRITE_TIME | Duration of the slowest individual H5D write operation
| H5D_F_FASTEST_RANK_TIME | The time of the rank which had the smallest amount of time spent in H5D I/O (cumulative read, write, and meta times)
| H5D_F_SLOWEST_RANK_TIME | The time of the rank which had the largest amount of time spent in H5D I/O (cumulative read, write, and meta times)
| H5D_F_VARIANCE_RANK_TIME | The population variance for H5D I/O time of all the ranks
| H5D_F_VARIANCE_RANK_BYTES | The population variance for bytes transferred of all the ranks at H5D level
| H5D_FILE_REC_ID | Darshan file record ID of the file the dataset belongs to
|====

.PNETCDF_FILE module
[cols="40%,60%",options="header"]
|====
| counter name | description
| PNETCDF_FILE_CREATES | PnetCDF file create operation counts
| PNETCDF_FILE_OPENS | PnetCDF file open operation counts
| PNETCDF_FILE_REDEFS | PnetCDF file re-define operation counts
| PNETCDF_FILE_INDEP_WAITS | PnetCDF independent file wait operation counts (for flushing non-blocking I/O)
| PNETCDF_FILE_COLL_WAITS | PnetCDF collective file wait operation counts (for flushing non-blocking I/O)
| PNETCDF_FILE_SYNCS | PnetCDF file sync operation counts
| PNETCDF_FILE_BYTES_READ | PnetCDF total bytes read for all file variables (includes internal library metadata I/O)
| PNETCDF_FILE_BYTES_WRITTEN | PnetCDF total bytes written for all file variables (includes internal library metadata I/O)
| PNETCDF_FILE_WAIT_FAILURES | PnetCDF file wait operation failure counts (failures indicate that variable-level counters are unreliable)
| PNETCDF_FILE_F_*_START_TIMESTAMP | Timestamp that the first PNETCDF file open/close/wait operation began
| PNETCDF_FILE_F_*_END_TIMESTAMP | Timestamp that the last PNETCDF file open/close/wait operation ended
| PNETCDF_FILE_F_META_TIME | Cumulative time spent in file open/close/sync/redef/enddef metadata operations
| PNETCDF_FILE_F_WAIT_TIME | Cumulative time spent in file wait operations (for flushing non-blocking I/O)
|====

.PNETCDF_VAR module
[cols="40%,60%",options="header"]
|====
| counter name | description
| PNETCDF_VAR_OPENS | PnetCDF variable define/inquire operation counts
| PNETCDF_VAR_INDEP_READS | PnetCDF variable independent read operation counts
| PNETCDF_VAR_INDEP_WRITES | PnetCDF variable independent write operation counts
| PNETCDF_VAR_COLL_READS | PnetCDF variable collective read operation counts
| PNETCDF_VAR_COLL_WRITES | PnetCDF variable collective write operation counts
| PNETCDF_VAR_NB_READS | PnetCDF variable nonblocking read operation counts
| PNETCDF_VAR_NB_WRITES | PnetCDF variable nonblocking write operation counts
| PNETCDF_VAR_BYTES_* | total bytes read and written at PnetCDF variable layer (not including internal library metadata I/O)
| PNETCDF_VAR_RW_SWITCHES | number of times access alternated between read and write
| PNETCDF_VAR_PUT_VAR* | number of calls to different ncmpi_put_var* APIs (var, var1, vara, vars, varm, varn, vard)
| PNETCDF_VAR_GET_VAR* | number of calls to different ncmpi_get_var* APIs (var, var1, vara, vars, varm, varn, vard)
| PNETCDF_VAR_IPUT_VAR* | number of calls to different ncmpi_iput_var* APIs (var, var1, vara, vars, varm, varn)
| PNETCDF_VAR_IGET_VAR* | number of calls to different ncmpi_iget_var* APIs (var, var1, vara, vars, varm, varn)
| PNETCDF_VAR_BPUT_VAR* | number of calls to different ncmpi_bput_var* APIs (var, var1, vara, vars, varm, varn)
| PNETCDF_VAR_MAX_*_TIME_SIZE | size of the slowest read and write operations
| PNETCDF_VAR_SIZE_*_AGG_* | histogram of PnetCDf total access sizes for read and write operations
| PNETCDF_VAR_ACCESS*_* | the four most common total accesses, in terms of size and length/stride (in last 5 dimensions)
| PNETCDF_VAR_ACCESS*_COUNT | count of the four most common total access sizes
| PNETCDF_VAR_NDIMS | number of dimensions in the variable
| PNETCDF_VAR_NPOINTS | number of points in the variable
| PNETCDF_VAR_DATATYPE_SIZE | size of each variable element
| PNETCDF_VAR_*_RANK | rank of the processes that were the fastest and slowest at I/O (for shared datasets)
| PNETCDF_VAR_*_RANK_BYTES | total bytes transferred at PnetCDF layer by the fastest and slowest ranks (for shared datasets)
| PNETCDF_VAR_F_*_START_TIMESTAMP | timestamp of first PnetCDF variable open/read/write/close
| PNETCDF_VAR_F_*_END_TIMESTAMP | timestamp of last PnetCDF variable open/read/write/close
| PNETCDF_VAR_F_READ/WRITE/META_TIME | cumulative time spent in PnetCDF read, write, or metadata operations
| PNETCDF_VAR_F_MAX_*_TIME | duration of the slowest PnetCDF read and write operations
| PNETCDF_VAR_F_*_RANK_TIME | fastest and slowest I/O time for a single rank (for shared datasets)
| PNETCDF_VAR_F_VARIANCE_RANK_* | variance of total I/O time and bytes moved for all ranks (for shared datasets)
| PNETCDF_VAR_FILE_REC_ID | Darshan file record ID of the file the variable belongs to
|====

.Lustre module (if enabled, for Lustre file systems)
[cols="40%,60%",options="header"]
|====
| counter name | description
| LUSTRE_NUM_COMPONENTS | number of instrumented components in the Lustre layout
| LUSTRE_NUM_STRIPES | number of active stripes in the Lustre layout components
| LUSTRE_COMP*_STRIPE_SIZE | stripe size for this file layout component in bytes
| LUSTRE_COMP*_STRIPE_COUNT | number of OSTs over which the file layout component is striped
| LUSTRE_COMP*_STRIPE_PATTERN | pattern (e.g., raid0, mdt, overstriped) for this file layout component
| LUSTRE_COMP*_FLAGS | captured flags (e.g. init, prefwr, stale) for this file layout component
| LUSTRE_COMP*_EXT_START | starting file extent for this file layout component
| LUSTRE_COMP*_EXT_END | ending file extent for this file layout component (-1 means EOF)
| LUSTRE_COMP*_MIRROR_ID | mirror ID for this file layout component, if mirrors are enabled
| LUSTRE_COMP*_POOL_NAME | Lustre OST pool used for this file layout component
| LUSTRE_COMP*\_OST_ID_* | indices of OSTs over which this file layout component is striped
|====

.DFS (DAOS File System) module (if enabled)
[cols="40%,60%",options="header"]
|====
| counter name | description
| DFS_OPENS | DFS file open operation counts
| DFS_GLOBAL_OPENS | DFS file global open operation (i.e., `dfs_obj_global2local()`) counts
| DFS_LOOKUPS | DFS file lookup operation counts
| DFS_DUPS | DFS file dup operation counts
| DFS_READS | DFS file read operation counts
| DFS_READXS | DFS non-contiguous file read operation counts
| DFS_WRITES | DFS file write operation counts
| DFS_WRITEXS | DFS non-contiguous file write operation counts
| DFS_NB_READS | DFS non-blocking file read operation counts (included in read/readx counts)
| DFS_NB_WRITES | DFS non-blocking file write operation counts (included in write/writex counts)
| DFS_GET_SIZES | DFS file get size operation counts
| DFS_PUNCHES | DFS file punch operation counts
| DFS_REMOVES | DFS file remove operation counts
| DFS_STATS | DFS file stat operation counts
| DFS_BYTES_READ | Total number of bytes that were read from the DFS file
| DFS_BYTES_WRITTEN | Total number of bytes that were written to the DFS file
| DFS_RW_SWITCHES | Number of times that access toggled between read and write in consecutive operations
| DFS_MAX_READ_TIME_SIZE | Size of the slowest DFS read operation
| DFS_MAX_WRITE_TIME_SIZE | Size of the slowest DFS write operation
| DFS_SIZE_READ_* | Histogram of read access sizes at DFS level
| DFS_SIZE_WRITE_* | Histogram of write access sizes at DFS level
| DFS_ACCESS[1-4]_ACCESS | 4 most common DFS access sizes
| DFS_ACCESS[1-4]_COUNT | Count of 4 most common DFS access sizes
| DFS_CHUNK_SIZE | DFS file chunk size
| DFS_FASTEST_RANK | The MPI rank with smallest time spent in DFS I/O (cumulative read, write, and meta times)
| DFS_FASTEST_RANK_BYTES | The number of bytes transferred by the rank with smallest time spent in DFS I/O (cumulative read, write, and meta times)
| DFS_SLOWEST_RANK | The MPI rank with largest time spent in DFS I/O (cumulative read, write, and meta times)
| DFS_SLOWEST_RANK_BYTES | The number of bytes transferred by the rank with the largest time spent in DFS I/O (cumulative read, write, and meta times)
| DFS_F_*_START_TIMESTAMP | Timestamp that the first DFS file open/read/write/close operation began
| DFS_F_*_END_TIMESTAMP | Timestamp that the last DFS file open/read/write/close operation ended
| DFS_F_READ_TIME | Cumulative time spent reading at the DFS level
| DFS_F_WRITE_TIME | Cumulative time spent writing at the DFS level
| DFS_F_META_TIME | Cumulative time spent in open, dup, lookup, get size, punch, release, remove, and stat at the DFS level
| DFS_F_MAX_READ_TIME | Duration of the slowest individual DFS read operation
| DFS_F_MAX_WRITE_TIME | Duration of the slowest individual DFS write operation
| DFS_F_FASTEST_RANK_TIME | The time of the rank which had the smallest amount of time spent in DFS I/O (cumulative read, write, and meta times)
| DFS_F_SLOWEST_RANK_TIME | The time of the rank which had the largest amount of time spent in DFS I/O (cumulative read, write, and meta times)
|====

.DAOS module (if enabled)
[cols="40%,60%",options="header"]
|====
| counter name | description
| DAOS_OBJ_OPENS | DAOS object open operation counts
| DAOS_OBJ_FETCHES | DAOS object fetch operation counts
| DAOS_OBJ_UPDATES | DAOS object update operation counts
| DAOS_OBJ_PUNCHES | DAOS object punch operation counts
| DAOS_OBJ_DKEY_PUNCHES | DAOS object dkey punch operation counts
| DAOS_OBJ_AKEY_PUNCHES | DAOS object akey punch operation counts
| DAOS_OBJ_DKEY_LISTS | DAOS object dkey list operation counts
| DAOS_OBJ_AKEY_LISTS | DAOS object akey list operation counts
| DAOS_OBJ_RECX_LISTS | DAOS object recx list operation counts
| DAOS_ARRAY_OPENS | DAOS array object open operation counts
| DAOS_ARRAY_READS | DAOS array object read operation counts
| DAOS_ARRAY_WRITES | DAOS array object write operation counts
| DAOS_ARRAY_GET_SIZES | DAOS array object get size operation counts
| DAOS_ARRAY_SET_SIZES | DAOS array object set size operation counts
| DAOS_ARRAY_STATS | DAOS array object stat operation counts
| DAOS_ARRAY_PUNCHES | DAOS array object punch operation counts
| DAOS_ARRAY_DESTROYS | DAOS array object destroy operation counts
| DAOS_KV_OPENS | DAOS kv object open operation counts
| DAOS_KV_GETS | DAOS kv object get operation counts
| DAOS_KV_PUTS | DAOS kv object put operation counts
| DAOS_KV_REMOVES | DAOS kv object remove operation counts
| DAOS_KV_LISTS | DAOS kv object list operation counts
| DAOS_KV_DESTROYS | DAOS kv object destroy operation counts
| DAOS_NB_OPS | DAOS non-blocking I/O operations (includes reads, writes, and metadata operations)
| DAOS_BYTES_READ | Total number of bytes that were read from the DAOS object
| DAOS_BYTES_WRITTEN | Total number of bytes that were written to the DAOS object
| DAOS_RW_SWITCHES | Number of times that access toggled between read and write in consecutive operations
| DAOS_MAX_READ_TIME_SIZE | Size of the slowest DAOS read operation
| DAOS_MAX_WRITE_TIME_SIZE | Size of the slowest DAOS write operation
| DAOS_SIZE_READ_* | Histogram of read access sizes at DAOS level
| DAOS_SIZE_WRITE_* | Histogram of write access sizes at DAOS level
| DAOS_ACCESS[1-4]_ACCESS | 4 most common DAOS access sizes
| DAOS_ACCESS[1-4]_COUNT | Count of 4 most common DAOS access sizes
| DAOS_OBJ_OTYPE | DAOS object otype ID
| DAOS_ARRAY_CELL_SIZE | For DAOS array objects, the array cell size
| DAOS_ARRAY_CHUNK_SIZE | For DAOS array objects, the array chunk size
| DAOS_FASTEST_RANK | The MPI rank with smallest time spent in DAOS I/O (cumulative read, write, and meta times)
| DAOS_FASTEST_RANK_BYTES | The number of bytes transferred by the rank with smallest time spent in DAOS I/O (cumulative read, write, and meta times)
| DAOS_SLOWEST_RANK | The MPI rank with largest time spent in DAOS I/O (cumulative read, write, and meta times)
| DAOS_SLOWEST_RANK_BYTES | The number of bytes transferred by the rank with the largest time spent in DAOS I/O (cumulative read, write, and meta times)
| DAOS_F_*_START_TIMESTAMP | Timestamp that the first DAOS object open/read/write/close operation began
| DAOS_F_*_END_TIMESTAMP | Timestamp that the last DAOS object open/read/write/close operation ended
| DAOS_F_READ_TIME | Cumulative time spent reading at the DAOS level
| DAOS_F_WRITE_TIME | Cumulative time spent writing at the DAOS level
| DAOS_F_META_TIME | Cumulative time spent in open, punch, list, get size, set size, stat, destroy, and remove at the DAOS level
| DAOS_F_MAX_READ_TIME | Duration of the slowest individual DAOS read operation
| DAOS_F_MAX_WRITE_TIME | Duration of the slowest individual DAOS write operation
| DAOS_F_FASTEST_RANK_TIME | The time of the rank which had the smallest amount of time spent in DAOS I/O (cumulative read, write, and meta times)
| DAOS_F_SLOWEST_RANK_TIME | The time of the rank which had the largest amount of time spent in DAOS I/O (cumulative read, write, and meta times)
|====

===== Heatmap fields

Each heatmap module record reports a histogram of the number of bytes read
or written, per process, over time, for a given I/O API.  It provides
a synopsis of I/O intensity regardless of how many files are accessed.
Heatmap records are never aggregated across ranks.

The file name field is used to indicate the API that produced the
histogram record.  For exmaple, "heatmap:POSIX" indicates that the record is
reporting I/O traffic that passed through the POSIX module.

The number of BIN fields present in each record may vary depending on the
job's execution time and the configurable maximum number of bins chosen at
execution time.

.HEATMAP module
[cols="40%,60%",options="header"]
|====
| counter name | description
| HEATMAP_F_BIN_WIDTH_SECONDS | time duration of each heatmap bin
| HEATMAP_READ\|WRITE_BIN_* | number of bytes read or written within specified heatmap bin
|====

===== Additional modules

.APXC module header record (if enabled, for Cray XC systems)
[cols="40%,60%",options="header"]
|====
| counter name | description
| APXC_GROUPS | total number of groups for the job
| APXC_CHASSIS | total number of chassis for the job
| APXC_BLADES | total number of blades for the job
| APXC_MEMORY_MODE | Intel Xeon memory mode
| APXC_CLUSTER_MODE | Intel Xeon NUMA configuration
| APXC_MEMORY_MODE_CONSISTENT | Intel Xeon memory mode consistent across all nodes
| APXC_CLUSTER_MODE_CONSISTENT | Intel Xeon cluster mode consistent across all nodes
|====

.APXC module per-router record (if enabled, for Cray XC systems)
[cols="40%,60%",options="header"]
|====
| counter name | description
| APXC_GROUP | group this router is on
| APXC_CHASSIS | chassis this router is on
| APXC_BLADE | blade this router is on
| APXC_NODE | node connected to this router
| APXC_AR_RTR_x_y_INQ_PRF_INCOMING_FLIT_VC[0-7] | flits on VCs of x y tile for router-router ports
| APXC_AR_RTR_x_y_INQ_PRF_ROWBUS_STALL_CNT | stalls on x y tile for router-router ports
| APXC_AR_RTR_PT_x_y_INQ_PRF_INCOMING_FLIT_VC[0,4] | flits on VCs of x y tile for router-nic ports
| APXC_AR_RTR_PT_x_y_INQ_PRF_REQ_ROWBUS_STALL_CNT | stalls on x y tile for router-nic ports
|====

.APMPI module header record (if enabled, for MPI applications)
[cols="40%,60%",options="header"]
|====
| counter name | description
| MPI_TOTAL_COMM_TIME_VARIANCE | variance in total communication time across all the processes
| MPI_TOTAL_COMM_SYNC_TIME_VARIANCE | variance in total sync time across all the processes, if enabled
|====

.APMPI module per-process record (if enabled, for MPI applications)
[cols="40%,60%",options="header"]
|====
| counter name | description
| MPI_PROCESSOR_NAME | name of the processor used by the MPI process
| MPI_*_CALL_COUNT | total call count for an MPI op
| MPI_*_TOTAL_BYTES | total bytes (i.e., cumulative across all calls) moved with an MPI op
| MPI_*\_MSG_SIZE_AGG_* | histogram of total bytes moved for all the calls of an MPI op
| MPI_*_TOTAL_TIME | total time (i.e, cumulative across all calls) of an MPI op
| MPI_*_MIN_TIME | minimum time across all calls of an MPI op
| MPI_*_MAX_TIME | maximum time across all calls of an MPI op
| MPI_*_TOTAL_SYNC_TIME | total sync time (cumulative across all calls of an op) of an MPI op, if enabled
| MPI_TOTAL_COMM_TIME | total communication (MPI) time of a process across all the MPI ops
| MPI_TOTAL_COMM_SYNC_TIME | total sync time of a process across all the MPI ops, if enabled
|====


.BG/Q module (if enabled on BG/Q systems)
[cols="40%,60%",options="header"]
|====
| counter name | description
| BGQ_CSJOBID | Control system job ID
| BGQ_NNODES | Total number of BG/Q compute nodes
| BGQ_RANKSPERNODE | Number of MPI ranks per compute node
| BGQ_DDRPERNODE | Size of compute node DDR in MiB
| BGQ_INODES | Total number of BG/Q I/O nodes
| BGQ_ANODES | Dimension of A torus
| BGQ_BNODES | Dimension of B torus
| BGQ_CNODES | Dimension of C torus
| BGQ_DNODES | Dimension of D torus
| BGQ_ENODES | Dimension of E torus
| BGQ_TORUSENABLED | Bitfield indicating enabled torus dimensions
| BGQ_F_TIMESTAMP | Timestamp of when BG/Q data was collected
|====

==== Additional summary output
[[addsummary]]

The following sections describe addtitional parser options that provide
summary I/O characterization data for the given log.

*NOTE*: These options are currently only supported by the POSIX, MPI-IO, and stdio modules.

===== Performance

Job performance information can be generated using the `--perf` command-line option.

.Example output
----
# performance
# -----------
# total_bytes: 134217728
#
# I/O timing for unique files (seconds):
# ...........................
# unique files: slowest_rank_io_time: 0.000000
# unique files: slowest_rank_meta_only_time: 0.000000
# unique files: slowest_rank: 0
#
# I/O timing for shared files (seconds):
# (multiple estimates shown; time_by_slowest is generally the most accurate)
# ...........................
# shared files: time_by_cumul_io_only: 0.042264
# shared files: time_by_cumul_meta_only: 0.000325
# shared files: time_by_open: 0.064986
# shared files: time_by_open_lastio: 0.064966
# shared files: time_by_slowest: 0.057998
#
# Aggregate performance, including both shared and unique files (MiB/s):
# (multiple estimates shown; agg_perf_by_slowest is generally the most
# accurate)
# ...........................
# agg_perf_by_cumul: 3028.570529
# agg_perf_by_open: 1969.648064
# agg_perf_by_open_lastio: 1970.255248
# agg_perf_by_slowest: 2206.983935
----

The `total_bytes` line shows the total number of bytes transferred
(read/written) by the job.  That is followed by three sections:

.I/O timing for unique files

This section reports information about any files that were *not* opened
by every rank in the job.  This includes independent files (opened by
1 process) and partially shared files (opened by a proper subset of
the job's processes). The I/O time for this category of file access
is reported based on the *slowest* rank of all processes that performed this
type of file access.

* unique files: slowest_rank_io_time: total I/O time for unique files
  (including both metadata + data transfer time)
* unique files: slowest_rank_meta_only_time: metadata time for unique files
* unique files: slowest_rank: the rank of the slowest process

.I/O timing for shared files

This section reports information about files that were globally shared (i.e.
opened by every rank in the job).  This section estimates performance for
globally shared files using four different methods.  The `time_by_slowest`
is generally the most accurate, but it may not available in some older Darshan
log files. 

* shared files: time_by_cumul_*: adds the cumulative time across all
  processes and divides by the number of processes (inaccurate when there is
  high variance among processes).
** shared files: time_by_cumul_io_only: include metadata AND data transfer
   time for global shared files
** shared files: time_by_cumul_meta_only: metadata time for global shared
   files
* shared files: time_by_open: difference between timestamp of open and
  close (inaccurate if file is left open without I/O activity)
* shared files: time_by_open_lastio: difference between timestamp of open
  and the timestamp of last I/O (similar to above but fixes case where file is
  left open after I/O is complete)
* shared files: time_by_slowest : measures time according to which rank was
  the slowest to perform both metadata operations and data transfer for each
  shared file. (most accurate but requires newer log version)

.Aggregate performance

Performance is calculated by dividing the total bytes by the I/O time
(shared files and unique files combined) computed
using each of the four methods described in the previous output section. Note the unit for total bytes is
Byte and for the aggregate performance is MiB/s (1024*1024 Bytes/s).

===== Files
Use the `--file` option to get totals based on file usage.
Each line has 3 columns. The first column is the count of files for that
type of file, the second column is number of bytes for that type, and the third
column is the maximum offset accessed.

* total: All files
* read_only: Files that were only read from
* write_only: Files that were only written to
* read_write: Files that were both read and written
* unique: Files that were opened on only one rank
* shared: Files that were opened by more than one rank


.Example output
----
# <file_type> <file_count> <total_bytes> <max_byte_offset>
# total: 5 4371499438884 4364699616485
# read_only: 2 4370100334589 4364699616485
# write_only: 1 1399104295 1399104295
# read_write: 0 0 0
# unique: 0 0 0
# shared: 5 4371499438884 4364699616485
----

===== Totals

Use the `--total` option to get all statistics as an aggregate total rather
than broken down per file.  Each field is either summed across files and
process (for values such as number of opens), set to global minimums and
maximums (for values such as open time and close time), or zeroed out (for
statistics that are nonsensical in aggregate).

.Example output
----
total_POSIX_OPENS: 1024
total_POSIX_READS: 0
total_POSIX_WRITES: 16384
total_POSIX_SEEKS: 16384
total_POSIX_STATS: 1024
total_POSIX_MMAPS: 0
total_POSIX_FOPENS: 0
total_POSIX_FREADS: 0
total_POSIX_FWRITES: 0
total_POSIX_BYTES_READ: 0
total_POSIX_BYTES_WRITTEN: 68719476736
total_POSIX_MAX_BYTE_READ: 0
total_POSIX_MAX_BYTE_WRITTEN: 67108863
...
----

=== darshan-dxt-parser

The `darshan-dxt-parser` utility can be used to parse DXT traces out of Darshan
log files, assuming the corresponding application was executed with the DXT
modules enabled. The following example parses all DXT trace information out
of a Darshan log file and stores it in a text file:

----
darshan-dxt-parser shane_ior_id25016_1-31-38066-13864742673678115131_1.darshan > ~/ior-trace.txt
----

=== Guide to darshan-dxt-parser output

The preamble to `darshan-dxt-parser` output is identical to that of the traditional
`darshan-parser` utility, which is described above.

`darshan-dxt-parser` displays detailed trace information contained within a Darshan log
that was generated with DXT instrumentation enabled. Trace data is captured from both
POSIX and MPI-IO interfaces. Example output is given below:

.Example output
----
# ***************************************************
# DXT_POSIX module data
# ***************************************************

# DXT, file_id: 16457598720760448348, file_name: /tmp/test/testFile
# DXT, rank: 0, hostname: shane-thinkpad
# DXT, write_count: 4, read_count: 4
# DXT, mnt_pt: /, fs_type: ext4
# Module    Rank  Wt/Rd  Segment          Offset       Length    Start(s)      End(s)
 X_POSIX       0  write        0               0       262144      0.0029      0.0032
 X_POSIX       0  write        1          262144       262144      0.0032      0.0035
 X_POSIX       0  write        2          524288       262144      0.0035      0.0038
 X_POSIX       0  write        3          786432       262144      0.0038      0.0040
 X_POSIX       0   read        0               0       262144      0.0048      0.0048
 X_POSIX       0   read        1          262144       262144      0.0049      0.0049
 X_POSIX       0   read        2          524288       262144      0.0049      0.0050
 X_POSIX       0   read        3          786432       262144      0.0050      0.0051

# ***************************************************
# DXT_MPIIO module data
# ***************************************************

# DXT, file_id: 16457598720760448348, file_name: /tmp/test/testFile
# DXT, rank: 0, hostname: shane-thinkpad
# DXT, write_count: 4, read_count: 4
# DXT, mnt_pt: /, fs_type: ext4
# Module    Rank  Wt/Rd  Segment       Length    Start(s)      End(s)
 X_MPIIO       0  write        0       262144      0.0029      0.0032
 X_MPIIO       0  write        1       262144      0.0032      0.0035
 X_MPIIO       0  write        2       262144      0.0035      0.0038
 X_MPIIO       0  write        3       262144      0.0038      0.0040
 X_MPIIO       0   read        0       262144      0.0048      0.0049
 X_MPIIO       0   read        1       262144      0.0049      0.0049
 X_MPIIO       0   read        2       262144      0.0049      0.0050
 X_MPIIO       0   read        3       262144      0.0050      0.0051
----

==== DXT POSIX module

This module provides details on each read or write access at the POSIX layer.
The trace output is organized first by file then by process rank. So, for each
file accessed by the application, DXT will provide each process's I/O trace
segments in separate blocks, ordered by increasing process rank. Within each
file/rank block, I/O trace segments are ordered chronologically.

Before providing details on each I/O operation, DXT provides a short preamble
for each file/rank trace block with the following bits of information: the Darshan
identifier for the file (which is equivalent to the identifers used by Darshan in its
traditional modules), the full file path, the corresponding MPI rank the current
block of trace data belongs to, the hostname associated with this process rank, the
number of individual POSIX read and write operations by this process, and the mount
point and file system type corresponding to the traced file.

The output format for each indvidual I/O operation segment is:

----
# Module    Rank  Wt/Rd  Segment          Offset       Length    Start(s)      End(s)
----

* Module: corresponding DXT module (DXT_POSIX or DXT_MPIIO)
* Rank: process rank responsible for I/O operation
* Wt/Rd: whether the operation was a write or read
* Segment: The operation number for this segment (first operation is segment 0)
* Offset: file offset the I/O operation occured at
* Length: length of the I/O operation in bytes
* Start: timestamp of the start of the operation (w.r.t. application start time)
* End: timestamp of the end of the operation (w.r.t. application start time)

==== DXT MPI-IO module

If the MPI-IO interface is used by an application, this module provides details on
each read or write access at the MPI-IO layer. This data is often useful in
understanding how MPI-IO read or write operations map to underlying POSIX read
or write operations issued to the traced file.

The output format for the DXT MPI-IO module is essentially identical to the DXT
POSIX module, except that the offset of file operations is not tracked.

=== Other darshan-util utilities

The darshan-util package includes a number of other utilies that can be
summarized briefly as follows:

* darshan-convert: converts an existing log file to the newest log format.
If the `--bzip2` flag is given, then the output file will be re-compressed in
bzip2 format rather than libz format.  It also has command line options for
anonymizing personal data, adding metadata annotation to the log header, and
restricting the output to a specific instrumented file.
* darshan-diff: provides a text diff of two Darshan log files, comparing both
job-level metadata and module data records between the files.
* darshan-analyzer: walks an entire directory tree of Darshan log files and
produces a summary of the types of access methods used in those log files.
* darshan-logutils*: this is a library rather than an executable, but it
provides a C interface for opening and parsing Darshan log files.  This is
the recommended method for writing custom utilities, as darshan-logutils
provides a relatively stable interface across different versions of Darshan
and different log formats.
* dxt_analyzer: plots the read or write activity of a job using data obtained
from Darshan's DXT modules (if DXT is enabled).

=== PyDarshan

PyDarshan is a Python package that provides functionality for analyzing Darshan
log files, first introduced as part of Darshan 3.3.0. This package provides
easier to use Python interfaces to Darshan log file data (compared to the C-based
darshan-util library), enabling Darshan users to develop their own custom log file
analysis utilities.

PyDarshan has independent documentation outlining how to install and use this package
which can be found
link:https://www.mcs.anl.gov/research/projects/darshan/docs/pydarshan/index.html[HERE].
