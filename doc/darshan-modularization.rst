********************************************************************
Modularized I/O characterization using Darshan 3.x
********************************************************************

Introduction
==============================================

Darshan is a lightweight toolkit for characterizing the I/O performance of
instrumented HPC applications.

Starting with version 3.0.0, the Darshan runtime environment and log file
format have been redesigned such that new "instrumentation modules" can be
added without breaking existing tools. Developers are given a framework to
implement arbitrary instrumentation modules, which are responsible for
gathering I/O data from a specific system component (which could be from an I/O
library, platform-specific data, etc.). Darshan can then manage these modules
at runtime and create a valid Darshan log regardless of how many or what types
of modules are used.

Overview of Darshan's modularized architecture
==============================================

The Darshan source tree is organized into two primary components:

* **darshan-runtime**: Darshan runtime framework necessary for instrumenting MPI
  applications and generating I/O characterization logs.

* **darshan-util**: Darshan utilities for analyzing the contents of a given
  Darshan I/O characterization log.

The following subsections provide detailed overviews of each of these
components to give a better understanding of the architecture of the
modularized version of Darshan.  In :ref:`Sec add instr`, we actually outline
the necessary steps for integrating new instrumentation modules into Darshan.

.. _Sec darshan-runtime:

Darshan-runtime
-------------------------------------

The primary responsibilities of the darshan-runtime component are:

* intercepting I/O functions of interest from a target application;

* extracting statistics, timing information, and other data characterizing the
  application's I/O workload;

* compressing I/O characterization data and corresponding metadata;

* logging the compressed I/O characterization to file for future evaluation

The first two responsibilities are the burden of module developers, while the
last two are handled automatically by Darshan.

In general, instrumentation modules are composed of:

* wrapper functions for intercepting I/O functions;

* internal functions for initializing and maintaining internal data structures
  and module-specific I/O characterization data;

* a set of functions for interfacing with the Darshan runtime environment

A block diagram illustrating the interaction of an example POSIX
instrumentation module and the Darshan runtime environment is given below in
Figure 1.

**Figure 1. Darshan runtime environment**

.. image:: darshan-dev-modular-runtime.png
   :align: center
   :width: 600
   :alt: A block diagram of Darshan runtime environment

As shown in Figure 1, the Darshan runtime environment is just a library
(libdarshan) which intercepts and instruments functions of interest made by an
application to existing system libraries. Two primary components of this
library are ``darshan-core`` and ``darshan-common``.  ``darshan-core`` is the
central component which manages the initialization/shutdown of Darshan,
coordinates with active instrumentation modules, and writes I/O
characterization logs to disk, among other things. ``darshan-core`` intercepts
``MPI_Init()`` to initialize key internal data structures and intercepts
``MPI_Finalize()`` to initiate Darshan's shutdown process. ``darshan-common``
simply provides module developers with functionality that is likely to be
reused across modules to minimize development and maintenance costs.
Instrumentation modules must utilize ``darshan-core`` to register themselves
and corresponding I/O records with Darshan so they can be added to the output
I/O characterization. While not shown in Figure 1, numerous modules can be
registered with Darshan at any given time and Darshan is capable of correlating
records between these modules.

In the next three subsections, we describe instrumentation modules, the
``darshan-core`` component, and the ``darshan-common`` component in more
detail.

Instrumentation modules
^^^^^^^^^^^^^^^^^^^^^^^^^^

The new modularized version of Darshan allows for the generation of I/O
characterizations composed from numerous instrumentation modules, where an
instrumentation module is simply a Darshan component responsible for capturing
I/O data from some arbitrary source. For example, distinct instrumentation
modules may be defined for different I/O interfaces or to gather
system-specific I/O parameters from a given computing system. Each
instrumentation module interfaces with the ``darshan-core`` component to
coordinate its initialization and shutdown and to provide output I/O
characterization data to be written to log.

In general, there are two different methods an instrumentation module can use
to initialize itself: static initialization at Darshan startup time or dynamic
initialization within intercepted function calls during application execution.
The initialization process should initialize module-specific data structures
and register the module with the ``darshan-core`` component so it is included
in the output I/O characterization.

The static initialization approach is useful for modules that do not have
function calls that can be intercepted and instead can just grab all I/O
characterization data at Darshan startup or shutdown time. A module can be
statically initialized at Darshan startup time by adding its initialization
routine to the ``mod_static_init_fns`` array at the top of the
``lib/darshan-core.c`` source file.

.. note::
   Modules may wish to add a corresponding configure option to disable the
   module from attempting to gather I/O data. The ability to disable a module
   using a configure option is especially necessary for system-specific modules
   which can not be built or used on other systems.

Most instrumentation modules can just bootstrap themselves within wrapper
functions during normal application execution. Each of Darshan's current I/O
library instrumentation modules (POSIX, MPI-IO, stdio, HDF5, PnetCDF) follow
this approach. Each wrapper function should just include logic to initialize
data structures and register with ``darshan-core`` if this initialization has
not already occurred. Darshan intercepts function calls of interest by
inserting these wrappers at compile time for statically linked executables
(e.g., using the linkers ``--wrap`` mechanism) and at runtime for dynamically
linked executables (using LD_PRELOAD).

.. note::
   Modules should not perform any I/O or communication within wrapper
   functions. Darshan records I/O data independently on each application
   process, then merges the data from all processes when the job is shutting
   down. This defers expensive I/O and communication operations to the shutdown
   process, minimizing Darshan's impact on application I/O performance.

When the instrumented application terminates and Darshan begins its shutdown
procedure, it requires a way to interface with any active modules that have
data to contribute to the output I/O characterization.  The following function
is implemented by each module to finalize (and perhaps reorganize) module
records before returning the record memory back to darshan-core to be
compressed and written to file.

.. code-block:: C

    typedef void (*darshan_module_shutdown)(
        MPI_Comm mod_comm,
        darshan_record_id *shared_recs,
        int shared_rec_count,
        void** mod_buf,
        int* mod_buf_sz
    );

This function can be used to run collective MPI operations on module data; for
instance, Darshan typically tries to reduce file records which are shared
across all application processes into a single data record (more details on the
shared file reduction mechanism are given in :ref:`Sec add instr`).  This
function also serves as a final opportunity for modules to cleanup and free any
allocated data structures, etc.

* ``mod_comm`` is the MPI communicator to use for collective communication

* ``shared_recs`` is a list of Darshan record identifiers that are shared across
  all application processes

* ``shared_rec_count`` is the size of the shared record list

* ``mod_buf`` is a pointer to the buffer address of the module's contiguous set
  of data records

* ``mod_buf_sz`` is a pointer to a variable storing the aggregate size of the
  module's records. On input, the pointed to value indicates the aggregate size
  of the module's registered records; on output, the value may be updated if,
  for instance, certain records are discarded

darshan-core
^^^^^^^^^^^^^^^^^^^^^^^^^^

Within darshan-runtime, the darshan-core component manages the initialization
and shutdown of the Darshan environment, provides an interface for modules to
register themselves and their data records with Darshan, and manages the
compressing and the writing of the resultant I/O characterization. As
illustrated in Figure 1, the darshan-core runtime environment intercepts
``MPI_Init`` and ``MPI_Finalize`` routines to initialize and shutdown the
Darshan runtime environment, respectively.

Each of the functions provided by ``darshan-core`` to interface with
instrumentation modules are described in detail below.

.. code-block:: C

    void darshan_core_register_module(
        darshan_module_id mod_id,
        darshan_module_shutdown mod_shutdown_func,
        int *mod_mem_limit,
        int *rank,
        int *sys_mem_alignment);

The ``darshan_core_register_module`` function registers Darshan instrumentation
modules with the ``darshan-core`` runtime environment. This function needs to
be called once for any module that will contribute data to Darshan's final I/O
characterization.

* ``mod_id`` is a unique identifier for the given module, which is defined in the
  Darshan log format header file (``darshan-log-format.h``).

* ``mod_shutdown_func`` is the function pointer to the module shutdown function
  described in the previous section.

* ``inout_mod_buf_size`` is an input/output argument that stores the amount of
  module memory being requested when calling the function and the amount of
  memory actually reserved by darshan-core when returning.

* ``rank`` is a pointer to an integer to store the calling process's application
  MPI rank in.  ``NULL`` may be passed in to ignore this value.

* ``sys_mem_alignment`` is a pointer to an integer which will store the system
  memory alignment value Darshan was configured with. ``NULL`` may be passed in
  to ignore this value.

.. code-block:: C

    void darshan_core_unregister_module(darshan_module_id mod_id);

The ``darshan_core_unregister_module`` function disassociates the given module
from the ``darshan-core`` runtime. Consequentially, Darshan does not interface
with the given module at shutdown time and will not log any I/O data from the
module. This function should only be used if a module registers itself with
darshan-core but later decides it does not want to contribute any I/O data.
Note that, in the current implementation, Darshan does not have the ability to
reclaim the record memory allocated to the calling module to assign to other
modules.

* ``mod_id`` is the unique identifier for the module being unregistered.

.. code-block:: C

    darshan_record_id darshan_core_gen_record_id(const char *name);

The ``darshan_core_gen_record_id`` function simply generates a unique record
identifier for a given record name. This function is generally called to
convert a name string to a unique record identifier that is needed to register
a data record with darshan-core. The generation of IDs is consistent, such that
modules which reference records with the same names will store these records
using the same unique IDs, simplifying the correlation of these records for
analysis.

* ``name`` is the name of the corresponding data record (often times this is just
  a file name).

.. code-block:: C

    void *darshan_core_register_record(
        darshan_record_id rec_id,
        const char *name,
        darshan_module_id mod_id,
        int rec_len,
        int *fs_info);

The ``darshan_core_register_record`` function registers a data record with the
darshan-core runtime, allocating memory for the record so that it is persisted
in the output log file.  This record could reference a POSIX file or perhaps an
object identifier for an object storage system, for instance. This function
should only be called once for each record being tracked by a module to avoid
duplicating record memory. This function returns the address which the record
should be stored at or ``NULL`` if there is insufficient memory for storing the
record.

* ``rec_id`` is a unique integer identifier for this record (generally generated
  using the ``darshan_core_gen_record_id`` function).

* ``name`` is the string name of the data record, which could be a file path,
  object ID, etc.  If given, darshan-core will associate the given name with
  the record identifier and store this mapping in the log file so it can be
  retrieved for analysis. ``NULL`` may be passed in to generate an anonymous
  (unnamed) record.

* ``mod_id`` is the identifier for the module attempting to register this record.

* ``rec_len`` is the length of the record.

* ``fs_info`` is a pointer to a structure of relevant info for the file system
  associated with the given record -- this structure is defined in the
  ``darshan.h`` header. Note that this functionality only works for record
  names that are absolute file paths, since we determine the file system by
  matching the file path to the list of mount points Darshan is aware of.
  ``NULL`` may be passed in to ignore this value.

.. code-block:: C

    double darshan_core_wtime(void);

The ``darshan_core_wtime`` function simply returns a floating point number of
seconds since Darshan was initialized. This functionality can be used to time
the duration of application I/O calls or to store timestamps of when functions
of interest were called.

.. code-block:: C

    double darshan_core_excluded_path(const char *path);

The ``darshan_core_excluded_path`` function checks to see if a given file path
is in Darshan's list of excluded file paths (i.e., paths that we don't
instrument I/O to/from, such as /etc, /dev, /usr, etc.).

* ``path`` is the absolute file path we are checking.

darshan-common
^^^^^^^^^^^^^^^^^^^^^^^^^^

``darshan-common`` is a utility component of darshan-runtime, providing module
developers with general functions that are likely to be reused across multiple
modules. These functions are distinct from darshan-core functions since they do
not require access to internal Darshan state.

.. code-block:: C

    char* darshan_clean_file_path(const char* path);

The ``darshan_clean_file_path`` function just cleans up the input path string,
converting relative paths to absolute paths and suppressing any potential noise
within the string.  The address of the new string is returned and should be
freed by the user.

* ``path_`` is the input path string to be cleaned up.

``darshan-common`` also currently includes functions for maintaining counters
that store common I/O values (such as common I/O access sizes or strides used
by an application), as well as functions for calculating the variance of a
given counter across all processes.  As more modules are contributed, it is
likely that more functionality can be refactored out of module implementations
and maintained in darshan-common, facilitating code reuse and simplifying
maintenance.

Darshan-util
-------------------------------------

The darshan-util component is composed of a helper library for accessing log
file data records (``libdarshan-util``) and a set of utilities that use this
library to analyze application I/O behavior. ``libdarhan-util`` includes a
generic interface (``darshan-logutils``) for retrieving specific components of
a given log file. Specifically, this interface allows utilities to retrieve a
log's header metadata, job details, record ID to name mapping, and any
module-specific data contained within the log.

``libdarshan-util`` additionally includes the definition of a generic module
interface (``darshan-mod-logutils``) that may be implemented by modules to
provide a consistent way for Darshan utilities to interact with module data
stored in log files. This interface is necessary since each module has records
of varying size and format, so module-specific code is needed to interact with
the records in a generic manner. This interface is used by the
``darshan-parser`` utility, for instance, to extract data records from all
modules contained in a log file and to print these records in a consistent
format that is amenable to further analysis by other tools.

darshan-logutils
^^^^^^^^^^^^^^^^^^^^^^^^^^

Here we define each function in the ``darshan-logutils`` interface, which can
be used to create new log utilities and to implement module-specific interfaces
into log files.

.. code-block:: C

    darshan_fd darshan_log_open(const char *name);

Opens Darshan log file stored at path ``name``. The log file must already exist
and is opened for reading only. As part of the open routine, the log file
header is read to set internal file descriptor data structures. Returns a
Darshan file descriptor on success or ``NULL`` on error.

.. code-block:: C

    darshan_fd darshan_log_create(const char *name, enum darshan_comp_type comp_type, int partial_flag);

Creates a new darshan log file for writing only at path ``name``. ``comp_type``
denotes the underlying compression type used on the log file (currently either
libz or bzip2) and ``partial_flag`` denotes whether the log is storing partial
data (that is, all possible application file records were not tracked by
darshan). Returns a Darshan file descriptor on success or ``NULL`` on error.

.. code-block:: C

    int darshan_log_get_job(darshan_fd fd, struct darshan_job *job);
    int darshan_log_put_job(darshan_fd fd, struct darshan_job *job);

Reads/writes ``job`` structure from/to the log file referenced by descriptor
``fd``. The ``darshan_job`` structure is defined in ``darshan-log-format.h``.
Returns ``0`` on success, ``-1`` on failure.

.. code-block:: C

    int darshan_log_get_exe(darshan_fd fd, char *buf);
    int darshan_log_put_exe(darshan_fd fd, char *buf);

Reads/writes the corresponding executable string (exe name and command line
arguments) from/to the Darshan log referenced by ``fd``. Returns ``0`` on
success, ``-1`` on failure.

.. code-block:: C

    int darshan_log_get_mounts(darshan_fd fd, char*** mnt_pts, char*** fs_types, int* count);
    int darshan_log_put_mounts(darshan_fd fd, char** mnt_pts, char** fs_types, int count);

Reads/writes mounted file system information for the Darshan log referenced by
``fd``. ``mnt_pnts`` points to an array of strings storing mount points,
``fs_types`` points to an array of strings storing file system types (e.g.,
ext4, nfs, etc.), and ``count`` points to an integer storing the total number
of mounted file systems recorded by Darshan. Returns ``0`` on success, ``-1``
on failure.

.. code-block:: C

    int darshan_log_get_namehash(darshan_fd fd, struct darshan_name_record_ref **hash);
    int darshan_log_put_namehash(darshan_fd fd, struct darshan_name_record_ref *hash);

Reads/writes the hash table of Darshan record identifiers to full names for all
records contained in the Darshan log referenced by ``fd``. ``hash`` is a
pointer to the hash table (of type ``struct darshan_name_record_ref *``), which
should be initialized to ``NULL`` for reading. This hash table is defined by
the ``uthash`` hash table implementation and includes corresponding macros for
searching, iterating, and deleting records from the hash. For detailed
documentation on using this hash table, consult ``uthash`` documentation in
``darshan-util/uthash-1.9.2/doc/txt/userguide.txt``.  The ``darshan-parser``
utility (for parsing module information out of a Darshan log) provides an
example of how this hash table may be used. Returns ``0`` on success, ``-1`` on
failure.

.. code-block:: C

    int darshan_log_get_mod(darshan_fd fd, darshan_module_id mod_id, void *mod_buf, int mod_buf_sz);
    int darshan_log_put_mod(darshan_fd fd, darshan_module_id mod_id, void *mod_buf, int mod_buf_sz, int ver);

Reads/writes a chunk of (uncompressed) module data for the module identified by
``mod_id`` from/to the Darshan log referenced by ``fd``. ``mod_buf`` is the
buffer to read data into or write data from, and ``mod_buf_sz`` is the
corresponding size of the buffer. The ``darshan_log_getmod`` routine can be
repeatedly called to retrieve chunks of uncompressed data from a specific
module region of the log file given by ``fd``. The ``darshan_log_putmod``
routine just continually appends data to a specific module region in the log
file given by ``fd`` and accepts an additional ``ver`` parameter indicating the
version number for the module data records being written. These functions
return the number of bytes read/written on success, ``-1`` on failure.

.. note::
   Darshan use a "reader makes right" conversion strategy to rectify Endianness
   issues between the machine a log was generated on and a machine analyzing
   the log. Accordingly, module-specific log utility functions will need to
   check the ``swap_flag`` variable of the Darshan file descriptor to determine
   if byte swapping is necessary. 32-bit and 64-bit byte swapping macros
   (DARSHAN_BSWAP32/DARSHAN_BSWAP64) are provided in ``darshan-logutils.h``.

.. code-block:: C

    void darshan_log_close(darshan_fd fd);

Close Darshan file descriptor ``fd``. This routine *must* be called for newly
created log files, as it flushes pending writes and writes a corresponding log
file header before closing.

.. note::
   For newly created Darshan log files, care must be taken to write log file
   data in the correct order, since the log file write routines basically are
   appending data to the log file.  The correct order for writing all log file
   data to file is: (1) job data, (2) exe string, (3) mount data, (4) record id
   -> file name map, (5) each module's data, in increasing order of module
   identifiers.

darshan-mod-logutils
^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``darshan-mod-logutils`` interface provides a convenient way to implement
new log functionality across all Darshan instrumentation modules, which can
potentially greatly simplify the development of new Darshan log utilities.
These functions are defined in the ``darshan_mod_logutil_funcs`` structure in
``darshan-logutils.h`` -- instrumentation modules simply provide their own
implementation of each function, then utilities can leverage this functionality
using the ``mod_logutils`` array defined in ``darshan-logutils.c``. A
description of some of the currently implemented functions are provided below.

.. code-block:: C

    int log_get_record(darshan_fd fd, void **buf);
    int log_put_record(darshan_fd fd, void *buf);

Reads/writes the module record stored in ``buf`` to the log referenced by
``fd``. Notice that a size parameter is not needed since the utilities calling
this interface will likely not know the record size -- the module-specific log
utility code can determine the corresponding size before reading/writing the
record from/to file.

.. note::
   ``log_get_record`` takes a pointer to a buffer address rather than just the
   buffer address.  If the pointed to address is equal to ``NULL``, then record
   memory should be allocated instead. This functionality helps optimize memory
   usage, since utilities often don't know the size of records being accessed
   but still must provide a buffer to read them into.

.. code-block:: C

    void log_print_record(void *rec, char *name, char *mnt_pt, char *fs_type);

Prints all data associated with the record pointed to by ``rec``. ``name``
holds the corresponding name string for this record. ``mnt_pt`` and ``fs_type``
hold the corresponding mount point path and file system type strings associated
with the record (only valid for records with names that are absolute file
paths).

.. code-block:: C

    void log_print_description(int ver);

Prints a description of the data stored within records for this module (with
version number ``ver``).

.. _Sec add instr:

Adding new instrumentation modules
==============================================

In this section we outline each step necessary for adding a module to Darshan.
To assist module developers, we have provided the example "NULL" module as part
of the Darshan source tree (``darshan-null-log-format.h``,
``darshan-runtime/lib/darshan-null.c``, and
``darshan-util/darshan-null-logutils.*``) This example can be used as a minimal
stubbed out module implementation that is heavily annotated to further clarify
how modules interact with Darshan and to provide best practices to future
module developers. For full-fledged module implementation examples, developers
are encouraged to examine the POSIX and MPI-IO modules.

Log format headers
-------------------------------------

The following modifications to Darshan log format headers are required for
defining the module's record structure:

* Add a module identifier to the ``DARSHAN_MODULE_IDS`` macro at the top of the
  ``darshan-log-format.h`` header. In this macro, the first field is a
  corresponding enum value that can be used to identify the module, the second
  field is a string name for the module, the third field is the current version
  number of the given module's log format, and the fourth field is a
  corresponding pointer to a Darshan log utility implementation for this module
  (which can be set to ``NULL`` until the module has its own log utility
  implementation).

* Add a top-level header that defines an I/O data record structure for the
  module. Consider the "NULL" module and POSIX module log format headers for
  examples (``darshan-null-log-format.h`` and ``darshan-posix-log-format.h``,
  respectively).

These log format headers are defined at the top level of the Darshan source
tree, since both the darshan-runtime and darshan-util repositories depend on
their definitions.

Darshan-runtime
-------------------------------------

Build modifications
^^^^^^^^^^^^^^^^^^^^^^^^^^

The following modifications to the darshan-runtime build system are necessary
to integrate new instrumentation modules:

* Necessary linker flags for inserting this module's wrapper functions need to
  be added to a module-specific file which is used when linking applications
  with Darshan.  For an example, consider
  ``darshan-runtime/share/ld-opts/darshan-posix-ld-opts``, the required linker
  options for the POSIX module. The base linker options file for Darshan
  (``darshan-runtime/share/ld-opts/darshan-base-ld-opts.in``) must also be
  updated to point to the new module-specific linker options file.

* Targets must be added to ``Makefile.in`` to build static and shared objects
  for the module's source files, which will be stored in the
  ``darshan-runtime/lib/`` directory.  The prerequisites to building static and
  dynamic versions of ``libdarshan`` must be updated to include these objects,
  as well.

  - If the module defines a linker options file, a rule must also be added to
    install this file with libdarshan.

Instrumentation module implementation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In addtion to the development notes from above and the exemplar "NULL" and
POSIX modules, we provide the following notes to assist module developers:

* Modules only need to include the ``darshan.h`` header to interface with
  darshan-core.

* The file record identifier given when registering a record with darshan-core
  should be used to store the record structure in a hash table or some other
  structure.

  - Subsequent calls that need to modify this record can then use the
    corresponding record identifier to lookup the record in this local hash
    table.
  - It may be necessary to maintain a separate hash table for other handles
    which the module may use to refer to a given record. For instance, the
    POSIX module may need to look up a file record based on a given file
    descriptor, rather than a path name.

Darshan-util
-------------------------------------

Build modifications
^^^^^^^^^^^^^^^^^^^^^^^^^^

The following modifications to the darshan-util build system are necessary to
integrate new instrumentation modules:

* Update ``Makefile.in`` with new targets necessary for building
  module-specific logutil source.

  - Make sure to add the module's logutil implementation objects as a
    prerequisite for building ``libdarshan-util``.
  - Make sure to update ``all``, ``clean``, and ``install`` rules to reference
    updates.

Module-specific logutils and utilities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For a straightforward reference implementation of module-specific log utility
functions, consider the implementations for the NULL module
(``darshan-util/darshan-null-logutils.*``) and the POSIX module
(``darshan-util/darshan-posix-logutils.*``). These module-specific log utility
implementations are built on top of the ``darshan_log_getmod()`` and
``darshan_log_putmod()`` functions, and are used to read/write complete module
records from/to file.

Also, consider the ``darshan-parser`` source code for an example of a utility
which can leverage ``libdarshan-util`` for analyzing the contents of a Darshan
I/O characterization log with data from arbitrary instrumentation modules.

.. _Sec shared record:

Shared record reductions
==============================================

Since Darshan prefers to aggregate data records which are shared across all
processes into a single data record, module developers should consider
implementing this functionality eventually, though it is not strictly required.

Module developers should implement the shared record reduction mechanism within
the module's ``darshan_module_shutdown()`` function, as it provides an MPI
communicator for the module to use for collective communication and a list of
record identifiers which are shared globally by the module (as described in
:ref:`Sec darshan-runtime`).

In general, implementing a shared record reduction involves the following
steps:

* reorganizing shared records into a contiguous region in the buffer of module
  records

* allocating a record buffer to store the reduction output on application rank
  0

* creating an MPI reduction operation using the ``MPI_Op_create()`` function
  (see more in `MPI_Op_create manpage
  <http://www.mpich.org/static/docs/v3.1/www3/MPI_Op_create.html>`_).

* reducing all shared records using the created MPI reduction operation and the
  send and receive buffers described above

For a more in-depth example of how to use the shared record reduction
mechanism, consider the implementations of this in the POSIX or MPI-IO modules.

Other resources
==============================================

* `Darshan GitLab page <https://xgitlab.cels.anl.gov/darshan/darshan>`_
* `Darshan project website <http://www.mcs.anl.gov/research/projects/darshan/>`_
* :ref:`TOC Darshan Runtime`
* :ref:`TOC Darshan Utilities`
