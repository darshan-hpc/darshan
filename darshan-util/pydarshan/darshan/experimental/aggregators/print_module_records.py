from darshan.report import *

def print_module_records(self, mode='append'):
    """
    Compile the I/O operations summary for the current report.

    Args:
        mode (str): Whether to 'append' (default) or to 'return' aggregation. 

    Return:
        None or dict: Depending on mode
    """

    
    descriptions = {
        'POSIX': """
# *******************************************************
# POSIX module data
# *******************************************************

# description of POSIX counters:
#   POSIX_*: posix operation counts.
#   READS,WRITES,OPENS,SEEKS,STATS,MMAPS,SYNCS,FILENOS,DUPS are types of operations.
#   POSIX_RENAME_SOURCES/TARGETS: total count file was source or target of a rename operation
#   POSIX_RENAMED_FROM: Darshan record ID of the first rename source, if file was a rename target
#   POSIX_MODE: mode that file was opened in.
#   POSIX_BYTES_*: total bytes read and written.
#   POSIX_MAX_BYTE_*: highest offset byte read and written.
#   POSIX_CONSEC_*: number of exactly adjacent reads and writes.
#   POSIX_SEQ_*: number of reads and writes from increasing offsets.
#   POSIX_RW_SWITCHES: number of times access alternated between read and write.
#   POSIX_*_ALIGNMENT: memory and file alignment.
#   POSIX_*_NOT_ALIGNED: number of reads and writes that were not aligned.
#   POSIX_MAX_*_TIME_SIZE: size of the slowest read and write operations.
#   POSIX_SIZE_*_*: histogram of read and write access sizes.
#   POSIX_STRIDE*_STRIDE: the four most common strides detected.
#   POSIX_STRIDE*_COUNT: count of the four most common strides.
#   POSIX_ACCESS*_ACCESS: the four most common access sizes.
#   POSIX_ACCESS*_COUNT: count of the four most common access sizes.
#   POSIX_*_RANK: rank of the processes that were the fastest and slowest at I/O (for shared files).
#   POSIX_*_RANK_BYTES: bytes transferred by the fastest and slowest ranks (for shared files).
#   POSIX_F_*_START_TIMESTAMP: timestamp of first open/read/write/close.
#   POSIX_F_*_END_TIMESTAMP: timestamp of last open/read/write/close.
#   POSIX_F_READ/WRITE/META_TIME: cumulative time spent in read, write, or metadata operations.
#   POSIX_F_MAX_*_TIME: duration of the slowest read and write operations.
#   POSIX_F_*_RANK_TIME: fastest and slowest I/O time for a single rank (for shared files).
#   POSIX_F_VARIANCE_RANK_*: variance of total I/O time and bytes moved for all ranks (for shared files).

# WARNING: POSIX module log format version <=3 has the following limitations:
# - No support for the following counters to properly instrument dup, fileno, and rename operations:
# 	- POSIX_FILENOS
# 	- POSIX_DUPS
# 	- POSIX_RENAME_SOURCES
# 	- POSIX_RENAME_TARGETS
# 	- POSIX_RENAMED_FROM

#<module>	<rank>	<record id>	<counter>	<value>	<file name>	<mount pt>	<fs type>
        """,

        "MPI-IO": """
# *******************************************************
# MPI-IO module data
# *******************************************************

# description of MPIIO counters:
#   MPIIO_INDEP_*: MPI independent operation counts.
#   MPIIO_COLL_*: MPI collective operation counts.
#   MPIIO_SPLIT_*: MPI split collective operation counts.
#   MPIIO_NB_*: MPI non blocking operation counts.
#   READS,WRITES,and OPENS are types of operations.
#   MPIIO_SYNCS: MPI file sync operation counts.
#   MPIIO_HINTS: number of times MPI hints were used.
#   MPIIO_VIEWS: number of times MPI file views were used.
#   MPIIO_MODE: MPI-IO access mode that file was opened with.
#   MPIIO_BYTES_*: total bytes read and written at MPI-IO layer.
#   MPIIO_RW_SWITCHES: number of times access alternated between read and write.
#   MPIIO_MAX_*_TIME_SIZE: size of the slowest read and write operations.
#   MPIIO_SIZE_*_AGG_*: histogram of MPI datatype total sizes for read and write operations.
#   MPIIO_ACCESS*_ACCESS: the four most common total access sizes.
#   MPIIO_ACCESS*_COUNT: count of the four most common total access sizes.
#   MPIIO_*_RANK: rank of the processes that were the fastest and slowest at I/O (for shared files).
#   MPIIO_*_RANK_BYTES: total bytes transferred at MPI-IO layer by the fastest and slowest ranks (for shared files).
#   MPIIO_F_*_START_TIMESTAMP: timestamp of first MPI-IO open/read/write/close.
#   MPIIO_F_*_END_TIMESTAMP: timestamp of last MPI-IO open/read/write/close.
#   MPIIO_F_READ/WRITE/META_TIME: cumulative time spent in MPI-IO read, write, or metadata operations.
#   MPIIO_F_MAX_*_TIME: duration of the slowest MPI-IO read and write operations.
#   MPIIO_F_*_RANK_TIME: fastest and slowest I/O time for a single rank (for shared files).
#   MPIIO_F_VARIANCE_RANK_*: variance of total I/O time and bytes moved for all ranks (for shared files).

# WARNING: MPIIO module log format version <=2 does not support the following counters:
# - MPIIO_F_CLOSE_START_TIMESTAMP
# - MPIIO_F_OPEN_END_TIMESTAMP

#<module>	<rank>	<record id>	<counter>	<value>	<file name>	<mount pt>	<fs type>
        """,

        "LUSTRE": """
# *******************************************************
# LUSTRE module data
# *******************************************************

# description of LUSTRE counters:
#   LUSTRE_OSTS: number of OSTs across the entire file system.
#   LUSTRE_MDTS: number of MDTs across the entire file system.
#   LUSTRE_STRIPE_OFFSET: OST ID offset specified when the file was created.
#   LUSTRE_STRIPE_SIZE: stripe size for file in bytes.
#   LUSTRE_STRIPE_WIDTH: number of OSTs over which the file is striped.
#   LUSTRE_OST_ID_*: indices of OSTs over which the file is striped.

#<module>	<rank>	<record id>	<counter>	<value>	<file name>	<mount pt>	<fs type>
        """,

        "STDIO": """
# *******************************************************
# STDIO module data
# *******************************************************

# description of STDIO counters:
#   STDIO_{OPENS|FDOPENS|WRITES|READS|SEEKS|FLUSHES} are types of operations.
#   STDIO_BYTES_*: total bytes read and written.
#   STDIO_MAX_BYTE_*: highest offset byte read and written.
#   STDIO_*_RANK: rank of the processes that were the fastest and slowest at I/O (for shared files).
#   STDIO_*_RANK_BYTES: bytes transferred by the fastest and slowest ranks (for shared files).
#   STDIO_F_*_START_TIMESTAMP: timestamp of the first call to that type of function.
#   STDIO_F_*_END_TIMESTAMP: timestamp of the completion of the last call to that type of function.
#   STDIO_F_*_TIME: cumulative time spent in different types of functions.
#   STDIO_F_*_RANK_TIME: fastest and slowest I/O time for a single rank (for shared files).
#   STDIO_F_VARIANCE_RANK_*: variance of total I/O time and bytes moved for all ranks (for shared files).

# WARNING: STDIO module log format version 1 has the following limitations:
# - No support for properly instrumenting fdopen operations (STDIO_FDOPENS)

#<module>	<rank>	<record id>	<counter>	<value>	<file name>	<mount pt>	<fs type>
        """,
     

    }



    pass
