"""
Draft utility code for the `data access by category` section
of Phil's hand drawing of future report layout.
"""

import pathlib
from typing import List, Dict, Optional, Any, Callable

import numpy as np
import pandas as pd
import darshan


def process_byte_counts(df_reads, df_writes):
    """
    Given separate dataframes for read/write IO activity,
    returns separate pandas Series objects with the counts
    of bytes read or written per filesystem root.

    Paramaters
    ----------

    df_reads: a ``pd.DataFrame`` where each row presents
    at least 1 byte of read-related IO activity, and where
    additional columns for ``filesystem_root`` and 
    ``filepath`` are present

    df_reads: a ``pd.DataFrame`` where each row presents
    at least 1 byte of write-related IO activity, and where
    additional columns for ``filesystem_root`` and 
    ``filepath`` are present

    Returns
    -------

    Tuple of form ``(read_groups, write_groups)``
    where each tuple element is a ``pd.Series`` object
    of per-filesystem read/write byte counts.
    """
    # TODO: generalize to other instrumentation
    # modules if we keep this design
    read_groups = df_reads.groupby('filesystem_root')['POSIX_BYTES_READ'].sum()
    write_groups = df_writes.groupby('filesystem_root')['POSIX_BYTES_WRITTEN'].sum()
    return read_groups, write_groups


def process_unique_files(df_reads, df_writes):
    """
    Given separate dataframes for read/write IO activity,
    returns separate pandas Series objects with the counts
    of unique files per filesytem root to which 1 byte has
    been read or written.

    Paramaters
    ----------

    df_reads: a ``pd.DataFrame`` where each row presents
    at least 1 byte of read-related IO activity, and where
    additional columns for ``filesystem_root`` and 
    ``filepath`` are present

    df_reads: a ``pd.DataFrame`` where each row presents
    at least 1 byte of write-related IO activity, and where
    additional columns for ``filesystem_root`` and 
    ``filepath`` are present

    Returns
    -------

    Tuple of form ``(read_groups, write_groups)``
    where each tuple element is a ``pd.Series`` object
    of per-filesystem root unique file read/write counts.
    """
    # groupby filesystem root and filepath, then get sizes
    # these are Pandas series objects where the index
    # is the name of the filesystem_root and the value
    # is the number of unique files counted per above criteria
    read_groups = df_reads.groupby('filesystem_root')['filepath'].nunique()
    write_groups = df_writes.groupby('filesystem_root')['filepath'].nunique()
    return read_groups, write_groups

def check_empty_series(read_groups, write_groups, filesystem_roots: List[str]):
    """
    Add ``0`` values for inactive filesystem roots
    for plotting purposes.

    Paramaters
    ----------

    read_groups: a ``pd.Series`` object with IO read activity data

    write_groups: a ``pd.Series`` object with IO write activity data

    filesystem_roots: a list of strings containing unique filesystem root paths

    Returns
    -------
    A tuple of form: `(read_groups, write_groups)` where
    each tuple element is a `pd.Series` object adjusted to
    contain "0" values for inactive filesystem roots (for
    plotting purposes).
    """
    read_groups = empty_series_handler(series=read_groups,
                                       filesystem_roots=filesystem_roots)
    write_groups = empty_series_handler(series=write_groups,
                                       filesystem_roots=filesystem_roots)
    return read_groups, write_groups

def empty_series_handler(series, filesystem_roots: List[str]):
    """
    Paramaters
    ----------

    series: a ``pd.Series`` object

    filesystem_roots: a list of strings containing unique filesystem root paths

    Returns
    -------
    A new ``Series`` with any missing filesystem_root indices
    filled in along with count values of 0 (for plotting purposes).
    """
    # first, guarantee that the index has all
    # of the filesystem_roots
    new_series = pd.Series(np.zeros(len(filesystem_roots)),
                           index=filesystem_roots)
    # for any filesystem roots that were already present,
    # preserve their values by adding them back in; otherwise,
    # counts should be set to 0 for plotting purposes
    series = (new_series + series).fillna(0)
    # preserve the index order to respect filesystem_roots
    series = series[filesystem_roots]
    return series


def rec_to_rw_counter_dfs_with_cols(report: Any,
                                    file_id_dict: Dict[int, str],
                                    mod: str = 'POSIX'):
    """
    Filter a DarshanReport into two "counters" dataframes,
    each with read/write activity in each row (at least 1
    byte read or written per row) and two extra columns
    for filesystem root path and filename data.

    Paramaters
    ----------

    report: a darshan.DarshanReport()

    file_id_dict: a dictionary mapping integer file hash values
                  to string values corresponding to their respective
                  paths

    mod: a string indicating the darshan module to use for parsing
         (default: ``POSIX``)


    Returns:
    --------
    tuple of form: (df_reads, df_writes)
    """
    # filter the DarshanReport into two "counters" dataframes
    # with read/write activity in each row (at least 1
    # byte read or written per row)
    df_reads, df_writes = rec_to_rw_counter_dfs(report=report,
                                                mod=mod)

    # add columns with filepaths and filesystem root
    # paths for each event
    df_reads, df_writes = add_filesystem_cols(df_reads=df_reads,
                                              df_writes=df_writes,
                                              file_id_dict=file_id_dict)
    return df_reads, df_writes

def rec_to_rw_counter_dfs(report: Any,
                          mod: str = 'POSIX'):
    """
    Filter a DarshanReport into two "counters" dataframes,
    each with read/write activity in each row (at least 1
    byte read or written per row).

    Paramaters
    ----------

    report: a darshan.DarshanReport()

    mod: a string indicating the darshan module to use for parsing
         (default: ``POSIX``)

    Returns:
    --------
    tuple of form: (df_reads, df_writes)
    """
    report.mod_read_all_records(mod, dtype='pandas')
    rec_counters = report.records[mod][0]['counters']
    
    # first, filter to produce a dataframe where {mod}_BYTES_READ >= 1
    # for each row (tracked event for a given rank or group of ranks)
    df_reads = rec_counters.loc[rec_counters[f'{mod}_BYTES_READ'] >= 1]

    # similar filter for writing
    df_writes = rec_counters.loc[rec_counters[f'{mod}_BYTES_WRITTEN'] >= 1]
    return df_reads, df_writes


def add_filesystem_cols(df_reads, df_writes, file_id_dict: Dict[int, str]):
    """
    Adds two columns to the input dataframes, one with
    the filepaths for each event, and the other with the
    filesystem root for each event.

    Paramaters
    ----------

    df_reads: ``pandas.DataFrame`` that has been filtered
              to only include rows with ``>= 1`` bytes
              read

    df_writes: ``pandas.DataFrame`` that has been filtered
               to only include rows with ``>= 1`` bytes
               written

    file_id_dict: a dictionary mapping integer file hash values
                  to string values corresponding to their respective
                  paths

    Returns
    -------
    A tuple of form ``(df_reads, df_writes)`` with
    the modified dataframes.
    """

    # add column with filepaths for each event
    df_reads = df_reads.assign(filepath=df_reads['id'].map(lambda a: convert_file_id_to_path(a, file_id_dict)))
    df_writes = df_writes.assign(filepath=df_writes['id'].map(lambda a: convert_file_id_to_path(a, file_id_dict)))

    # add column with filesystem root paths for each event
    df_reads = df_reads.assign(filesystem_root=df_reads['filepath'].map(lambda path: convert_file_path_to_root_path(path)))
    df_writes = df_writes.assign(filesystem_root=df_writes['filepath'].map(lambda path: convert_file_path_to_root_path(path)))
    return df_reads, df_writes


def convert_file_path_to_root_path(file_path: str) -> str:
    """
    Paramaters
    ----------

    file_path: a string containing the absolute file path

    Returns: 
    --------
    A string containing the root path.

    Examples:
    ---------

    >>> filesystem_root = convert_file_path_to_root_path("/scratch1/scratchdirs/glock/testFile.00000046")
    >>> filesystem_root
    '/scratch1'

    """
    path_parts = pathlib.Path(file_path).parts
    filesystem_root = ''.join(path_parts[:2])
    return filesystem_root

def convert_file_id_to_path(input_id: float, file_id_dict: Dict[int, str]) -> Optional[str]:
    """
    Paramaters
    ----------

    input_id: a float representing the file hash

    file_id_dict: a dictionary mapping integer file hash values
                  to string values corresponding to their respective
                  paths

    Returns
    -------
    A string containing the file path path corresponding to ``input_id``,
    or ``None`` if no matching file path was found for the input hash.

    Examples
    --------

    >>> # file_id_dict typically comes from `report.data["name_records"]`
    >>> file_id_dict = {210703578647777632: '/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/test.out.locktest.0',
    ...                 9457796068806373448: '/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/test.out'}
    >>> # input_id may came from i.e., a pandas dataframe of the record data
    >>> result = convert_file_id_to_path(input_id=9.457796068806373e+18, file_id_dict=file_id_dict)
    >>> result
    '/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/test.out'

    """
    for file_id_hash, file_path in file_id_dict.items():
        if np.allclose(input_id, file_id_hash):
            return file_path
    return None

def identify_filesystems(file_id_dict: Dict[int, str], verbose: bool = False) -> List[str]:
    """
    Paramaters
    ----------
    file_id_dict: a dictionary mapping integer file hash values
                  to string values corresponding to their respective
                  paths

    verbose: if ``True``, will print the filesystem root paths that
             are identified

    Returns
    -------
    A list of strings containing the unique filesystem root paths
    parsed from the input dictionary.

    Examples
    --------

    >>> # file_id_dict is typically from report.data["name_records"]
    >>> file_id_dict = {210703578647777632: '/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/test.out.locktest.0',
    ...                 14388265063268455899: '/tmp/ompi.sn176.28751/jf.29186/1/test.out_cid-0-3400.sm'}

    >>> filesystem_roots = identify_filesystems(file_id_dict=file_id_dict, verbose=True)
    filesystem_roots: ['/yellow', '/tmp']
    """
    filesystem_roots = []
    excluded = ['<STDIN>', '<STDOUT>', '<STDERR>']
    for file_id_hash, file_path in file_id_dict.items():
        filesystem_root = convert_file_path_to_root_path(file_path=file_path)
        if filesystem_root not in filesystem_roots:
            if filesystem_root not in excluded:
                filesystem_roots.append(filesystem_root)
    if verbose:
        print("filesystem_roots:", filesystem_roots)
    return filesystem_roots

def unique_fs_rw_counter(report: Any,
                         filesystem_roots: List[str],
                         file_id_dict: Dict[int, str],
                         processing_func: Callable,
                         mod: str = 'POSIX',
                         verbose: bool = False):
    """
    For each filesystem root path, apply the custom
    analysis specified by ``processing_func``.

    The data supplied to ``processing_func`` will be
    filtered to only include rows with activity where
    at least 1 byte has been read, or to which at least
    1 byte has been written.

    Paramaters:
    -----------
    report: a darshan.DarshanReport()

    filesystem_roots: a list of strings containing unique filesystem root paths

    file_id_dict: a dictionary mapping integer file hash values
                  to string values corresponding to their respective
                  paths

    processing_func: the function that will be used to process the
                     read and write dataframes; it should accept
                     ``df_reads`` and ``df_writes`` dataframes; see
                     the example process_unique_files() for details

    mod: a string indicating the darshan module to use for parsing
         (default: ``POSIX``)

    verbose: if ``True``, print the calculated values of ``read_groups``
    and ``write_groups``

    Returns:
    --------
    tuple of form: (read_groups, write_groups)

    Where each element of the tuple is a pandas
    ``Series`` object with a format like the one shown below
    when ``process_unique_files()`` is the ``processing_func``

    # filesystem_root count
    # /tmp       1
    # /yellow    1

    The ``int64`` values in the ``Series`` are the counts
    of unique files to which a single byte has been read
    (or written) on a given filesystem (index).

    Raises:
    -------
    NotImplementedError: for unsupported modules
    """

    if not mod == 'POSIX':
        raise NotImplementedError("Only the POSIX module is currently supported")

    # filter the DarshanReport into two "counters" dataframes
    # with read/write activity in each row (at least 1
    # byte read or written per row) and extra columns for
    # filesystem root and filename data
    df_reads, df_writes = rec_to_rw_counter_dfs_with_cols(report=report,
                                                          file_id_dict=file_id_dict,
                                                          mod=mod)
    read_groups, write_groups = processing_func(df_reads=df_reads,
                                                df_writes=df_writes)
    # if either of the Series are effectively empty we want
    # to produce a new Series with the filesystem_root indices
    # and count values of 0 (for plotting purposes)
    read_groups, write_groups = check_empty_series(read_groups=read_groups,
                                                   write_groups=write_groups,
                                                   filesystem_roots=filesystem_roots)
    if verbose:
        print("read_groups:\n", read_groups)
        print("write_groups:\n", write_groups)
    return (read_groups, write_groups)

