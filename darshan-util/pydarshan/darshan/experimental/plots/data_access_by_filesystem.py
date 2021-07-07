"""
Draft utility code for the `data access by category` section
of Phil's hand drawing of future report layout.
"""

import os
import pathlib
from typing import List, Dict, Optional, Any, Callable

import numpy as np
import pandas as pd
import darshan
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def process_byte_counts(df_reads, df_writes):
    """
    Given separate dataframes for read/write IO activity,
    returns separate pandas Series objects with the counts
    of bytes read or written per filesystem root.

    Parameters
    ----------

    df_reads: a ``pd.DataFrame`` where each row presents
    at least 1 byte of read-related IO activity, and where
    additional columns for ``filesystem_root`` and 
    ``filepath`` are present

    df_writes: a ``pd.DataFrame`` where each row presents
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

    Parameters
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

    Parameters
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
    Parameters
    ----------

    series: a ``pd.Series`` object

    filesystem_roots: a list of strings containing unique filesystem root paths

    Returns
    -------
    A new ``Series`` with any missing filesystem_root indices
    filled in along with count values of 0 (for plotting purposes).
    """
    return series.reindex(filesystem_roots, fill_value=0).astype(np.float64)


def rec_to_rw_counter_dfs_with_cols(report: Any,
                                    file_id_dict: Dict[int, str],
                                    mod: str = 'POSIX'):
    """
    Filter a DarshanReport into two "counters" dataframes,
    each with read/write activity in each row (at least 1
    byte read or written per row) and two extra columns
    for filesystem root path and filename data.

    Parameters
    ----------

    report: a darshan.DarshanReport()

    file_id_dict: a dictionary mapping integer file hash values
                  to string values corresponding to their respective
                  paths

    mod: a string indicating the darshan module to use for parsing
         (default: ``POSIX``)


    Returns
    -------
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

    Parameters
    ----------

    report: a darshan.DarshanReport()

    mod: a string indicating the darshan module to use for parsing
         (default: ``POSIX``)

    Returns
    -------
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

    Parameters
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
    Parameters
    ----------

    file_path: a string containing the absolute file path

    Returns
    -------
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
    Parameters
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
    Parameters
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

    Parameters
    ----------
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

    Returns
    -------
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
    if 'POSIX' not in report.modules:
        raise ValueError("POSIX module data is required")

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


def plot_data(fig, file_rd_series, file_wr_series, bytes_rd_series, bytes_wr_series, filesystem_roots):
    print("filesystem_roots:", filesystem_roots)
    for row, filesystem in enumerate(filesystem_roots):
        ax_filesystem_bytes = fig.add_subplot(len(filesystem_roots), 
                                              2,
                                              row * 2 + 1)
        ax_filesystem_counts = fig.add_subplot(len(filesystem_roots), 
                                              2,
                                              row * 2 + 2)
        # hide the right side plot frame (spine) so that
        # the value labels don't overlap with the plot frame
        for axis in [ax_filesystem_bytes, ax_filesystem_counts]:
            axis.spines['right'].set_visible(False)

        print("filesystem:", filesystem)
        print("bytes_rd_series:", bytes_rd_series)
        print("bytes_wr_series:", bytes_wr_series)
        # convert to MiB using the factor suggested
        # by Google (approximate result only for now)
        bytes_read = bytes_rd_series[filesystem]/1.049e+6
        bytes_written = bytes_wr_series[filesystem]/1.049e+6

        # scale to fit longer filesystem
        # strings on the left side of the plots
        # NOTE: may need more sophisticated scaling
        # eventually
        if len(filesystem) <= 8:
            fontsize = 18
        else:
            fontsize = 12

        ax_filesystem_bytes.annotate(filesystem, (-0.3, 0.5), fontsize=fontsize, xycoords='axes fraction')
        ax_filesystem_counts.barh(0, file_wr_series[filesystem], color='red', alpha=0.3)
        ax_filesystem_counts.barh(1, file_rd_series[filesystem], color='blue', alpha=0.3)

        ax_filesystem_bytes.text(0, 0.75, f' # bytes read ({bytes_read:.2E} MiB)', transform=ax_filesystem_bytes.transAxes)
        ax_filesystem_bytes.text(0, 0.25, f' # bytes written ({bytes_written:.2E} MiB)', transform=ax_filesystem_bytes.transAxes)

        if file_rd_series[filesystem] == 0:
            ax_filesystem_counts.text(0, 0.75, ' 0 files read', transform=ax_filesystem_counts.transAxes)
        else:
            ax_filesystem_counts.text(0, 1, '# files read')
            ax_filesystem_counts.text(file_rd_series[filesystem], 1, '  ' + str(file_rd_series[filesystem]))

        if file_wr_series[filesystem] == 0:
            ax_filesystem_counts.text(0, 0.25, ' 0 files written', transform=ax_filesystem_counts.transAxes)
        else:
            ax_filesystem_counts.text(0, 0, '# files written')
            ax_filesystem_counts.text(file_wr_series[filesystem], 0, str(file_wr_series[filesystem]))

        if bytes_written != 0:
            ax_filesystem_bytes.barh(0, bytes_written, color='red', alpha=0.3)
        else:
            ax_filesystem_bytes.barh(0, bytes_written, color='red', alpha=0.0)

        if bytes_read != 0:
            ax_filesystem_bytes.barh(1, bytes_read, color='blue', alpha=0.3)
        else:
            ax_filesystem_bytes.barh(1, bytes_read, color='blue', alpha=0.0)

        ax_filesystem_counts.set_xticks([])
        ax_filesystem_counts.set_yticks([])
        ax_filesystem_bytes.set_xticks([])
        ax_filesystem_bytes.set_yticks([])


def plot_with_log_file(log_file_path: str, plot_filename: str):
    """
    Plot the data access by category given a darshan log
    file path.

    Parameters
    ----------

    log_file_path: path to the darshan log file

    plot_filename: name of the plot file produced

    Returns
    -------

    fig: matplotlib figure object
    """
    fig = plt.figure()
    log_file = os.path.basename(log_file_path)
    fig.suptitle(f"Data Access by Category for log file: '{log_file}'")
    report = darshan.DarshanReport(log_file_path, read_all=True)
    file_id_dict = report.data["name_records"]
    filesystem_roots = identify_filesystems(file_id_dict=file_id_dict,
                                            verbose=True)
    file_rd_series, file_wr_series = unique_fs_rw_counter(report=report,
                                                          filesystem_roots=filesystem_roots,
                                                          file_id_dict=file_id_dict,
                                                          processing_func=process_unique_files,
                                                          mod='POSIX',
                                                          verbose=True)
    bytes_rd_series, bytes_wr_series = unique_fs_rw_counter(report=report,
                                                            filesystem_roots=filesystem_roots,
                                                            file_id_dict=file_id_dict,
                                                            processing_func=process_byte_counts,
                                                            mod='POSIX', verbose=True)
    plot_data(fig,
              file_rd_series,
              file_wr_series,
              bytes_rd_series,
              bytes_wr_series,
              filesystem_roots)

    fig.set_size_inches(12, 4)
    figname = f'{plot_filename}_data_access_by_category.png'
    fig.savefig(figname, dpi=300)
    return fig
