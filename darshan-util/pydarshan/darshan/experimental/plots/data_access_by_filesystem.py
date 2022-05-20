"""
Draft utility code for the `data access by category` section
of Phil's hand drawing of future report layout.
"""

import os
import pathlib
from typing import List, Dict, Optional, Any, Callable, Sequence

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
    try:
        read_groups_posix = df_reads.groupby('filesystem_root')['POSIX_BYTES_READ'].sum()
    except KeyError:
        read_groups_posix = pd.Series(dtype=np.int64)
    try:
        read_groups_stdio = df_reads.groupby('filesystem_root')['STDIO_BYTES_READ'].sum()
    except KeyError:
        read_groups_stdio = pd.Series(dtype=np.int64)
    try:
        write_groups_posix = df_writes.groupby('filesystem_root')['POSIX_BYTES_WRITTEN'].sum()
    except KeyError:
        write_groups_posix = pd.Series(dtype=np.int64)
    try:
        write_groups_stdio = df_writes.groupby('filesystem_root')['STDIO_BYTES_WRITTEN'].sum()
    except KeyError:
        write_groups_stdio = pd.Series(dtype=np.int64)


    read_groups = read_groups_posix.add(read_groups_stdio, fill_value=0.0).astype(np.int64)
    write_groups = write_groups_posix.add(write_groups_stdio, fill_value=0.0).astype(np.int64)
    read_groups.name = "BYTES_READ"
    write_groups.name = "BYTES_WRITTEN"
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

    df_writes: a ``pd.DataFrame`` where each row presents
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

def check_empty_series(read_groups, write_groups, filesystem_roots: Sequence[str]):
    """
    Add ``0`` values for inactive filesystem roots
    for plotting purposes.

    Parameters
    ----------

    read_groups: a ``pd.Series`` object with IO read activity data

    write_groups: a ``pd.Series`` object with IO write activity data

    filesystem_roots: a sequence of strings containing unique filesystem root paths

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

def empty_series_handler(series, filesystem_roots: Sequence[str]):
    """
    Parameters
    ----------

    series: a ``pd.Series`` object

    filesystem_roots: a sequence of strings containing unique filesystem root paths

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
    rec_counters = pd.DataFrame()
    df_reads = pd.DataFrame()
    df_writes = pd.DataFrame()
    if "POSIX" in report.modules:
        rec_counters = pd.concat(objs=(rec_counters, report.records["POSIX"].to_df()['counters']))
        df_reads = pd.concat(objs=(df_reads, rec_counters.loc[rec_counters[f'POSIX_BYTES_READ'] >= 1]))
        df_writes = pd.concat(objs=(df_writes, rec_counters.loc[rec_counters[f'POSIX_BYTES_WRITTEN'] >= 1]))
    if "STDIO" in report.modules:
        rec_counters = pd.concat(objs=(rec_counters, report.records["STDIO"].to_df()['counters']))
        df_reads = pd.concat(objs=(df_reads, rec_counters.loc[rec_counters[f'STDIO_BYTES_READ'] >= 1]))
        df_writes = pd.concat(objs=(df_writes, rec_counters.loc[rec_counters[f'STDIO_BYTES_WRITTEN'] >= 1]))
    
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

    file_hashes, file_paths = convert_id_dict_to_arrays(file_id_dict=file_id_dict)
    # add column with filepaths for each event
    df_reads = df_reads.assign(filepath=df_reads['id'].map(lambda a: convert_file_id_to_path(a, file_hashes, file_paths)))
    df_writes = df_writes.assign(filepath=df_writes['id'].map(lambda a: convert_file_id_to_path(a, file_hashes, file_paths)))

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
    if filesystem_root.isdigit():
        # this is probably an anonymized STD..
        # stream, so make that clear
        filesystem_root = f'anonymized\n({filesystem_root})'
    if filesystem_root.startswith("//"):
        # Shane indicates that these are individual files
        # mounted on root
        # see:
        # https://github.com/darshan-hpc/darshan/pull/397#discussion_r769186581
        filesystem_root = "/"
    return filesystem_root


def convert_id_dict_to_arrays(file_id_dict: Dict[int, str]):
    """
    Splits the file hash/path dictionary into corresponding arrays, 
    one for the file hashes and another for the file paths.

    Parameters 
    ----------
    file_id_dict: a dictionary mapping integer file hash values
                to string values corresponding to their respective
                paths

    Returns
    -------
    A tuple of form ``(file_id_hash_arr, file_path_arr)`` where the first 
    element is an array containing the integer file hash values and the 
    second is an array containing the corresponding file paths.
    """
    file_id_hash_arr = np.array(list(file_id_dict.keys()))
    file_path_arr = np.array(list(file_id_dict.values()))
    return file_id_hash_arr, file_path_arr


def convert_file_id_to_path(input_id: float, file_hashes, file_paths) -> Optional[str]:
    """
    Parameters
    ----------

    input_id: a float representing the file hash

    file_id_dict: a dictionary mapping integer file hash values
                  to string values corresponding to their respective
                  paths

    Returns
    -------
    A string containing the file path corresponding to ``input_id``,
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
    file_idx = np.nonzero((file_hashes - input_id) == 0)[0]
    try:
        file_path = file_paths[file_idx][0]
    except IndexError:
        return None
    return file_path

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
    for file_id_hash, file_path in file_id_dict.items():
        filesystem_root = convert_file_path_to_root_path(file_path=file_path)
        if filesystem_root not in filesystem_roots:
            filesystem_roots.append(filesystem_root)
    if verbose:
        print("filesystem_roots:", filesystem_roots)
    return filesystem_roots

def unique_fs_rw_counter(report: Any,
                         filesystem_roots: Sequence[str],
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

    filesystem_roots: a sequence of strings containing unique filesystem root paths

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

    ValueError: when POSIX module data is absent from the report
    """

    if not mod in ['POSIX', 'STDIO']:
        raise NotImplementedError("Only the POSIX and STDIO modules are currently supported")
    if mod not in report.modules:
        raise ValueError(f"{mod} module data is required")

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


def plot_data(fig: Any,
              file_rd_series,
              file_wr_series,
              bytes_rd_series,
              bytes_wr_series,
              filesystem_roots: Sequence[str],
              num_cats: Optional[int] = None):
    """
    Produce the horizontal bar plots for the data
    access by category analysis.

    Parameters
    ----------

    fig: matplotlib ``figure`` object into which the
    bar plots will be placed

    file_rd_series: a ``pd.Series`` object with per-category
    counts of unique files from which at least 1 byte has been
    read

    file_wr_series: a ``pd.Series`` object with per-category
    counts of unique files to which at least 1 byte has been
    written

    bytes_rd_series: a ``pd.Series`` object with per-category
    counts of bytes read

    bytes_wr_series: a ``pd.Series`` object with per-category
    counts of bytes written

    filesystem_roots: a sequence of strings containing unique filesystem root paths

    num_cats: an integer representing the number of categories
    to plot; default ``None`` plots all categories
    """
    list_byte_axes: list = []
    list_count_axes: list = []
    # use log10 scale if range exceeds
    # two orders of magnitude in a column
    use_log = [False, False]
    for idx, series_pair in enumerate([[bytes_rd_series, bytes_wr_series, 1048576],
                                       [file_rd_series, file_wr_series, 1]]):
        maxval = max(series_pair[0].max(), series_pair[1].max())
        minval = max(min(series_pair[0].min(), series_pair[1].min()), 1)
        # adjust ratio to MiB when needed
        ratio = ((maxval / series_pair[2]) / (minval / series_pair[2]))
        if ratio > 100:
            use_log[idx] = True

    if num_cats is None:
        num_cats = len(filesystem_roots)

    for row, filesystem in enumerate(bytes_rd_series.index[:num_cats]):
        ax_filesystem_bytes = fig.add_subplot(len(filesystem_roots[:num_cats]),
                                              2,
                                              row * 2 + 1)
        ax_filesystem_counts = fig.add_subplot(len(filesystem_roots[:num_cats]),
                                              2,
                                              row * 2 + 2)
        if row > 0:
            ax_filesystem_bytes.get_shared_x_axes().join(ax_filesystem_bytes, list_byte_axes[row - 1])
            if use_log[0]:
                ax_filesystem_bytes.set_xscale('symlog')
            ax_filesystem_counts.get_shared_x_axes().join(ax_filesystem_counts, list_count_axes[row - 1])
            if use_log[1]:
                ax_filesystem_counts.set_xscale('symlog')

        list_byte_axes.append(ax_filesystem_bytes)
        list_count_axes.append(ax_filesystem_counts)

        # convert to MiB using 1048576 (ie: 2**20)
        bytes_read = bytes_rd_series[filesystem]/1048576
        bytes_written = bytes_wr_series[filesystem]/1048576
        files_written = file_wr_series[filesystem]
        files_read = file_rd_series[filesystem]

        # scale to fit longer filesystem
        # strings on the left side of the plots
        # NOTE: may need more sophisticated scaling
        # eventually
        if len(filesystem) <= 8 and not '<STD' in filesystem:
            fontsize = 18
        else:
            fontsize = 12

        # anonymized STD.. streams have associated integers
        # that are stored in the filesystem data field
        # but that are confusing to display, so strip them
        if filesystem.startswith('anonymized'):
            ax_filesystem_bytes.annotate('anonymized',
                                         (-0.3, 0.5),
                                         fontsize=fontsize,
                                         xycoords='axes fraction',
                                         va="center")
        else:
            ax_filesystem_bytes.annotate(filesystem,
                                         (-0.3, 0.5),
                                         fontsize=fontsize,
                                         xycoords='axes fraction',
                                         va="center")

        ax_filesystem_counts.barh(0, files_written, color='red', alpha=0.3)
        ax_filesystem_counts.barh(1, files_read, color='blue', alpha=0.3)

        ax_filesystem_bytes.text(0, 0.75, f' # bytes read ({bytes_read:.2E} MiB)',
                                 transform=ax_filesystem_bytes.transAxes,
                                 va="center")
        ax_filesystem_bytes.text(0, 0.25, f' # bytes written ({bytes_written:.2E} MiB)',
                                 transform=ax_filesystem_bytes.transAxes,
                                 va="center")

        if files_read == 0:
            ax_filesystem_counts.text(0, 0.75, ' 0 files read',
                                      transform=ax_filesystem_counts.transAxes,
                                      va="center")
        else:
            ax_filesystem_counts.text(0, 0.75, f' # files read ({files_read:.2E})',
                                      transform=ax_filesystem_counts.transAxes,
                                      va="center")

        if files_written == 0:
            ax_filesystem_counts.text(0, 0.25, ' 0 files written',
                                      transform=ax_filesystem_counts.transAxes,
                                      va="center")
        else:
            ax_filesystem_counts.text(0, 0.25, f' # files written ({files_written:.2E})',
                                      transform=ax_filesystem_counts.transAxes,
                                      va="center")

        ax_filesystem_bytes.barh(0, bytes_written, color='red', alpha=0.3)
        ax_filesystem_bytes.barh(1, bytes_read, color='blue', alpha=0.3)

        for axis in fig.axes:
            # hide the right side plot frame (spine) so that
            # the value labels don't overlap with the plot frame
            axis.spines['right'].set_visible(False)
            axis.set_xticklabels([])
            axis.set_yticklabels([])
            axis.xaxis.set_ticks_position('none')
            axis.yaxis.set_ticks_position('none')
            axis.minorticks_off()

    if use_log[0]:
        ax_filesystem_bytes.set_xlabel('symmetric log scaled')
    if use_log[1]:
        ax_filesystem_counts.set_xlabel('symmetric log scaled')


def plot_with_report(report: darshan.DarshanReport,
                     verbose: bool = False,
                     num_cats: Optional[int] = None):
    """
    Plot the data access by category given a darshan ``DarshanReport``
    object.

    Parameters
    ----------

    report: a ``darshan.DarshanReport`` object

    plot_filename: name of the plot file produced

    verbose: if ``True``, provide extra debug information

    num_cats: an integer representing the number of categories
    to plot; default ``None`` plots all categories

    Returns
    -------

    fig: matplotlib figure object
    """
    fig = plt.figure()
    file_id_dict = report.data["name_records"]
    allowed_file_id_dict = {}

    # only POSIX/STDIO entries allowed
    for module in ["POSIX", "STDIO"]:
        for key in ["counters", "fcounters"]:
            try:
                allowed_ids = report.records[module].to_df()[key]["id"]
            except KeyError:
                allowed_ids = []
            for ident in allowed_ids:
                allowed_file_id_dict[ident] = file_id_dict[ident]

    filesystem_roots = identify_filesystems(file_id_dict=allowed_file_id_dict,
                                            verbose=verbose)
    # NOTE: this is a bit ugly, STDIO and POSIX are both combined
    # automatically later in the control flow, so an API redesign
    # may be in order eventually
    default_mod = "POSIX"
    if "POSIX" not in report.modules:
        # STDIO can also be used for this figure/analysis
        default_mod = "STDIO"

    file_rd_series, file_wr_series = unique_fs_rw_counter(report=report,
                                                          filesystem_roots=filesystem_roots,
                                                          file_id_dict=allowed_file_id_dict,
                                                          processing_func=process_unique_files,
                                                          mod=default_mod,
                                                          verbose=verbose)
    bytes_rd_series, bytes_wr_series = unique_fs_rw_counter(report=report,
                                                            filesystem_roots=filesystem_roots,
                                                            file_id_dict=allowed_file_id_dict,
                                                            processing_func=process_byte_counts,
                                                            mod=default_mod, verbose=verbose)
    # reverse sort by total bytes IO per category
    sort_inds = (bytes_rd_series + bytes_wr_series).argsort()[::-1]
    if num_cats is None:
        height = len(file_rd_series)
    else:
        height = num_cats

    plot_data(fig,
              file_rd_series[sort_inds],
              file_wr_series[sort_inds],
              bytes_rd_series[sort_inds],
              bytes_wr_series[sort_inds],
              filesystem_roots,
              num_cats=num_cats)

    # at least this much height seems to
    # produce a decent aspect ratio
    if height < 16:
        height = 16

    fig.set_size_inches(12, height)
    plt.close(fig)
    return fig
