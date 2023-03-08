import darshan
from darshan.experimental.plots import plot_common_access_table

darshan.enable_experimental()

import numpy as np
import pandas as pd
import humanize


def log_get_bytes_bandwidth(derived_metrics, mod_name: str) -> str:
    """
    Summarize I/O performance for a given darshan module.

    Parameters
    ----------
    derived_metrics:
        structure (cdata object) describing metrics derived from a
        set of records passed to the Darshan accumulator interface
    mod_name: str
        Name of the darshan module to summarize the I/O
        performance for.

    Returns
    -------
    out: str
        A short string summarizing the performance of the given module
        in the provided log file, including bandwidth and total data
        transferred.

    Raises
    ------
    RuntimeError
        When a provided module name is not supported for the accumulator
        interface for provision of the summary data, or for any other
        error that occurs in the C/CFFI interface.
    ValueError
        When a provided module name does not exist in the log file.

    Examples
    --------

    >>> from darshan.log_utils import get_log_path
    >>> from darshan.lib.accum import log_get_bytes_bandwidth

    >>> log_path = get_log_path("imbalanced-io.darshan")
    >>> log_get_bytes_bandwidth(log_path, "POSIX")
    I/O performance estimate (at the POSIX layer): transferred 101785.8 MiB at 164.99 MiB/s

    >>> log_get_bytes_bandwidth(log_path, "MPI-IO")
    I/O performance estimate (at the MPI-IO layer): transferred 126326.8 MiB at 101.58 MiB/s
    """
    # get total bytes (in MiB) and bandwidth (in MiB/s) for
    # a given module -- this information was commonly reported
    # in the old perl-based summary reports
    total_mib = derived_metrics.total_bytes / 2 ** 20
    total_bw = derived_metrics.agg_perf_by_slowest
    ret_str = f"I/O performance estimate (at the {mod_name} layer): transferred {total_mib:.1f} MiB at {total_bw:.2f} MiB/s"
    return ret_str


def log_file_count_summary_table(derived_metrics,
                                 mod_name: str):
    # the darshan_file_category enum is not really
    # exposed in CFFI/Python layer, so we effectively
    # re-export the content indices we need here
    # so that we can properly index the C-level data
    darshan_file_category = {"total files":0,
                             "read-only files":1,
                             "write-only files":2,
                             "read/write files":3}
    df = pd.DataFrame.from_dict(darshan_file_category, orient="index")
    df.rename(columns={0:"index"}, inplace=True)
    df.index.rename('type', inplace=True)
    df["number of files"] = np.zeros(4, dtype=int)
    df["avg. size"] = np.zeros(4, dtype=str)
    df["max size"] = np.zeros(4, dtype=str)

    for cat_name, index in darshan_file_category.items():
        cat_counters = derived_metrics.category_counters[index]
        num_files = int(cat_counters.count)
        if num_files == 0:
            max_size = "0"
            avg_size = "0"
        else:
            max_size, binary_units = humanize.naturalsize(cat_counters.max_offset_bytes + 1,
                                                          binary=True,
                                                          format="%.2f").split()
            if max_size != "0":
                max_size = f"{max_size} {binary_units}"
            # NOTE: internal formula based on discussion with Phil Carns
            avg_size, binary_units = humanize.naturalsize((cat_counters.total_max_offset_bytes + num_files) / num_files,
                                                           binary=True,
                                                           format="%.2f").split()
            if avg_size != "0":
                avg_size = f"{avg_size} {binary_units}"
        df.iloc[index] = [index, num_files, avg_size, max_size]
    # we don't need the index column once we're done with
    # the CFFI/C interaction
    df.drop(columns="index", inplace=True)
    ret = plot_common_access_table.DarshanReportTable(df,
                                                      col_space=200,
                                                      justify="center")
    return ret
