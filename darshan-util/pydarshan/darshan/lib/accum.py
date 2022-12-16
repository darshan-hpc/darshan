from darshan.backend.cffi_backend import log_get_derived_metrics


def log_get_bytes_bandwidth(log_path: str, mod_name: str) -> str:
    """
    Summarize I/O performance for a given darshan module.

    Parameters
    ----------
    log_path : str
            Path to the darshan binary log file.
    mod_name : str
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
    darshan_derived_metrics = log_get_derived_metrics(log_path=log_path,
                                                      mod_name=mod_name)
    total_mib = darshan_derived_metrics.total_bytes / 2 ** 20
    total_bw = darshan_derived_metrics.agg_perf_by_slowest
    ret_str = f"I/O performance estimate (at the {mod_name} layer): transferred {total_mib:.1f} MiB at {total_bw:.2f} MiB/s"
    return ret_str
