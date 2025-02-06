import pandas as pd
import argparse
import darshan
import darshan.cli
from darshan.backend.cffi_backend import accumulate_records
from typing import Any, Union, Callable

def df_IO_data(file_path, mod):
    """
    Save the statistical data from a single Darshan log file to a DataFrame.

    Parameters
    ----------
    file_path : a string, the path to a Darshan log file.
    mod : a string, the Darshan module name

    Returns
    -------
    a single DataFrame of job statistics.

    """
    report = darshan.DarshanReport(file_path, read_all=True)
    posix_recs = report.records[mod].to_df()
    acc_recs = accumulate_records(posix_recs, mod, report.metadata['job']['nprocs'])
    dict_recs = {}
    dict_recs['agg_perf_by_slowest'] = acc_recs.derived_metrics.agg_perf_by_slowest
    dict_recs['agg_time_by_slowest'] = acc_recs.derived_metrics.agg_time_by_slowest
    dict_recs['total_bytes'] = acc_recs.derived_metrics.total_bytes
    df = pd.DataFrame.from_dict([dict_recs])
    return df

def combine_dfs(list_dfs):
    """
    Combine per-job DataFrames of each Darshan log into one DataFrame.

    Parameters
    ----------
    list_dfs : a list of DataFrames.

    Returns
    -------
    a single DataFrame with data from multiple Darshan logs.

    """
    combined_dfs = pd.concat(list_dfs, ignore_index=True)
    return combined_dfs

def sort_dfs_desc(combined_dfs, order_by):
    """
    Sort data by the column name the user inputs in a descending order.

    Parameters
    ----------
    combined_dfs : a DataFrame with data from multiple Darshan logs.
    order_by : a string, the column name of the statistical metric to sort by

    Returns
    -------
    a DataFrame sorted in descending order by a given column.

    """
    combined_dfs_sorted = combined_dfs.sort_values(by=[order_by], ascending=False)
    return combined_dfs_sorted

def first_n_recs(df, n):
    """
    Filter the data to return only the first n records.

    Parameters
    ----------
    df : a dataframe
    n : an int, number of rows.

    Returns
    -------
    a DataFrame with n rows.

    """
    if n >= 0:
        return df.head(n)
    else:
        return df

def setup_parser(parser: argparse.ArgumentParser):
    """
    Parses the command line arguments.

    Parameters
    ----------
    parser : command line argument parser.

    """
    parser.description = "Print statistics describing key metadata and I/O performance metrics for a given list of jobs."

    parser.add_argument(
        "log_paths",
        type=str,
        nargs='+',
        help="specify the paths to Darshan log files"
    )
    parser.add_argument(
        "--module", "-m",
        type=str,
        nargs='?', default='POSIX',
        choices=['POSIX', 'MPI-IO', 'STDIO'],
        help="specify the Darshan module to generate job stats for (default: %(default)s)"
    )
    parser.add_argument(
        "--order_by", "-o",
        type=str,
        nargs='?', default='total_bytes',
        choices=['agg_perf_by_slowest', 'agg_time_by_slowest', 'total_bytes'],
        help="specify the I/O metric to order jobs by (default: %(default)s)"
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        nargs='?', default='-1',
        help="limit output to the top LIMIT number of jobs according to selected metric"
    )
    parser.add_argument(
        "--csv", "-c",
        action='store_true',
        help="output job stats in CSV format"
    )

def main(args: Union[Any, None] = None):
    """
    Prints statistics on a set of input Darshan job logs.

    Parameters
    ----------
    args: command line arguments.

    """
    if args is None:
        parser = argparse.ArgumentParser(description="")
        setup_parser(parser)
        args = parser.parse_args()
    mod = args.module
    order_by = args.order_by
    limit = args.limit
    log_paths = args.log_paths
    list_dfs = []
    for log_path in log_paths:
        df_i = df_IO_data(log_path, mod)
        list_dfs.append(df_i)
    combined_dfs = combine_dfs(list_dfs)
    combined_dfs_sorted = sort_dfs_desc(combined_dfs, order_by)
    combined_dfs_selected = first_n_recs(combined_dfs_sorted, limit)
    if args.csv:
        print(combined_dfs_selected.to_csv(index=False))
    else:
        print(combined_dfs_selected.to_string(index=False))

if __name__ == "__main__":
    main()
