import pandas as pd
import argparse
import darshan
import darshan.cli
from darshan.backend.cffi_backend import accumulate_records
from typing import Any, Union, Callable

def df_IO_data(file_path, mod):
    """
    Save the data from a single log file to a DataFrame.

    Parameters
    ----------
    file_path : a string, the path to a darshan log file.
    mod : a string, the module name

    Returns
    -------
    a single DataFrame.

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

def combined_dfs(list_dfs):
    """
    Combine DataFrames of each log files to one DataFrame.

    Parameters
    ----------
    list_dfs : a list of DataFrames.

    Returns
    -------
    a single DataFrame with data from multiple DataFrames.

    """
    combined_dfs = pd.concat(list_dfs, ignore_index = True)
    return combined_dfs

def sort_data_desc(combined_dfs, order_by_colname):
    """
    Sort data by the column name the user inputs in a descending order.

    Parameters
    ----------
    combined_dfs : a DataFrame with data from multiple DataFrames.
    order_by_colname : a string, the column name

    Returns
    -------
    a DataFrame with a descending order of one column.

    """
    combined_dfs_sort = combined_dfs.sort_values(by=[order_by_colname], ascending=False)
    return combined_dfs_sort

def first_n_recs(df, n):
    """
    Filtering the data with the first n records.

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
    Configures the command line arguments.

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

def main(args: Union[Any, None] = None):
    """
    Generates a DataFrame based on user's input.

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
    com_dfs = combined_dfs(list_dfs)
    combined_dfs_sort = sort_data_desc(com_dfs, order_by_colname)
    combined_dfs_selected = first_n_recs(combined_dfs_sort, n)
    print("Statistical data of jobs:\n", combined_dfs_selected)

if __name__ == "__main__":
    main()
