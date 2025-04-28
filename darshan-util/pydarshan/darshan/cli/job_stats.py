import sys
import pandas as pd
import argparse
import darshan
import darshan.cli
from darshan.backend.cffi_backend import accumulate_records
from typing import Any, Union, Callable
from datetime import datetime
from humanize import naturalsize

import concurrent.futures
from functools import partial

from rich.console import Console
from rich.table import Table

def process_logfile(log_path, mod, filter_patterns, filter_mode):
    """
    Save the statistical data from a single Darshan log file to a DataFrame.

    Parameters
    ----------
    log_path : a string, the path to a Darshan log file.
    mod : a string, the Darshan module name
    filter_patterns: regex patterns for names to exclude/include
    filter_mode: whether to "exclude" or "include" the filter patterns

    Returns
    -------
    a single DataFrame of job statistics.

    """
    try:
        extra_options = {}
        if filter_patterns:
            extra_options["filter_patterns"] = filter_patterns
            extra_options["filter_mode"] = filter_mode
        report = darshan.DarshanReport(log_path, read_all=False)
        if mod not in report.modules:
            return pd.DataFrame()
        report.mod_read_all_records(mod, **extra_options)
        if len(report.records[mod]) == 0:
            return pd.DataFrame()
        recs = report.records[mod].to_df()
        acc_rec = accumulate_records(recs, mod, report.metadata['job']['nprocs'])
        dict_acc_rec = {}
        dict_acc_rec['log_file'] = log_path.split('/')[-1]
        dict_acc_rec['exe'] = report.metadata['exe']
        dict_acc_rec['job_id'] = report.metadata['job']['jobid']
        dict_acc_rec['nprocs'] = report.metadata['job']['nprocs']
        dict_acc_rec['start_time'] = report.metadata['job']['start_time_sec']
        dict_acc_rec['end_time'] = report.metadata['job']['end_time_sec']
        dict_acc_rec['run_time'] = report.metadata['job']['run_time']
        dict_acc_rec['perf_by_slowest'] = acc_rec.derived_metrics.agg_perf_by_slowest * 1024**2
        dict_acc_rec['time_by_slowest'] = acc_rec.derived_metrics.agg_time_by_slowest
        dict_acc_rec['total_bytes'] = acc_rec.derived_metrics.total_bytes
        dict_acc_rec['total_files'] = acc_rec.derived_metrics.category_counters[0].count
        df = pd.DataFrame.from_dict([dict_acc_rec])
        return df
    except Exception as e:
        print(f"Error processing {log_path}: {e}", file=sys.stderr)
        return pd.DataFrame()

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
    order_by : a string, the column name of the statistical metric to sort by.

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

def rich_print(df, mod, order_by):
    """
    Pretty print the DataFrame using rich tables.

    Parameters
    ----------
    df : a dataframe
    mod : a string, the Darshan module name
    order_by : a string, the column name of the statistical metric to sort by

    """
    # calculate totals to plug in to table footer
    all_time_by_slowest = df['time_by_slowest'].sum()
    all_total_bytes = df['total_bytes'].sum()
    all_total_files = df['total_files'].sum()
    all_perf_by_slowest = all_total_bytes / all_time_by_slowest

    # instantiate a rich table and pretty print the dataframe
    console = Console()
    table = Table(title=f"Darshan {mod} Job Stats", show_lines=True, show_footer=True)
    table.add_column("job", f"[u i]TOTAL ({len(df)} jobs)", justify="center", ratio=4)
    default_kwargs = {"justify": "center", "no_wrap": True, "ratio": 1}
    table.add_column("perf_by_slowest", f"[u i]{naturalsize(all_perf_by_slowest, binary=True, format='%.2f')}/s", **default_kwargs)
    table.add_column("time_by_slowest", f"[u i]{all_time_by_slowest:.2f} s", **default_kwargs)
    table.add_column("total_bytes", f"[u i]{naturalsize(all_total_bytes, binary=True, format='%.2f')}", **default_kwargs)
    table.add_column("total_files", f"[u i]{all_total_files}", **default_kwargs)
    for column in table.columns:
        if column.header == order_by:
            column.style = column.header_style = column.footer_style = "bold cyan"
    for _, row in df.iterrows():
        job_str  = f"[bold]job id[/bold]: {row['job_id']}\n"
        job_str += f"[bold]nprocs[/bold]: {row['nprocs']}\n"
        job_str += f"[bold]start time[/bold]: {datetime.fromtimestamp(row['start_time']).strftime('%m/%d/%Y %H:%M:%S')}\n"
        job_str += f"[bold]end time[/bold]: {datetime.fromtimestamp(row['end_time']).strftime('%m/%d/%Y %H:%M:%S')}\n"
        job_str += f"[bold]runtime[/bold]: {row['run_time']:.2f} s\n"
        job_str += f"[bold]exe[/bold]: {row['exe']}\n"
        job_str += f"[bold]log file[/bold]: {row['log_file']}"
        table.add_row(job_str,
                      f"{naturalsize(row['perf_by_slowest'], binary=True, format='%.2f')}/s",
                      f"{row['time_by_slowest']:.2f} s",
                      f"{naturalsize(row['total_bytes'], binary=True, format='%.2f')}",
                      f"{row['total_files']}")
    console.print(table)

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
        nargs='+',
        help="specify the paths to Darshan log files"
    )
    parser.add_argument(
        "--module", "-m",
        nargs='?', default='POSIX',
        choices=['POSIX', 'MPI-IO', 'STDIO'],
        help="specify the Darshan module to generate job stats for (default: %(default)s)"
    )
    parser.add_argument(
        "--order_by", "-o",
        nargs='?', default='total_bytes',
        choices=['perf_by_slowest', 'time_by_slowest', 'total_bytes', 'total_files'],
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
    parser.add_argument(
        "--exclude_names", "-e",
        action='append',
        help="regex patterns for file record names to exclude in stats"
    )
    parser.add_argument(
        "--include_names", "-i",
        action='append',
        help="regex patterns for file record names to include in stats"
     )


def main(args: Union[Any, None] = None):
    """
    Prints job statistics on a set of input Darshan logs.

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
    filter_patterns=None
    filter_mode=None
    if args.exclude_names and args.include_names:
        print('job_stats error: only one of --exclude-names and --include-names may be used.')
        sys.exit(1)
    elif args.exclude_names:
        filter_patterns = args.exclude_names
        filter_mode = "exclude"
    elif args.include_names:
        filter_patterns = args.include_names
        filter_mode = "include"
    process_logfile_with_args = partial(process_logfile, mod=mod, filter_patterns=filter_patterns, filter_mode=filter_mode)
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = list(executor.map(process_logfile_with_args, log_paths, chunksize=32))
    list_dfs = [df for df in results if not df.empty]
    if len(list_dfs) == 0:
        sys.exit()
    combined_dfs = combine_dfs(list_dfs)
    combined_dfs_sorted = sort_dfs_desc(combined_dfs, order_by)
    df = first_n_recs(combined_dfs_sorted, limit)
    if args.csv:
        df = df.drop("exe", axis=1)
        print(df.to_csv(index=False), end="")
    else:
        rich_print(df, mod, order_by)

if __name__ == "__main__":
    main()
