import sys
import pandas as pd
import argparse
import darshan
import darshan.cli
from darshan.backend.cffi_backend import accumulate_records
from typing import Any, Union, Callable
from humanize import naturalsize

from rich.console import Console
from rich.table import Table

def df_IO_data(file_path, mod):
    """
    Save relevant file statisitcs from a single Darshan log file to a DataFrame.

    Parameters
    ----------
    file_path : a string, the path to a darshan log file.
    mod : a string, the module name

    Returns
    -------
    a single DataFrame.

    """
    report = darshan.DarshanReport(file_path, read_all=False)
    if mod not in report.modules:
        return pd.DataFrame()
    report.mod_read_all_records(mod)
    recs = report.records[mod].to_df()
    if mod != 'MPI-IO':
        rec_cols = ['id', f'{mod}_BYTES_READ', f'{mod}_BYTES_WRITTEN', f'{mod}_READS', f'{mod}_WRITES']
    else:
        rec_cols = ['id', 'MPIIO_BYTES_READ', 'MPIIO_BYTES_WRITTEN', 'MPIIO_INDEP_READS', 'MPIIO_COLL_READS', 'MPIIO_INDEP_WRITES', 'MPIIO_COLL_WRITES']
    df = recs['counters'][rec_cols].copy()
    if mod == 'MPI-IO':
        df['MPIIO_READS'] = df['MPIIO_INDEP_READS'] + df['MPIIO_COLL_READS']
        df['MPIIO_WRITES'] = df['MPIIO_INDEP_WRITES'] + df['MPIIO_COLL_WRITES']
        df.drop(columns=['MPIIO_INDEP_READS', 'MPIIO_COLL_READS', 'MPIIO_INDEP_WRITES', 'MPIIO_COLL_WRITES'], inplace=True)
    # try to make column names more uniform
    new_cols = []
    for col in df.columns:
        ndx = col.find('_')
        if ndx > 0:
            new_cols.append(col[ndx+1:].lower())
        else:
            new_cols.append(col)
    df.columns = new_cols
    df.insert(0, 'file', df['id'].map(report.name_records))
    df.insert(1, 'log_file', file_path)
    return df.drop('id', axis=1) # id not needed anymore

def combine_dfs(list_dfs):
    """
    Combine per-job DataFrames of each Darshan log to one DataFrame.

    Parameters
    ----------
    list_dfs : a list of DataFrames.

    Returns
    -------
    a single DataFrame with data from multiple Darshan logs.

    """
    combined_dfs = pd.concat(list_dfs, ignore_index=True)
    return combined_dfs

def group_by_file(combined_dfs):
    """
        Group data using the 'file' column. Additionally, calculate the
        total number of unique jobs accessing each file.

        Parameters
        ----------
        combined_dfs : a DataFrame with data from multiple Darshan logs.

        Returns
        -------
        a DataFrame with the sum of each group.

    """
    sum_cols = combined_dfs.select_dtypes('number').columns
    # group data by file name, counting number of unique jobs (i.e., log files)
    # that access each file, as well as sum total of numerical columns
    df_groupby_file = combined_dfs.groupby('file', as_index=False).agg(
        **{col: (col, 'sum') for col in sum_cols},
        total_jobs=('log_file', 'nunique')
    )
    return df_groupby_file

def sort_dfs_desc(combined_dfs, order_by):
    """
    Sort data by the column name the user inputs in a descending order.

    Parameters
    ----------
    combined_dfs : a DataFrame with data from multiple DataFrames.
    order_by : a string, the column name

    Returns
    -------
    a DataFrame with a descending order of one column.

    """
    combined_dfs_sort = combined_dfs.sort_values(by=[order_by], ascending=False)
    return combined_dfs_sort

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
    mod : a string, the module name
    order_by : a string, the column name of the statistical metric to sort by

    """
    # calculate totals to plug in to table footer
    all_bytes_read = df['bytes_read'].sum()
    all_bytes_written = df['bytes_written'].sum()
    all_reads = df['reads'].sum()
    all_writes = df['writes'].sum()
    all_total_jobs = df['total_jobs'].sum()

    console = Console()
    table = Table(title=f"Darshan {mod} File Stats", show_lines=True, show_footer=True)
    table.add_column("file", f"[u i]TOTAL ({len(df)} files)", justify="center", ratio=5)
    default_kwargs = {"justify": "center", "no_wrap": True, "ratio": 1}
    table.add_column("bytes_read", f"[u i]{naturalsize(all_bytes_read, binary=True, format='%.2f')}", **default_kwargs)
    table.add_column("bytes_written", f"[u i]{naturalsize(all_bytes_written, binary=True, format='%.2f')}", **default_kwargs)
    table.add_column("reads", f"[u i]{all_reads}", **default_kwargs)
    table.add_column("writes", f"[u i]{all_writes}", **default_kwargs)
    table.add_column("total_jobs", f"[u i]{all_total_jobs}", **default_kwargs)
    for column in table.columns:
        if column.header == order_by:
            column.style = column.header_style = column.footer_style = "bold cyan"
    for _, row in df.iterrows():
        table.add_row(row["file"], 
                      f"{naturalsize(row['bytes_read'], binary=True, format='%.2f')}",
                      f"{naturalsize(row['bytes_written'], binary=True, format='%.2f')}",
                      f"{row['reads']}",
                      f"{row['writes']}",
                      f"{row['total_jobs']}")
    console.print(table)

def setup_parser(parser: argparse.ArgumentParser):
    """
    Parses the command line arguments.

    Parameters
    ----------
    parser : command line argument parser.

    """
    parser.description = "Print statistics describing key metadata and I/O performance metrics for files accessed by a given list of jobs."

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
        help="specify the Darshan module to generate file stats for (default: %(default)s)"
    )
    parser.add_argument(
        "--order_by", "-o",
        type=str,
        nargs='?', default='bytes_read',
        choices=['bytes_read', 'bytes_written', 'reads', 'writes', 'total_jobs'],
        help="specify the I/O metric to order files by (default: %(default)s)"
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
    Prints file statistics on a set of input Darshan logs.

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
        if not df_i.empty:
            list_dfs.append(df_i)
    if len(list_dfs) == 0:
        sys.exit()
    combined_dfs = combine_dfs(list_dfs)
    combined_dfs_grouped = group_by_file(combined_dfs)
    combined_dfs_sorted = sort_dfs_desc(combined_dfs_grouped, order_by)
    df = first_n_recs(combined_dfs_sorted, limit)
    if args.csv:
        print(df.to_csv(index=False), end="")
    else:
        rich_print(df, mod, order_by)

if __name__ == "__main__":
    main()
