import pandas as pd
import argparse
import darshan
import darshan.cli
from darshan.backend.cffi_backend import accumulate_records
from typing import Any, Union, Callable
import glob

def df_IO_data(file_path, mod):
    """
    Save the data in the columns ('id', 'POSIX_BYTES_READ', 'POSIX_BYTES_WRITTEN') from a single log file to a DataFrame.

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
    df = posix_recs['counters'][['id', f'{mod}_BYTES_READ', f'{mod}_BYTES_WRITTEN']]
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

def group_by_id(combine_dfs):
    """
        Group DataFrame using the column 'id'.

        Parameters
        ----------
        combined_dfs : a DataFrame with data from multiple DataFrames.

        Returns
        -------
        a DataFrame with the sum of each group.

    """
    df_groupby_id_max = combine_dfs.groupby('id').sum()
    return df_groupby_id_max

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

    combined_dfs_first_n = df.head(n)
    return combined_dfs_first_n

def setup_parser(parser: argparse.ArgumentParser):
    """
    Configures the command line arguments.

    Parameters
    ----------
    parser : command line argument parser.

    """
    parser.description = "Generates a DataFrame with statistical data of n jobs for a certain module"

    parser.add_argument(
        "log_path",
        type=str,
        nargs='+',
        help="Specify the path to darshan log files."
    )
    parser.add_argument(
        "-module", "-m",
        type=str,
        nargs='?', default='POSIX',
        help="Specify the module name."
    )
    parser.add_argument(
        "-order_by_colname", "-o",
        type=str,
        nargs='?', default='POSIX_BYTES_READ',
        help="Specify the column name."
    )
    parser.add_argument(
        "-number_of_rows", "-n",
        type=int,
        nargs='?', default='10',
        help="The first n rows of the DataFrame"
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
    list_modules = ["POSIX", "MPI-IO", "LUSTRE", "STDIO"]
    if mod not in list_modules:
        print(f'{mod} is not in list')
        sys.exit()
    order_by_colname = args.order_by_colname
    if order_by_colname == " ":
        order_by_colname = f'{mod}_BYTES_READ'
    n = args.number_of_rows
    colname_list = [f'{mod}_BYTES_READ', f'{mod}_BYTES_WRITTEN']
    if order_by_colname in colname_list:
        log_paths = args.log_path
        item_number = len(log_paths)
        list_dfs = []
        for i in range(item_number):
            df_i = df_IO_data(log_paths[i], mod)
            list_dfs.append(df_i)
        com_dfs = combined_dfs(list_dfs)
        combined_dfs_groupby = group_by_id(com_dfs)
        combined_dfs_sort = sort_data_desc(combined_dfs_groupby, order_by_colname)
        combined_dfs_selected = first_n_recs(combined_dfs_sort, n)
        print("Statistical data of files:\n", combined_dfs_selected)
    else:
        print("Column name should be '{mod}_BYTES_READ' or '{mod}_BYTES_WRITTEN'")

if __name__ == "__main__":
    main()