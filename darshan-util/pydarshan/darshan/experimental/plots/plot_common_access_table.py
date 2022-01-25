from typing import Any, List

import pandas as pd

import darshan


def remove_nonzero_rows(df: Any) -> Any:
    """
    Removes dataframe rows that contain all zero values.

    Parameters
    ----------
    df: a ``pd.DataFrame``.

    Returns
    -------
    A ``pd.DataFrame`` containing a subset of rows from the input
    dataframe, where each row contains at least 1 non-zero value.

    """
    return df.loc[(df != 0).any(axis=1)]


def combine_access_sizes(df: Any) -> Any:
    """
    Combines rows with identical values in the "Access Size" column
    and calculates the sum for all other numeric columns.

    Parameters
    ----------
    df: a ``pd.DataFrame`` with a column named "Access Size".

    Returns
    -------
    A ``pd.DataFrame`` where "Access Size" column is the index and
    remaining columns contain the summed data from grouped rows.

    """
    return df.groupby("Access Size").sum().reset_index()


def get_most_common_access_sizes(df: Any, n_rows: int = 4) -> Any:
    """
    Returns the rows with the `n_rows` largest "Count" values.

    Parameters
    ----------
    df: a ``pd.DataFrame`` with a column named "Count".

    n_rows: number of rows to keep.

    Returns
    -------
    A ``pd.DataFrame`` containing the largest `n_rows` "Count" values.

    """
    return df.sort_values(by="Count", ascending=False, ignore_index=True).head(n=n_rows)


def collapse_access_cols(df: Any, col_name: str) -> Any:
    """
    Collapses all columns into a single column named `col_name`.

    Parameters
    ----------
    df: a ``pd.DataFrame``.

    col_name: name of new column to store collapsed data.

    Returns
    -------
    A ``pd.DataFrame`` containing all data collapsed into column `col_name`.

    """
    return df.melt(value_name=col_name).drop("variable", axis=1)


def get_access_count_df(mod_df: Any, mod: str) -> Any:
    """
    Creates a dataframe containing only the access size and count data.

    Parameters
    ----------
    mod_df: "counters" dataframe for the input
    module `mod` from a ``darshan.DarshanReport``.

    mod: the module to obtain the common accesses
    table for (i.e "POSIX", "MPI-IO", "H5D").

    Returns
    -------
    A ``pd.DataFrame`` containing all access size
    data and their respective counts.

    """
    col_dict = {"ACCESS": "Access Size", "COUNT": "Count"}
    df_list: List = []
    for counter_name, col_name in col_dict.items():
        # filter out any columns not related to the access sizes
        filter_keys = [f"{mod}_ACCESS{i}_{counter_name}" for i in range(1, 5)]
        df = mod_df.filter(filter_keys)
        df = collapse_access_cols(df=df, col_name=col_name)
        df_list.append(df)

    return pd.concat(df_list, axis=1)


class DarshanReportTable:
    """
    Stores table figures in dataframe and html formats.

    Parameters
    ----------
    df: a ``pd.DataFrame``.

    kwargs: keyword arguments passed to ``pd.DataFrame.to_html()``.

    """
    def __init__(self, df: Any, **kwargs):
        self.df = df
        self.html = self.df.to_html(**kwargs)


def plot_common_access_table(report: darshan.DarshanReport, mod: str, n_rows: int = 4) -> DarshanReportTable:
    """
    Creates a table containing the most
    common access sizes and their counts.

    Parameters
    ----------
    report: a ``darshan.DarshanReport``.

    mod: the module to obtain the common access size
    table for (i.e "POSIX", "MPI-IO", "H5D").

    n_rows: number of rows to keep.

    Returns
    -------
    common_access_table: a ``DarshanReportTable`` containing the `n_rows`
    most common access sizes and their counts for the specified module.
    The table is sorted in descending order based on the access size count
    and can be retrieved as either a ``pd.DataFrame`` or html table via
    the `df` or `html` attributes, respectively.

    """
    mod_df = report.records[mod].to_df(attach=None)["counters"]

    if mod == "MPI-IO":
        mod = "MPIIO"

    df = get_access_count_df(mod_df=mod_df, mod=mod)
    df = remove_nonzero_rows(df=df)
    df = combine_access_sizes(df=df)
    df = get_most_common_access_sizes(df=df, n_rows=n_rows)
    common_access_table = DarshanReportTable(
        # remove index labels and remove border
        df=df, index=False, border=0,
    )
    return common_access_table
