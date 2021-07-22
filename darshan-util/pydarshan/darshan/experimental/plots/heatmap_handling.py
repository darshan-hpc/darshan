"""
Module of data pre-processing functions for constructing the heatmap figure.
"""
from __future__ import annotations

from typing import Dict, Any, Tuple, Sequence, TYPE_CHECKING

import sys
if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict

import pandas as pd
import numpy as np

if TYPE_CHECKING:
    import numpy.typing as npt


class SegDict(TypedDict):
    """
    Custom type hint class for `dict_list` argument in `get_rd_wr_dfs()`.
    """

    id: int
    rank: int
    hostname: str
    write_count: int
    read_count: int
    write_segments: pd.DataFrame
    read_segments: pd.DataFrame


def get_rd_wr_dfs(
    dict_list: Sequence[SegDict], ops: Sequence[str] = ["read", "write"]
) -> Dict[str, pd.DataFrame]:
    """
    Uses the DXT records to construct individual
    dataframes for both read and write segments.

    Parameters
    ----------

    dict_list: a sequence of DXT records, where each record is a
    Python dictionary with the following keys: 'id', 'rank',
    'hostname', 'write_count', 'read_count', 'write_segments',
    and 'read_segments'. The read/write data is stored in
    ``read_segments`` and ``write_segments``, where each is a
    ```pd.DataFrame`` containing the following data (columns):
    'offset', 'length', 'start_time', 'end_time'.

    ops: a sequence of keys designating which Darshan operations
    to collect data for. Default is ``["read", "write"]``.

    Returns
    -------

    rd_wr_dfs: dictionary where each key is an operation from the input
    ``ops`` parameter (i.e. "read", "write") and each value is a
    ``pd.DataFrame`` object containing all of the read/write events.

    Notes
    -----

    Used in ``get_single_df_dict()``.

    Examples
    --------

    ``dict_list`` and ``rd_wr_dfs``
    generated from ``tests/input/sample-dxt-simple.darshan``:

        dict_list = [
            {
                'id': 14388265063268455899,
                'rank': 0,
                'hostname': 'sn176.localdomain',
                'write_count': 1,
                'read_count': 0,
                'write_segments':
                    offset  length  start_time  end_time
                    0       0      40    0.103379  0.103388,
                'read_segments':
                    Empty DataFrame
                    Columns: []
                    Index: []
            },
            {
                'id': 9457796068806373448,
                'rank': 0,
                'hostname': 'sn176.localdomain',
                'write_count': 1,
                'read_count': 0,
                'write_segments':
                    offset  length  start_time  end_time
                    0       0    4000    0.104217  0.104231,
                'read_segments':
                    Empty DataFrame
                    Columns: []
                    Index: []
            },
        ]

        rd_wr_dfs = {
            'read':
                Empty DataFrame
                Columns: []
                Index: [],
            'write':
                length  start_time  end_time  rank
                0      40    0.103379  0.103388     0
                1    4000    0.104217  0.104231     0
        }

    """
    # columns to drop when accumulating the dataframes.
    # Currently "offset" data is not utilized
    drop_columns = ["offset"]
    # create empty dictionary to store
    # the concatenated read/write dataframes
    rd_wr_dfs = {}
    # iterate over each operation
    for op_key in ops:
        # mypy can't tell that these keys are in fact
        # in the ``SegDict``, so just ignore the type
        seg_key = op_key + "_segments"  # type: ignore
        # create empty list to store each dataframe
        df_list = []
        # iterate over all records/dictionaries
        for _dict in dict_list:
            # ignore for the same reason as above
            seg_df = _dict[seg_key]  # type: ignore
            if seg_df.size:
                # drop unused columns from the dataframe
                seg_df = seg_df.drop(columns=drop_columns)
                # create new column for the ranks
                seg_df["rank"] = _dict["rank"]
                # add the dataframe to the list
                df_list.append(seg_df)

        if df_list:
            # concatenate the list of pandas dataframes into
            # a single one with new row indices
            rd_wr_dfs[op_key] = pd.concat(df_list, ignore_index=True)
        else:
            # if the list is empty assign an empty dataframe
            rd_wr_dfs[op_key] = pd.DataFrame()

    return rd_wr_dfs


def get_single_df_dict(
    report: Any,
    mods: Sequence[str] = ["DXT_POSIX"],
    ops: Sequence[str] = ["read", "write"],
) -> Dict[str, Dict[str, pd.DataFrame]]:
    """
    Reorganizes segmented read/write data into a single ``pd.DataFrame``
    and stores them in a dictionary with an entry for each DXT module.

    Parameters
    ----------

    report: a ``darshan.DarshanReport``.

    mods: a sequence of keys designating which Darshan modules to use for
    data aggregation. Default is ``["DXT_POSIX"]``.

    ops: a sequence of keys designating which Darshan operations to use for
    data aggregation. Default is ``["read", "write"]``.

    Returns
    -------

    flat_data_dict: a nested dictionary where the input module
    keys (i.e. "DXT_POSIX") are the top level keys, which contain
    an entry for each input operation (i.e. "read"/"write") that
    map to dataframes containing all events for the specified operation.

    Examples
    --------
    `flat_data_dict` generated from `tests/input/sample-dxt-simple.darshan`:
        {
            'DXT_POSIX':
                {
                    'read':
                        Empty DataFrame
                        Columns: []
                        Index: [],
                    'write':
                        length  start_time  end_time  rank
                        0      40    0.103379  0.103388     0
                        1    4000    0.104217  0.104231     0
                }
        }

    """
    # initialize an empty dictionary for storing
    # module and read/write data
    flat_data_dict = {}  # type: Dict[str, Dict[str, pd.DataFrame]]
    # iterate over the modules (i.e. DXT_POSIX)
    for module_key in mods:
        # read in the module data, update the name records
        report.mod_read_all_dxt_records(module_key, dtype="pandas")
        # retrieve the list of records in pd.DataFrame() form
        dict_list = report.records[module_key].to_df()
        # retrieve the list of read/write dataframes from the list of records
        rd_wr_dfs = get_rd_wr_dfs(dict_list=dict_list, ops=ops)
        # create empty dictionary for each module
        flat_data_dict[module_key] = {}
        for op_key in ops:
            # add the concatenated dataframe to the flat dictionary
            flat_data_dict[module_key][op_key] = rd_wr_dfs[op_key]

    return flat_data_dict


def get_aggregate_data(
    report: Any,
    mods: Sequence[str] = ["DXT_POSIX"],
    ops: Sequence[str] = ["read", "write"],
) -> pd.DataFrame:
    """
    Aggregates the data based on which
    modules and operations are selected.

    Parameters
    ----------

    report: a ``darshan.DarshanReport``.

    mods: a sequence of keys designating which Darshan modules to use for
    data aggregation. Default is ``["DXT_POSIX"]``.

    ops: a sequence of keys designating which Darshan operations to use for
    data aggregation. Default is ``["read", "write"]``.

    Returns
    -------

    agg_df: a ``pd.DataFrame`` containing the aggregated data determined
    by the input modules and operations.

    Raises
    ------

    ValueError: raised if the selected modules/operations
    don't contain any data.

    Notes
    -----
    Since read and write events are considered unique events, if both are
    selected their dataframes are simply concatenated.

    Examples
    --------
    `agg_df` generated from `tests/input/sample-dxt-simple.darshan`:

            length  start_time  end_time  rank
        0      40    0.103379  0.103388     0
        1    4000    0.104217  0.104231     0

    """
    # collect the concatenated dataframe data from the darshan report
    df_dict = get_single_df_dict(report=report, mods=mods, ops=ops)
    # TODO: generalize for all DXT modules, for now manually set `DXT_POSIX`
    module_key = "DXT_POSIX"
    # iterate over each dataframe based on which operations are selected
    df_list = []
    for op_key, op_df in df_dict[module_key].items():
        # if the dataframe has data, append it to the list
        if op_df.size:
            df_list.append(op_df)

    if df_list:
        # if there are dataframes in the list, concatenate them into 1 dataframe
        agg_df = pd.concat(df_list, ignore_index=True)
    else:
        raise ValueError("No data available for selected module(s) and operation(s).")

    return agg_df


def calc_prop_data_sum(
    tmin: float,
    tmax: float,
    total_elapsed: npt.NDArray[np.float64],
    total_data: npt.NDArray[np.float64],
) -> float:
    """
    Calculates the proportion of data read/written in the
    time interval of a single bin in ``get_heatmap_data``.

    Parameters
    ----------

    tmin: the lower bound of the time interval for a given bin.

    tmax: the upper bound of the time interval for a given bin.

    total_elapsed: an array of the elapsed times for every event
    that occurred within the time interval of a given bin.

    total_data: an array of the data totals for every event that
    occurred within the time interval of a given bin.

    Returns
    -------

    prop_data_sum: the amount of data read/written in the time
    interval of a given bin.

    """
    # calculate the elapsed time
    partial_elapsed = tmax - tmin
    # calculate the ratio of the elapsed time
    # to the total read/write event time
    proportionate_time = partial_elapsed / total_elapsed
    # calculate the amount of data read/written in the elapsed
    # time (assuming a constant read/write rate)
    proportionate_data = proportionate_time * total_data
    # sum the data
    prop_data_sum = proportionate_data.sum()
    return prop_data_sum


def get_heatmap_data(agg_df: pd.DataFrame, xbins: int) -> npt.NDArray[np.float64]:
    """
    Builds an array similar to a 2D-histogram, where the y data is the unique
    ranks and the x data is time. Each bin is populated with the data sum
    and/or proportionate data sum for all IO events read/written during the
    time spanned by the bin.

    Parameters
    ----------

    agg_df: a ``pd.DataFrame`` containing the aggregated data determined
    by the input modules and operations.

    xbins: the number of x-axis bins to create.

    Returns
    -------

    hmap_data: ``NxM`` array, where ``N`` is the number of unique ranks
    and ``M`` is the number of x-axis bins. Each element contains the
    data read/written by the corresponding rank in the x-axis bin time
    interval.

    """
    # get the unique ranks
    unique_ranks = np.unique(agg_df["rank"].values)

    # generate the bin edges by generating an array of length n_bins+1, then
    # taking pairs of data points as the min/max bin value
    min_time = 0.0
    max_time = agg_df["end_time"].values.max()
    bin_edge_data = np.linspace(min_time, max_time, xbins + 1)

    # calculate the elapsed time for each data point
    elapsed_time_data = agg_df["end_time"].values - agg_df["start_time"].values

    # generate an array for the heatmap data
    hmap_data = np.zeros((unique_ranks.size, xbins), dtype=float)

    # iterate over the unique ranks
    for i, rank in enumerate(unique_ranks):
        # for each rank, generate a mask to select only
        # the data points that correspond to that rank
        rank_mask = agg_df["rank"].values == rank
        bytes_data = agg_df["length"].values[rank_mask]
        start_data = agg_df["start_time"].values[rank_mask]
        end_data = agg_df["end_time"].values[rank_mask]
        elapsed_data = elapsed_time_data[rank_mask]

        # iterate over the bins
        for j, (bmin, bmax) in enumerate(zip(bin_edge_data[:-1], bin_edge_data[1:])):
            # create a mask for all data with a start time greater than the
            # bin minimum time
            start_mask_min = start_data >= bmin
            # create a mask for all data with an end time less than the
            # bin maximum time
            end_mask_max = end_data <= bmax
            # use the above masks to find the indices of all data with
            # start/end times that fall within the bin min/max times
            start_inside_idx = np.nonzero(start_mask_min & (start_data <= bmax))[0]
            end_inside_idx = np.nonzero((end_data > bmin) & end_mask_max)[0]
            # using the start/end indices, find all indices that both start
            # and end within the bin min/max limits
            inside_idx = np.intersect1d(start_inside_idx, end_inside_idx)
            # use the original masks to find the indices of data that start
            # before the bin minimum time and end after the bin maximum time
            outside_idx = np.nonzero(~start_mask_min & ~end_mask_max)[0]

            if inside_idx.size:
                # for data that start/end inside the bin limits,
                # add the sum of the correspondign data to the hmap data
                hmap_data[i, j] += bytes_data[inside_idx].sum()
                # now remove any indices from the start/end index arrays
                # to prevent double counting
                start_inside_idx = np.setdiff1d(start_inside_idx, inside_idx)
                end_inside_idx = np.setdiff1d(end_inside_idx, inside_idx)

            if outside_idx.size:
                # for data that start before the bin min time and end
                # after the bin max time (run longer than 1 bin time),
                # calculate the proportionate data used in 1 bin time
                # and add it to the hmap data
                prop_data_sum = calc_prop_data_sum(
                    tmin=bmin,
                    tmax=bmax,
                    total_elapsed=elapsed_data[outside_idx],
                    total_data=bytes_data[outside_idx],
                )
                hmap_data[i, j] += prop_data_sum

            if start_inside_idx.size:
                # for data with only a start time within the bin limits,
                # calculate the elapsed time (from start to bin max), use the
                # elapsed time to calculate the proportionate data read/written,
                # and add the data sum to the hmap data
                prop_data_sum = calc_prop_data_sum(
                    tmin=start_data[start_inside_idx],
                    tmax=bmax,
                    total_elapsed=elapsed_data[start_inside_idx],
                    total_data=bytes_data[start_inside_idx],
                )
                hmap_data[i, j] += prop_data_sum

            if end_inside_idx.size:
                # for data with only an end time within the bin limits,
                # calculate the elapsed time (from bin min to end time), use the
                # elapsed time to calculate the proportionate data read/written,
                # and add the data sum to the hmap data
                prop_data_sum = calc_prop_data_sum(
                    tmin=bmin,
                    tmax=end_data[end_inside_idx],
                    total_elapsed=elapsed_data[end_inside_idx],
                    total_data=bytes_data[end_inside_idx],
                )
                hmap_data[i, j] += prop_data_sum

    return hmap_data
