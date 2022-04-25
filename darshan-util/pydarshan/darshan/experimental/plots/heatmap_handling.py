"""
Module of data pre-processing functions for constructing the heatmap figure.
"""

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
        seg_key = op_key + "_segments"
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
    mod: str = "DXT_POSIX",
    ops: Sequence[str] = ["read", "write"],
) -> Dict[str, pd.DataFrame]:
    """
    Reorganizes segmented read/write data into a single ``pd.DataFrame``
    and stores them in a dictionary with an entry for each DXT module.

    Parameters
    ----------

    report: a ``darshan.DarshanReport``.

    mod: the DXT module to do analysis for (i.e. "DXT_POSIX"
    or "DXT_MPIIO"). Default is ``"DXT_POSIX"``.

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
    # initialize an empty dictionary for storing
    # module and read/write data
    flat_data_dict = {}  # type: Dict[str, Dict[str, pd.DataFrame]]
    # retrieve the list of records in pd.DataFrame() form
    dict_list = report.records[mod].to_df()
    # retrieve the list of read/write dataframes from the list of records
    rd_wr_dfs = get_rd_wr_dfs(dict_list=dict_list, ops=ops)
    # create empty dictionary
    flat_data_dict = {}
    for op_key in ops:
        # add the concatenated dataframe to the flat dictionary
        flat_data_dict[op_key] = rd_wr_dfs[op_key]

    return flat_data_dict


def get_aggregate_data(
    report: Any,
    mod: str = "DXT_POSIX",
    ops: Sequence[str] = ["read", "write"],
) -> pd.DataFrame:
    """
    Aggregates the data based on which
    modules and operations are selected.

    Parameters
    ----------

    report: a ``darshan.DarshanReport``.

    mod: the DXT module to do analysis for (i.e. "DXT_POSIX"
    or "DXT_MPIIO"). Default is ``"DXT_POSIX"``.

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
    df_dict = get_single_df_dict(report=report, mod=mod, ops=ops)
    # iterate over each dataframe based on which operations are selected
    df_list = []
    for op_key, op_df in df_dict.items():
        # if the dataframe has data, append it to the list
        if op_df.size:
            df_list.append(op_df)

    if df_list:
        # if there are dataframes in the list, concatenate them into 1 dataframe
        agg_df = pd.concat(df_list, ignore_index=True)
    else:
        raise ValueError("No data available for selected module(s) and operation(s).")

    return agg_df


def get_heatmap_df(agg_df: pd.DataFrame, xbins: int, nprocs: int) -> pd.DataFrame:
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

    nprocs: the number of MPI ranks/processes used at runtime.

    Returns
    -------

    hmap_df: dataframe with time intervals for columns and rank
    index (0, 1, etc.) for rows, where each element contains the data
    read/written by the corresponding rank in the given time interval.

    Examples
    --------
    The first column/bin for the `hmap_df` generated from
    "examples/example-logs/ior_hdf5_example.darshan":

               (0.0, 0.09552296002705891]
        rank
        0                   8.951484e+05
        1                   3.746313e+05
        2                   6.350999e+05
        3                   1.048576e+06

    """
    # generate the bin edges by generating an array of length n_bins+1, then
    # taking pairs of data points as the min/max bin value
    max_time = agg_df["end_time"].max()
    bin_edge_data = np.linspace(0.0, max_time, xbins + 1)
    # create dummy variables for start/end time data, where dataframe columns
    # are the x-axis bin ranges
    cats_start = pd.get_dummies(
        pd.cut(agg_df["start_time"], bin_edge_data, precision=16)
    )
    cats_end = pd.get_dummies(pd.cut(agg_df["end_time"], bin_edge_data, precision=16))
    # get series for the elapsed times for each dxt segment
    elapsed_times_dxt_segments = agg_df["end_time"] - agg_df["start_time"]
    # calculate the time interval spanned by each bin
    bin_size = bin_edge_data[1] - bin_edge_data[0]
    # get the ratio of the bin time interval over the dxt segment elapsed times
    true_fraction_of_dxt_segment_in_a_bin = bin_size / elapsed_times_dxt_segments
    # create a version of the above series where any ratio above 1 is set to 1
    fraction_of_dxt_segment_in_a_bin = true_fraction_of_dxt_segment_in_a_bin.copy()
    fraction_of_dxt_segment_in_a_bin.mask(
        fraction_of_dxt_segment_in_a_bin > 1, 1, inplace=True
    )
    # create a dataframe of binned dxt start events where occupied bins contain
    # the start time and unoccupied bins are populated with NaNs
    start_times_in_bins = cats_start.mul(agg_df["start_time"], axis=0).replace(
        0, np.nan
    )
    # create a dataframe of binned dxt start events where occupied bins contain
    # the proportional time spent in the bin
    start_fraction_bin_occupancy = (bin_edge_data[1:] - start_times_in_bins) / bin_size
    # create a dataframe of binned dxt end events where occupied bins contain
    # the end time and unoccupied bins are populated with NaNs
    end_times_in_bins = cats_end.mul(agg_df["end_time"], axis=0).replace(0, np.nan)
    # create a dataframe of binned dxt end events where occupied bins contain
    # the proportional time spent in the bin
    end_fraction_bin_occupancy = (end_times_in_bins - bin_edge_data[:-1]) / bin_size
    # combine the start/end fractional bin occupancy dataframes by multiplying
    # them together. Fill any missing values with (1)
    combo = start_fraction_bin_occupancy.mul(
        end_fraction_bin_occupancy, fill_value=1, axis=0
    )
    combo.mask(combo > 1, 1, inplace=True)
    # add the start/end dummy variable dataframes
    # and replace any zeros with NaN's
    cats = cats_start.add(cats_end, fill_value=0)
    cats.replace(0, np.nan, inplace=True)
    # for each row (IO event) fill in any empty (NaN) bins
    # between filled bins because those are time spans b/w start
    # and stop events
    # interpolation is pointless when there is
    # a single non-null value in a row
    if sys.version_info.minor < 7:
        cats.interpolate(method="linear", limit_area="inside", axis=1, inplace=True)
    else:
        null_mask = cats.notna().sum(axis=1) > 1
        null_mask = null_mask.loc[null_mask == True].index
        cats_vals_to_interp = pd.DataFrame(cats.iloc[null_mask].values)
        cats_vals_to_interp.interpolate(method="nearest", axis=1, inplace=True)
        cats.iloc[null_mask] = cats_vals_to_interp
    # each time bin containing an event has a 1 in it, otherwise NaN
    # store mask for restoring fully occupied bins
    mask_occ = cats == 2
    # set the fraction of segment per occupied bin
    cats = cats.mul(fraction_of_dxt_segment_in_a_bin, axis=0)
    # start/end in adjacent bins require special treatment
    adjacent_mask = (cats.count(axis=1) == 2) & (cats.sum(axis=1) == 2)
    # multiply them by the true fractions of DXT segments, which
    # may actually be > 1 (i.e., the non-1-capped fractions)
    cats[adjacent_mask] = cats[adjacent_mask].mul(
        true_fraction_of_dxt_segment_in_a_bin[adjacent_mask], axis=0
    )
    # adjust the fractions of the starts/ends for partial occupancy
    # of bins
    cats = cats.mul(combo, fill_value=1, axis=0)
    # start and end are in bin, so restore it
    cats = cats.mask(mask_occ, 1)
    # each full or fractional bin event is now multiplied by
    # the bytes data
    cats = cats.mul(agg_df["length"], axis=0)
    cats.index = agg_df["rank"]
    hmap_df = cats.groupby("rank").sum()
    hmap_df = hmap_df.reindex(index=range(nprocs), fill_value=0.0)
    return hmap_df
