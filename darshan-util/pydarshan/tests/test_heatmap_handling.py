import pytest
import numpy as np
from numpy.testing import assert_array_equal
import pandas as pd

import darshan
from darshan.experimental.plots import heatmap_handling


@pytest.fixture(scope="function")
def dict_list():
    # mock data structure created to test `heatmap_handling.get_rd_wr_dfs()`
    # generates a list of python dictionaries which each contain dataframes
    # for read/write events

    # create a small data set to store in a dataframe
    n_data_points = 10
    start_arr = np.linspace(0, 3, n_data_points)
    end_arr = start_arr + 0.5
    len_arr = np.arange(1, n_data_points + 1)
    offset_arr = np.arange(n_data_points) + n_data_points

    # use the data above to create a base dataframe to use
    # for creating the dictionary list
    base_df = pd.DataFrame(
        data=np.column_stack((start_arr, end_arr, len_arr, offset_arr)),
        columns=["length", "start_time", "end_time", "offset"],
    )

    # initialize an empty list for storing dictionaries (containing dataframes)
    dict_list = []
    # only iterate 3 times to keep data structure simple
    for i in range(1, 4):
        # create a dictionary with a rank index, a read segment (dataframe)
        # and a write segment (dataframe)
        _dict = {}
        # assign the rank using the index
        _dict["rank"] = i
        # for the read segment, multiply the dataframe data by the index so
        # each segment has distinguishable values
        _dict["read_segments"] = i * base_df
        # do the same for the write segments, but only for the middle iteration
        if i == 2:
            # again, assign a modified dataframe for uniqueness
            _dict["write_segments"] = i * (base_df + 10)
        else:
            # assign an empty dataframe for first and last iterations
            _dict["write_segments"] = pd.DataFrame()
        dict_list.append(_dict)

    return dict_list


@pytest.fixture(scope="function")
def dict_list_no_writes():
    # Similar to `dict_list`, this fixture is targeted at creating a
    # dictionary list for `heatmap_handling.get_rd_wr_dfs()` such that
    # the returned `write_df` is an empty dataframe

    # create a small data set to store in a dataframe
    n_data_points = 10
    start_arr = np.linspace(0, 3, n_data_points)
    end_arr = start_arr + 0.5
    len_arr = np.arange(1, n_data_points + 1)
    offset_arr = np.arange(n_data_points) + n_data_points

    # use the data above to create a base dataframe to use
    # for creating the dictionary list
    base_df = pd.DataFrame(
        data=np.column_stack((start_arr, end_arr, len_arr, offset_arr)),
        columns=["length", "start_time", "end_time", "offset"],
    )

    # initialize an empty list for storing dictionaries (containing dataframes)
    dict_list = []
    # only iterate 3 times to keep data structure simple
    for i in range(1, 4):
        # create a dictionary with a rank index, a read segment (dataframe)
        # and a write segment (dataframe)
        _dict = {}
        # assign the rank using the index
        _dict["rank"] = i
        # for the read segment, multiply the dataframe data by the index so
        # each segment has distinguishable values
        _dict["read_segments"] = i * base_df
        # for the write segments assign an empty dataframe
        _dict["write_segments"] = pd.DataFrame()
        dict_list.append(_dict)

    return dict_list


def test_get_rd_wr_dfs(dict_list):
    read_df, write_df = heatmap_handling.get_rd_wr_dfs(dict_list=dict_list)
    # check that we get the correct data shape after
    # combining the read/write dataframes
    assert read_df.shape == (30, 4)
    assert write_df.shape == (10, 4)

    # check that the correct column names are generated. We expect "offset"
    # to be missing and "rank" to be added
    df_keys = ["length", "start_time", "end_time", "rank"]
    assert list(read_df.columns) == df_keys
    assert list(write_df.columns) == df_keys

    # verify the correct rank values are displayed. Since a read segment was
    # generated for each iteration, there should be ranks 1-3, and since
    # a write segment was only generated for the middle iteration we should
    # only get 2
    assert_array_equal(np.unique(read_df["rank"].values), [1, 2, 3])
    assert_array_equal(np.unique(write_df["rank"].values), [2])

    # since we ignore the original row indices in the individual dataframes
    # make sure we get the correct indices (0-29) and (0-9) for read and write,
    # respectively
    assert_array_equal(read_df.index, np.arange(30))
    assert_array_equal(write_df.index, np.arange(10))


def test_get_rd_wr_dfs_no_write(dict_list_no_writes):
    # based on `test_get_rd_wr_dfs`

    read_df, write_df = heatmap_handling.get_rd_wr_dfs(dict_list=dict_list_no_writes)
    # since there are no write dataframes we should get an empty write dataframe
    assert write_df.empty
    # check that we get the correct data shape after
    # combining the read dataframes
    assert read_df.shape == (30, 4)

    # check that the correct column names are generated. We expect "offset"
    # to be missing and "rank" to be added
    df_keys = ["length", "start_time", "end_time", "rank"]
    assert list(read_df.columns) == df_keys

    # verify the correct rank values are displayed. Since a read segment was
    # generated for each iteration, there should be ranks 1-3
    assert_array_equal(np.unique(read_df["rank"].values), [1, 2, 3])

    # since we ignore the original row indices in the individual dataframes
    # make sure we get the correct indices (0-29) for read
    assert_array_equal(read_df.index, np.arange(30))


@pytest.mark.parametrize(
    # all 3 test cases are based on the outputs for
    # `tests/input/sample-dxt-simple.darshan`, which only has write data
    "ops, expected_df_dict",
    [
        (
            # check the result using both operations
            ["read", "write"],
            {
                "DXT_POSIX": {
                    "read": pd.DataFrame(),
                    "write": pd.DataFrame(
                        columns=["length", "start_time", "end_time", "rank"],
                        data=np.array(
                            [
                                [40, 0.10337884305045009, 0.10338771319948137, 0],
                                [4000, 0.10421665315516293, 0.10423145908862352, 0],
                            ]
                        ),
                    ),
                },
            },
        ),
        (
            # check the result for only the "read" operation, should be empty
            ["read"],
            {"DXT_POSIX": {"read": pd.DataFrame()}},
        ),
        (
            # the results for only checking the "write" data should be the same
            # as checking both operations
            ["write"],
            {
                "DXT_POSIX": {
                    "write": pd.DataFrame(
                        columns=["length", "start_time", "end_time", "rank"],
                        data=np.array(
                            [
                                [40, 0.10337884305045009, 0.10338771319948137, 0],
                                [4000, 0.10421665315516293, 0.10423145908862352, 0],
                            ]
                        ),
                    ),
                },
            },
        ),
    ],
)
def test_get_single_df_dict(expected_df_dict, ops):
    # regression test for `heatmap_handling.get_single_df_dict()`

    report = darshan.DarshanReport("tests/input/sample-dxt-simple.darshan")

    actual_df_dict = heatmap_handling.get_single_df_dict(
        report=report, mods=["DXT_POSIX"], ops=ops
    )

    # make sure we get the same key(s) ("DXT_POSIX")
    assert actual_df_dict.keys() == expected_df_dict.keys()
    # make sure we get the correct key(s)
    assert actual_df_dict["DXT_POSIX"].keys() == expected_df_dict["DXT_POSIX"].keys()
    # also check that we only get the key(s) we requested
    assert list(actual_df_dict["DXT_POSIX"].keys()) == ops

    if "read" in ops:
        # for the read case, check that we get an empty dataframe
        assert actual_df_dict["DXT_POSIX"]["read"].empty

    if "write" in ops:
        # check that we get the same column names
        assert_array_equal(
            actual_df_dict["DXT_POSIX"]["write"].columns,
            expected_df_dict["DXT_POSIX"]["write"].columns,
        )

        # verify the returned values are the same
        assert_array_equal(
            actual_df_dict["DXT_POSIX"]["write"].values,
            expected_df_dict["DXT_POSIX"]["write"].values,
        )


@pytest.mark.parametrize(
    "mods, ops, expected_agg_data",
    [
        # all 3 test cases are based on the outputs for
        # `tests/input/sample-dxt-simple.darshan`, which only has write data
        (
            ["DXT_POSIX"],
            ["read", "write"],
            np.array(
                [
                    [40, 0.10337884305045009, 0.10338771319948137, 0],
                    [4000, 0.10421665315516293, 0.10423145908862352, 0],
                ]
            ),
        ),
        # for "read" case input None since there is no data to compare
        (["DXT_POSIX"], ["read"], None),
        (["DXT_MPIIO"], ["read"], None),
        (
            ["DXT_POSIX"],
            ["write"],
            np.array(
                [
                    [40, 0.10337884305045009, 0.10338771319948137, 0],
                    [4000, 0.10421665315516293, 0.10423145908862352, 0],
                ]
            ),
        ),
    ],
)
def test_get_aggregate_data(expected_agg_data, mods, ops):
    # regression test for `heatmap_handling.get_aggregate_data()`

    report = darshan.DarshanReport("tests/input/sample-dxt-simple.darshan")

    if ops == ["read"]:
        if mods == ["DXT_POSIX"]:
            expected_msg = (
                "No data available for selected module\\(s\\) and operation\\(s\\)."
            )
            with pytest.raises(ValueError, match=expected_msg):
                # expect an error because there are no read segments
                # in sample-dxt-simple.darshan
                actual_agg_data = heatmap_handling.get_aggregate_data(
                    report=report, mods=mods, ops=ops
                )
        elif mods == ["DXT_MPIIO"]:
            with pytest.raises(KeyError, match="'DXT_POSIX'"):
                # expect an error because there are no read segments
                # in sample-dxt-simple.darshan
                actual_agg_data = heatmap_handling.get_aggregate_data(
                    report=report, mods=mods, ops=ops
                )
    else:
        actual_agg_data = heatmap_handling.get_aggregate_data(
            report=report, mods=mods, ops=ops
        )
        # for other cases, make sure the value arrays are identically valued
        assert_array_equal(actual_agg_data.values, expected_agg_data)


@pytest.mark.parametrize(
    "filepath, xbins, ops, expected_hmap_data",
    [
        # iterate over 3 different darshan logs, various bin counts, and
        # combinations of operations, checking the heatmap data array
        # output for each case.
        # For `sample-dxt-simple.darshan` the selected
        # operations are not changed because there is no "read" data
        (
            "tests/input/sample-dxt-simple.darshan",
            1,
            ["read", "write"],
            np.array([[4040]]),
        ),
        (
            "tests/input/sample-dxt-simple.darshan",
            4,
            ["read", "write"],
            np.array([[0, 0, 0, 4040]]),
        ),
        (
            "tests/input/sample-dxt-simple.darshan",
            10,
            ["read", "write"],
            np.array([[0, 0, 0, 0, 0, 0, 0, 0, 0, 4040]]),
        ),
        # `dxt.darshan` is complex enough to warrant changing the
        # selected operations
        ("examples/example-logs/dxt.darshan", 1, ["read"], np.array([[22517726]])),
        (
            "examples/example-logs/dxt.darshan",
            4,
            ["read"],
            np.array([[10214363, 0, 8070137, 4233226]]),
        ),
        (
            "examples/example-logs/dxt.darshan",
            10,
            ["read"],
            np.array([[10214363, 0, 0, 0, 0, 0, 8070137, 0, 0, 4233226]]),
        ),
        ("examples/example-logs/dxt.darshan", 1, ["write"], np.array([[13021781]])),
        (
            "examples/example-logs/dxt.darshan",
            4,
            ["write"],
            np.array([[4381, 0, 10915913, 2101487]]),
        ),
        (
            "examples/example-logs/dxt.darshan",
            10,
            ["write"],
            np.array([[4381, 0, 0, 0, 0, 0, 10915913, 0, 0, 2101487]]),
        ),
        (
            "examples/example-logs/dxt.darshan",
            1,
            ["read", "write"],
            np.array([[35539507]]),
        ),
        (
            "examples/example-logs/dxt.darshan",
            4,
            ["read", "write"],
            np.array([[10218744, 0, 18986050, 6334713]]),
        ),
        (
            "examples/example-logs/dxt.darshan",
            10,
            ["read", "write"],
            np.array([[10218744, 0, 0, 0, 0, 0, 18986050, 0, 0, 6334713]]),
        ),
        # `ior_hdf5_example.darshan` is the only log with multiple ranks (4),
        # so it also gets different operation combinations
        (
            "examples/example-logs/ior_hdf5_example.darshan",
            1,
            ["read"],
            np.array([[1051088], [1050472], [1050472], [1050472]]),
        ),
        (
            "examples/example-logs/ior_hdf5_example.darshan",
            10,
            ["read"],
            np.array(
                [
                    [0, 0, 0, 0, 0, 0, 0, 0, 1051088, 0],
                    [0, 0, 0, 0, 0, 0, 0, 0, 107988.68001937, 942483.31998063],
                    [0, 0, 0, 0, 0, 0, 0, 0, 1050472, 0],
                    [0, 0, 0, 0, 0, 0, 0, 0, 1050472, 0],
                ]
            ),
        ),
        (
            "examples/example-logs/ior_hdf5_example.darshan",
            1,
            ["write"],
            np.array(
                [
                    [1048808],
                    [1049240],
                    [1048848],
                    [1048904],
                ]
            ),
        ),
        (
            "examples/example-logs/ior_hdf5_example.darshan",
            10,
            ["write"],
            np.array(
                [
                    [
                        0,
                        808091.3650729951,
                        41175.65189967951,
                        41175.6518996795,
                        41175.6518996795,
                        41175.65189967952,
                        41175.65189967947,
                        34606.37542860738,
                        0,
                        232,
                    ],
                    [
                        0,
                        288603.1671221,
                        40689.00335231,
                        40689.00335231,
                        40689.00335231,
                        40689.00335231,
                        40689.00335231,
                        556527.81611634,
                        0,
                        664,
                    ],
                    [
                        0,
                        548158.6819154,
                        41120.86590861,
                        41120.86590861,
                        41120.86590861,
                        41120.86590861,
                        41120.86590861,
                        63152.77796803,
                        149027.37037175,
                        82904.84020176,
                    ],
                    [0, 1048576, 0, 0, 0, 0, 0, 0, 0, 328],
                ]
            ),
        ),
        (
            "examples/example-logs/ior_hdf5_example.darshan",
            1,
            ["read", "write"],
            np.array(
                [
                    [2099896],
                    [2099712],
                    [2099320],
                    [2099376],
                ]
            ),
        ),
        (
            "examples/example-logs/ior_hdf5_example.darshan",
            10,
            ["read", "write"],
            np.array(
                [
                    [
                        0,
                        827385.0734944909,
                        50822.506110427385,
                        50822.50611042739,
                        50822.50611042739,
                        50822.50611042737,
                        17900.90206379957,
                        30.830417529761142,
                        1051289.1695824703,
                        0,
                    ],
                    [
                        0,
                        307668.84624124144,
                        50221.842911882275,
                        50221.84291188228,
                        50221.84291188228,
                        50221.84291188226,
                        540019.7821112294,
                        418.49887595643366,
                        108234.18114341467,
                        942483.3199806289,
                    ],
                    [
                        0,
                        567426.7192208751,
                        50754.88456134755,
                        50754.88456134756,
                        50754.88456134756,
                        50754.884561347535,
                        141846.72456250372,
                        136490.77078712088,
                        1050536.24718411,
                        0,
                    ],
                    [
                        0,
                        1048576,
                        0,
                        0,
                        0,
                        0,
                        0,
                        251.0322619047617,
                        1050548.9677380952,
                        0,
                    ],
                ]
            ),
        ),
    ],
)
def test_get_heatmap_data(
    filepath,
    expected_hmap_data,
    xbins,
    ops,
):
    # regression test for `heatmap_handling.get_heatmap_data()`

    # generate the report and use it to obtain the aggregated data
    report = darshan.DarshanReport(filepath)
    agg_df = heatmap_handling.get_aggregate_data(
        report=report, mods=["DXT_POSIX"], ops=ops
    )
    # run the aggregated data through the heatmap data code
    actual_hmap_data = heatmap_handling.get_heatmap_data(agg_df=agg_df, xbins=xbins)

    if filepath == "tests/input/sample-dxt-simple.darshan":
        # check the data is conserved
        assert actual_hmap_data.sum() == 4040
        # make sure the output array is the correct shape
        assert actual_hmap_data.shape == (1, xbins)
        # make sure the output data contains identical values
        assert_array_equal(actual_hmap_data, expected_hmap_data)

    elif filepath == "examples/example-logs/dxt.darshan":
        # make sure the output array is the correct shape
        assert actual_hmap_data.shape == (1, xbins)
        # make sure the output data contains identical values
        assert_array_equal(actual_hmap_data, expected_hmap_data)

        # for each combination of operations, make sure the sum is correct
        if len(ops) == 2:
            assert actual_hmap_data.sum() == 35539507
        elif ops[0] == "read":
            assert actual_hmap_data.sum() == 22517726
        elif ops[0] == "write":
            assert actual_hmap_data.sum() == 13021781

    elif filepath == "examples/example-logs/ior_hdf5_example.darshan":
        # make sure the output array is the correct shape
        assert actual_hmap_data.shape == (4, xbins)
        # make sure the output data contains identical values
        assert np.allclose(actual_hmap_data, expected_hmap_data)

        # for each combination of operations, make sure the sum is correct
        if len(ops) == 2:
            assert actual_hmap_data.sum() == 8398304
        elif ops[0] == "read":
            assert actual_hmap_data.sum() == 4202504
        elif ops[0] == "write":
            assert actual_hmap_data.sum() == 4195800
