import pytest
import numpy as np
from numpy.testing import assert_allclose, assert_array_equal
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

import darshan
from darshan.log_utils import get_log_path
from darshan.experimental.plots.plot_io_cost import (
    get_by_avg_series,
    get_io_cost_df,
    plot_io_cost,
    combine_hdf5_modules,
)


@pytest.mark.parametrize(
    "logname, expected_df",
    [
        (
            "ior_hdf5_example.darshan",
            pd.DataFrame(
                np.array([
                    [0.0196126699, 0.1342029571533203, 0.0074423551],
                    [0.0196372866, 0.13425052165985107, 0.0475],
                    [0.016869, 0.086689, 0.097160],
                    [0.0, 2.5570392608642578e-05, 0.0],
                ]),
                ["POSIX", "MPIIO", "HDF5", "STDIO"],
                ["Read", "Write", "Meta"],
            ),
        ),
        (
            "sample-badost.darshan",
            pd.DataFrame(
                np.array([
                    [0.0, 33.48587587394286, 0.5547398688504472],
                    [0.011203573201783001, 4.632166e-07, 0.135187],
                ]),
                ["POSIX", "STDIO"],
                ["Read", "Write", "Meta"],
            ),
        ),
    ],
)
def test_get_io_cost_df(logname, expected_df):
    # regression test for `plot_io_cost.get_io_cost_df()`
    report = darshan.DarshanReport(get_log_path(logname))
    actual_df = get_io_cost_df(report=report)
    assert_frame_equal(actual_df, expected_df)


@pytest.mark.parametrize(
    "logname, expected_ylims", [
        (
            "ior_hdf5_example.darshan",
            [0.0, 1.0],
        ),
        (
            "sample-badost.darshan",
            [0.0, 779.0],
        ),
        (
            "dxt.darshan",
            [0.0, 1468.0],
        ),
        (
            "noposix.darshan",
            [0.0, 39212.0],
        ),
        (
            "noposixopens.darshan",
            [0.0, 1110.0],
        ),
    ],
)
def test_plot_io_cost_ylims(logname, expected_ylims):
    # test the y limits for both axes for the IO cost stacked bar graph

    report = darshan.DarshanReport(get_log_path(logname))
    fig = plot_io_cost(report=report)
    for i, ax in enumerate(fig.axes):
        # there are only 2 axes, the first being the "raw" data
        # and the second being the normalized data (percent)
        actual_ylims = ax.get_ylim()
        if i == 0:
            assert_allclose(actual_ylims, expected_ylims)
        else:
            # normalized data is always the same
            assert_allclose(actual_ylims, [0.0, 100.0])

@pytest.mark.parametrize(
    "logname, expected_yticks", [
        (
            "ior_hdf5_example.darshan",
            [0.0, 0.2, 0.4, 0.6, 0.8, 1.0],
        ),
        (
            "sample-badost.darshan",
            [0.0, 155.8, 311.6, 467.4, 623.2, 779.0],
        ),
    ],
)
def test_plot_io_cost_y_ticks_and_labels(logname, expected_yticks):
    # check the y-axis tick marks are at the appropriate
    # locations and the labels are as expected

    # create the expected y-axis tick labels from the y ticks
    expected_yticklabels = [str(i) for i in expected_yticks]

    logpath = get_log_path(logname)
    report = darshan.DarshanReport(logpath)
    fig = plot_io_cost(report=report)
    for i, ax in enumerate(fig.axes):
        # there are only 2 axes, the first being the "raw" data
        # and the second being the normalized data (percent)
        actual_yticks = ax.get_yticks()
        yticklabels = ax.get_yticklabels()
        actual_yticklabels = [tl.get_text() for tl in yticklabels]
        if i == 0:
            assert_allclose(actual_yticks, expected_yticks)
            assert_array_equal(actual_yticklabels, expected_yticklabels)
        else:
            # normalized data always has the same 5 tick labels
            assert_array_equal(actual_yticks, [0, 20, 40, 60, 80, 100])
            assert_array_equal(
                actual_yticklabels,
                ["0%", "20%", "40%", "60%", "80%", "100%"],
            )


@pytest.mark.parametrize("mod_key, input_df, expected_series", [
    (
        # generate a dataframe that has easy-to-calculate average values
        # to check if the averages are being calculated appropriately
        "POSIX",
        pd.DataFrame(
            data=[
                [0, 1, 10, 3],
                [12, 5, 20, 3]
            ],
            columns=[
                "POSIX_F_READ_TIME", "POSIX_F_WRITE_TIME",
                "POSIX_F_META_TIME", "TEST"
            ],
        ),
        pd.Series(
            data=[1.2, .6, 3.0],
            index=["Read", "Write", "Meta"],
        ),
    ),
    (
        # generate a dataframe similar to a shared-record-enabled log
        # where there is a single entry that needs to be divided through
        # by `nprocs`
        "STDIO",
        pd.DataFrame(
            data=[
                [30000, 3000, 300, 10],
            ],
            columns=[
                "STDIO_F_READ_TIME", "STDIO_F_WRITE_TIME",
                "STDIO_F_META_TIME", "TEST"
            ],
        ),
        pd.Series(
            data=[3000.0, 300.0, 30.0],
            index=["Read", "Write", "Meta"],
        ),
    ),
    (
        # combine 2 previous cases to check if
        # calculations are being done appropriately
        "MPIIO",
        pd.DataFrame(
            data=[
                [0, 1, 10, 3],
                [12, 5, 20, 3],
                [30000, 3000, 300, 10],
            ],
            columns=[
                "MPIIO_F_READ_TIME", "MPIIO_F_WRITE_TIME",
                "MPIIO_F_META_TIME", "TEST"
            ],
        ),
        pd.Series(
            data=[3001.2, 300.6, 33.0],
            index=["Read", "Write", "Meta"],
        ),
    )
])
def test_get_by_avg_series(mod_key, input_df, expected_series):
    # unit test for `plot_io_cost.get_by_avg_series`
    actual_series = get_by_avg_series(df=input_df, mod_key=mod_key, nprocs=10)
    assert_series_equal(actual_series, expected_series)


@pytest.mark.parametrize(
    "filename, expected_df",
    [
        (
            "nonmpi_dxt_anonymized.darshan",
            pd.DataFrame(
                np.array([
                    [0.281718, 0.504260, 0.170138],
                    [0.232386, 0.165982, 0.072751],
                ]),
                ["POSIX", "STDIO"],
                ["Read", "Write", "Meta"],
            ),
        ),
    ])
def test_issue_590(filename, expected_df):
    # regression test for issue #590
    # see: https://github.com/darshan-hpc/darshan/issues/590
    log_path = get_log_path(filename)
    report = darshan.DarshanReport(log_path)
    actual_df = get_io_cost_df(report=report)
    assert_frame_equal(actual_df, expected_df)


@pytest.mark.parametrize(
    "input_df, expected_df",
    [
        # if input dataframe does not contain HDF5 module
        # it should remain unchanged
        (
            pd.DataFrame([[10.0, 20.0, 30.0]], index=["POSIX"]),
            pd.DataFrame([[10.0, 20.0, 30.0]], index=["POSIX"]),
        ),
        # for cases where only "H5F" data is present, it should
        # effectively get renamed to "HDF5"
        (
            pd.DataFrame([[10.0, 20.0, 30.0], [0.1, 0.2, 0.3]], index=["POSIX", "H5F"]),
            pd.DataFrame([[10.0, 20.0, 30.0], [0.1, 0.2, 0.3]], index=["POSIX", "HDF5"]),
        ),
        # for cases with both HDF5 modules, the resultant HDF5 entry
        # should be the sum of the `H5F` and `H5D` rows
        (
            pd.DataFrame(
                [
                    [10.0, 20.0, 30.0],
                    [0.1, 0.2, 0.3],
                    [0.9, 0.8, 0.7],
                ],
                index=["POSIX", "H5F", "H5D"],
            ),
            pd.DataFrame([[10.0, 20.0, 30.0], [1.0, 1.0, 1.0]], index=["POSIX", "HDF5"]),
        ),
    ])
def test_combine_hdf5_modules(input_df, expected_df):
    # `plot_io_cost.combine_hdf5_modules()` unit test

    # add the proper column names to the input and expected dataframes
    for df in (input_df, expected_df):
        df.columns = ["Read", "Write", "Meta"]

    actual_df = combine_hdf5_modules(input_df)

    # check actual and expected dataframes are identical
    assert_frame_equal(actual_df, expected_df)
