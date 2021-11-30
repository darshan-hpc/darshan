import pytest
import numpy as np
from numpy.testing import assert_allclose, assert_array_equal
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

import darshan
from darshan.experimental.plots.plot_io_cost import (
    get_by_avg_series,
    get_io_cost_df,
    plot_io_cost,
)

@pytest.mark.parametrize(
    "report, mod, expected_df",
    [
        (
            darshan.DarshanReport("examples/example-logs/ior_hdf5_example.darshan"),
            "POSIX",
            pd.DataFrame(
                np.array([[0.0196126699, 0.134203, 0.0074423551]]),
                ["by-average"],
                ["Read", "Write", "Meta"],
            ),
        ),
        (
            darshan.DarshanReport("examples/example-logs/ior_hdf5_example.darshan"),
            "MPI-IO",
            pd.DataFrame(
                np.array([[0.0196372866, 0.134251, 0.0475]]),
                ["by-average"],
                ["Read", "Write", "Meta"],
            ),
        ),
        (
            darshan.DarshanReport("examples/example-logs/ior_hdf5_example.darshan"),
            "STDIO",
            pd.DataFrame(
                np.array([[0.0, 0.0001022815, 0.0]]),
                ["by-average"],
                ["Read", "Write", "Meta"],
            ),
        ),
        (
            darshan.DarshanReport("examples/example-logs/sample-badost.darshan"),
            "POSIX",
            pd.DataFrame(
                np.array([[0.0, 33.48587587394286, 0.5547398688504472]]),
                ["by-average"],
                ["Read", "Write", "Meta"],
            ),
        ),
        (
            darshan.DarshanReport("examples/example-logs/sample-badost.darshan"),
            "STDIO",
            pd.DataFrame(
                np.array([[0.0037345244005943337, 1.544055218497912e-07, 0.045062407424362995]]),
                ["by-average"],
                ["Read", "Write", "Meta"],
            ),
        ),
    ],
)
def test_get_io_cost_df(report, mod, expected_df):
    # regression test for `plot_io_cost.get_io_cost_df()`
    actual_df = get_io_cost_df(report=report, mod_key=mod)
    assert_frame_equal(actual_df, expected_df)


@pytest.mark.parametrize(
    "report, expected_ylims, mods", [
        (
            darshan.DarshanReport("examples/example-logs/ior_hdf5_example.darshan"),
            [0.0, 1.0],
            ["POSIX", "MPI-IO", "STDIO"],
        ),
        (
            darshan.DarshanReport("examples/example-logs/sample-badost.darshan"),
            [0.0, 800.0],
            ["POSIX", "STDIO"],
        ),
    ],
)
def test_plot_io_cost_ylims(report, expected_ylims, mods):
    # test the y limits for both axes for the IO cost stacked bar graph

    for mod in mods:
        fig = plot_io_cost(report=report, mod_key=mod)
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
    "report, expected_yticks, mods", [
        (
            darshan.DarshanReport("examples/example-logs/ior_hdf5_example.darshan"),
            [0.0, 0.2, 0.4, 0.6, 0.8, 1.0],
            ["POSIX", "MPI-IO", "STDIO"],
        ),
        (
            darshan.DarshanReport("examples/example-logs/sample-badost.darshan"),
            [0, 160, 320, 480, 640, 800],
            ["POSIX", "STDIO"],
        ),
    ],
)
def test_plot_io_cost_y_ticks_and_labels(
        report,
        expected_yticks,
        mods
    ):
    # check the y-axis tick marks are at the appropriate
    # locations and the labels are as expected

    # create the expected y-axis tick labels from the y ticks
    expected_yticklabels = [str(i) for i in expected_yticks]

    for mod in mods:
        fig = plot_io_cost(report=report, mod_key=mod)
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

def test_plot_io_cost_unsupported_modules():
    # test that using an unsupported module raises the appropriate error
    report = darshan.DarshanReport("examples/example-logs/ior_hdf5_example.darshan")
    with pytest.raises(NotImplementedError) as err:
        plot_io_cost(report=report, mod_key="LUSTRE")
        assert "module is not supported." in str(err)

@pytest.mark.parametrize("mod_key, input_df, expected_series", [
    (
        # generate a dataframe that has easy-to-calculate average values
        # to check if the averages are being calculated appropriately
        "POSIX",
        pd.DataFrame(
            data=[
                [0, 0, 1, 10, 3],
                [1, 12, 5, 20, 3]
            ],
            columns=[
                "rank", "POSIX_F_READ_TIME", "POSIX_F_WRITE_TIME",
                "POSIX_F_META_TIME", "TEST"
            ],
        ),
        pd.Series(
            data=[6.0, 3.0, 15.0],
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
                [-1, 30000, 3000, 300, 10],
            ],
            columns=[
                "rank", "STDIO_F_READ_TIME", "STDIO_F_WRITE_TIME",
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
                [0, 0, 1, 10, 3],
                [1, 12, 5, 20, 3],
                [-1, 30000, 3000, 300, 10],
            ],
            columns=[
                "rank", "MPIIO_F_READ_TIME", "MPIIO_F_WRITE_TIME",
                "MPIIO_F_META_TIME", "TEST"
            ],
        ),
        pd.Series(
            data=[1004.0, 102.0, 20.0],
            index=["Read", "Write", "Meta"],
        ),
    )
])
def test_get_by_avg_series(mod_key, input_df, expected_series):
    # unit test for `plot_io_cost.get_by_avg_series`
    actual_series = get_by_avg_series(df=input_df, mod_key=mod_key, nprocs=10)
    assert_series_equal(actual_series, expected_series)
