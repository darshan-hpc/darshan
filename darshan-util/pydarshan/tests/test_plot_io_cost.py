import pytest
import numpy as np
from numpy.testing import assert_allclose, assert_array_equal
import pandas as pd
from pandas.testing import assert_frame_equal # type: ignore

import darshan
from darshan.experimental.plots.plot_io_cost import get_io_cost_df, plot_io_cost


@pytest.mark.parametrize(
    "report, mod, expected_df",
    [
        (
            darshan.DarshanReport("examples/example-logs/ior_hdf5_example.darshan"),
            "POSIX",
            pd.DataFrame(
                np.array([[0.0196126699, 0.134203, 0.0074423551]]),
                ["Avg. Operation Time"],
                ["Read", "Write", "Meta"],
            ),
        ),
        (
            darshan.DarshanReport("examples/example-logs/ior_hdf5_example.darshan"),
            "MPI-IO",
            pd.DataFrame(
                np.array([[0.0196372866, 0.134251, 0.0475]]),
                ["Avg. Operation Time"],
                ["Read", "Write", "Meta"],
            ),
        ),
        (
            darshan.DarshanReport("examples/example-logs/ior_hdf5_example.darshan"),
            "STDIO",
            pd.DataFrame(
                np.array([[0.0, 0.0001022815, 0.0]]),
                ["Avg. Operation Time"],
                ["Read", "Write", "Meta"],
            ),
        ),
        (
            darshan.DarshanReport("examples/example-logs/sample.darshan"),
            "POSIX",
            pd.DataFrame(
                np.array([[0.0, 49.022266, 0.005518]]),
                ["Avg. Operation Time"],
                ["Read", "Write", "Meta"],
            ),
        ),
        (
            darshan.DarshanReport("examples/example-logs/sample.darshan"),
            "MPI-IO",
            pd.DataFrame(
                np.array([[0.0, 49.022411, 0.023936]]),
                ["Avg. Operation Time"],
                ["Read", "Write", "Meta"],
            ),
        ),
    ],
)
def test_get_io_cost_df(report, mod, expected_df):
    # regression test for `plot_io_cost.get_io_cost_df()`
    actual_df = get_io_cost_df(report=report, mod_key=mod)
    assert_frame_equal(actual_df, expected_df)


@pytest.mark.parametrize("mod", ["POSIX", "MPI-IO", "STDIO"])
@pytest.mark.parametrize(
    "report, expected_ylims", [
        (
            darshan.DarshanReport("examples/example-logs/ior_hdf5_example.darshan"),
            [0.0, 1.0],
        ),
        (
            darshan.DarshanReport("examples/example-logs/sample.darshan"),
            [0.0, 120.0],
        ),
    ],
)
def test_plot_io_cost_ylims(report, expected_ylims, mod):
    # test the y limits for both axes for the IO cost stacked bar graph
    fig = plot_io_cost(report=report, mod_key=mod)
    for i, ax in enumerate(fig.axes):
        # there are only 2 axes, the first being the "raw" data
        # and the second being the normalized data (percent)
        actual_ylims = ax.get_ylim()
        if i == 0:
            assert_array_equal(actual_ylims, expected_ylims)
        else:
            # normalized data is always the same
            assert_array_equal(actual_ylims, [0.0, 100.0])

@pytest.mark.parametrize("mod", ["POSIX", "MPI-IO", "STDIO"])
@pytest.mark.parametrize(
    "report, expected_yticks, expected_yticklabels", [
        (
            darshan.DarshanReport("examples/example-logs/ior_hdf5_example.darshan"),
            [0.0, 0.2, 0.4, 0.6, 0.8, 1.0],
            ["0.0", "0.2", "0.4", "0.6", "0.8", "1.0"],
        ),
        (
            darshan.DarshanReport("examples/example-logs/sample.darshan"),
            [0, 24, 48, 72, 96, 120],
            ["0", "24", "48", "72", "96", "120"],
        ),
    ],
)
def test_plot_io_cost_y_ticks_and_labels(
        report,
        expected_yticks,
        expected_yticklabels,
        mod
    ):
    # check the y-axis tick marks are at the appropriate
    # locations and the labels are as expected
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
