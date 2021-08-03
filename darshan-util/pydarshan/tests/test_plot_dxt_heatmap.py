import pytest
import numpy as np
from numpy.testing import assert_array_equal, assert_allclose
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

import darshan
from darshan.experimental.plots import heatmap_handling, plot_dxt_heatmap


@pytest.fixture(scope="function")
def jointgrid():
    # generates a `sns.JointGrid` object
    jgrid = sns.jointplot(kind="hist", bins=(4, 100))
    return jgrid


@pytest.mark.parametrize(
    "filepath, n_xlabels, expected_xticks, expected_xticklabels",
    [
        ("examples/example-logs/ior_hdf5_example.darshan", 2, [0.0, 1.0], [0.0, 0.29]),
        (
            "examples/example-logs/ior_hdf5_example.darshan",
            4,
            [0.0, 0.4, 0.6, 1.0],
            [0.0, 0.1, 0.19, 0.29],
        ),
        (
            "examples/example-logs/ior_hdf5_example.darshan",
            6,
            [0.0, 0.2, 0.4, 0.6, 0.8, 1.0],
            [0.0, 0.06, 0.11, 0.17, 0.23, 0.29],
        ),
        (
            "examples/example-logs/ior_hdf5_example.darshan",
            10,
            [0.0, 1 / 9, 2 / 9, 3 / 9, 4 / 9, 5 / 9, 6 / 9, 7 / 9, 8 / 9, 1.0],
            [
                0.0,
                0.03,
                0.06,
                0.1,
                0.13,
                0.16,
                0.19,
                0.22,
                0.25,
                0.29,
            ],
        ),
        ("examples/example-logs/dxt.darshan", 2, [0.0, 1.0], [0, 1468]),
        (
            "examples/example-logs/dxt.darshan",
            4,
            [0.0, 0.4, 0.6, 1.0],
            [0, 489, 978, 1468],
        ),
        (
            "examples/example-logs/dxt.darshan",
            6,
            [0.0, 0.2, 0.4, 0.6, 0.8, 1.0],
            [0, 293, 587, 880, 1174, 1468],
        ),
        (
            "examples/example-logs/dxt.darshan",
            10,
            [0.0, 1 / 9, 2 / 9, 3 / 9, 4 / 9, 5 / 9, 6 / 9, 7 / 9, 8 / 9, 1.0],
            [0, 163, 326, 489, 652, 815, 978, 1141, 1304, 1468],
        ),
        ("tests/input/sample-dxt-simple.darshan", 2, [0.0, 1.0], [0.0, 0.1]),
        (
            "tests/input/sample-dxt-simple.darshan",
            4,
            [0.0, 0.4, 0.6, 1.0],
            [0.0, 0.03, 0.07, 0.1],
        ),
        (
            "tests/input/sample-dxt-simple.darshan",
            6,
            [0.0, 0.2, 0.4, 0.6, 0.8, 1.0],
            [0.0, 0.02, 0.04, 0.06, 0.08, 0.1],
        ),
        (
            "tests/input/sample-dxt-simple.darshan",
            10,
            [0.0, 1 / 9, 2 / 9, 3 / 9, 4 / 9, 5 / 9, 6 / 9, 7 / 9, 8 / 9, 1.0],
            [
                0.0,
                0.01,
                0.02,
                0.03,
                0.05,
                0.06,
                0.07,
                0.08,
                0.09,
                0.1,
            ],
        ),
        (None, 2, [0.0, 1.0], [0.0, 1.0]),
    ],
)
def test_set_x_axis_ticks_and_labels(
    filepath,
    n_xlabels,
    expected_xticks,
    expected_xticklabels,
    jointgrid,
):
    # make sure the x-axis ticks and
    # tick labels are generated appropriately

    if filepath is None:
        # don't have any data sets with a max time between 1 and 10, so
        # create a synthetic one here
        data = [[4, 1.03378843, 1.03387713, 0], [4000, 1.04216653, 1.04231459, 0]]
        cols = ["length", "start_time", "end_time", "rank"]
        agg_df = pd.DataFrame(data=data, columns=cols)

    else:
        # for all other data sets just load the data from the log file
        report = darshan.DarshanReport(filepath)
        agg_df = heatmap_handling.get_aggregate_data(
            report=report, mods=["DXT_POSIX"], ops=["read", "write"]
        )

    # set the x-axis ticks and tick labels
    plot_dxt_heatmap.set_x_axis_ticks_and_labels(
        jointgrid=jointgrid, agg_df=agg_df, n_xlabels=n_xlabels
    )

    # collect the actual x-axis tick labels
    actual_xticks = jointgrid.ax_joint.get_xticks()
    actual_xticklabels = [tl.get_text() for tl in jointgrid.ax_joint.get_xticklabels()]
    actual_xticklabels = np.asarray(actual_xticklabels, dtype=float)

    # make sure the figure object gets closed
    plt.close()

    # verify the actual ticks/labels match the expected
    assert_allclose(actual_xticks, expected_xticks, atol=1e-14, rtol=1e-17)
    assert_allclose(actual_xticklabels, expected_xticklabels, atol=1e-14, rtol=1e-17)


@pytest.mark.parametrize(
    "n_ylabels, expected_yticks",
    [
        # try the minimum number of tick marks
        (2, [0, 1]),
        # request more y-axis tick marks than are available
        (10, [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]),
        (4, [0, 0.4, 0.6, 1]),
        (6, [0, 0.2, 0.4, 0.6, 0.8, 1]),
    ],
)
def test_get_y_axis_ticks(n_ylabels, expected_yticks, jointgrid):
    # test some edge cases for the
    # y-axis tick mark location code

    # get the y-axis tick mark locations
    actual_yticks = plot_dxt_heatmap.get_y_axis_ticks(
        ax=jointgrid.ax_joint, n_ylabels=n_ylabels
    )

    # close the figure object
    plt.close()

    # make sure the actual tick mark locations match the expected
    assert_allclose(actual_yticks, expected_yticks, atol=1e-14, rtol=1e-17)


@pytest.mark.parametrize(
    "n_ylabels, unique_ranks, expected_yticklabels",
    [
        (2, np.arange(10), ["0", "9"]),
        # request more y-axis tick labels than are available
        (10, np.arange(4), ["0", "1", "2", "3"]),
        (4, np.arange(10), ["0", "3", "6", "9"]),
        (6, np.arange(10), ["0", "2", "4", "5", "7", "9"]),
        (8, np.arange(10), ["0", "1", "3", "4", "5", "6", "8", "9"]),
        (4, np.arange(100), ["0", "32", "64", "96"]),
        (8, np.arange(100), ["0", "12", "28", "40", "56", "68", "84", "96"]),
        (6, np.arange(1000), ["0", "192", "384", "608", "800", "992"]),
    ],
)
def test_get_y_axis_tick_labels(
    n_ylabels,
    unique_ranks,
    expected_yticklabels,
):
    # test some specific cases for
    # the y-axis tick mark label function

    # x-axis bins are arbitrary, y-axis bins are the number of unique ranks
    xbins = 50
    ybins = unique_ranks.size
    bins = [xbins, ybins]

    # generate a jointgrid object using the bin dimensions above
    jointgrid = sns.jointplot(kind="hist", bins=bins)

    # generate array of ones and put a heatmap as the main 'joint' plot
    data = np.ones((ybins, xbins))
    sns.heatmap(data, ax=jointgrid.ax_joint)

    # retrieve the actual y-axis tick labels
    actual_yticklabels = plot_dxt_heatmap.get_y_axis_tick_labels(
        ax=jointgrid.ax_joint, n_ylabels=n_ylabels
    )

    # make sure the figure object gets closed
    plt.close()

    # make sure the actual tick mark labels match the expected
    assert_array_equal(actual_yticklabels, expected_yticklabels)


@pytest.mark.parametrize(
    "filepath, n_ylabels, expected_yticks, expected_yticklabels",
    [
        # check that if less y-axis labels are input, we get the
        # correct number of labels back
        ("examples/example-logs/ior_hdf5_example.darshan", 2, [0.5, 3.5], [0.0, 3.0]),
        (
            "examples/example-logs/ior_hdf5_example.darshan",
            4,
            [0.5, 1.5, 2.5, 3.5],
            [0.0, 1.0, 2.0, 3.0],
        ),
        # check that if we input more y-axis labels than available,
        # we just get back the maximum number available
        (
            "examples/example-logs/ior_hdf5_example.darshan",
            8,
            [0.5, 1.5, 2.5, 3.5],
            [0.0, 1.0, 2.0, 3.0],
        ),
        ("examples/example-logs/dxt.darshan", 2, [0.5], [0]),
        # check that if we ask for more y-axis labels than
        # available, we still get the same output
        ("examples/example-logs/dxt.darshan", 4, [0.5], [0]),
        ("tests/input/sample-dxt-simple.darshan", 2, [0.5], [0.0]),
        # check that if we ask for more y-axis labels than
        # available, we still get the same output
        ("tests/input/sample-dxt-simple.darshan", 4, [0.5], [0.0]),
    ],
)
def test_set_y_axis_ticks_and_labels(
    filepath,
    n_ylabels,
    expected_yticks,
    expected_yticklabels,
):
    # make sure the x-axis ticks and
    # tick labels are generated appropriately

    # load the report and generate the aggregate data dataframe
    report = darshan.DarshanReport(filepath)
    agg_df = heatmap_handling.get_aggregate_data(
        report=report, mods=["DXT_POSIX"], ops=["read", "write"]
    )

    # x-axis bins are arbitrary
    xbins = 100

    # generate the heatmap data
    data = heatmap_handling.get_heatmap_df(agg_df=agg_df, xbins=xbins)

    # use the unique ranks to get the number of y-axis bins to use
    unique_ranks = np.unique(agg_df["rank"].values)
    ybins = unique_ranks.size

    # generate a joint plot object, then add the heatmap to it
    jointgrid = sns.jointplot(kind="hist", bins=[xbins, ybins])
    sns.heatmap(data, ax=jointgrid.ax_joint)

    # set the x-axis ticks and tick labels
    plot_dxt_heatmap.set_y_axis_ticks_and_labels(
        jointgrid=jointgrid, n_ylabels=n_ylabels
    )

    # collect the actual x-axis tick labels
    actual_yticks = jointgrid.ax_joint.get_yticks()
    actual_yticklabels = [tl.get_text() for tl in jointgrid.ax_joint.get_yticklabels()]
    actual_yticklabels = np.asarray(actual_yticklabels, dtype=float)

    # make sure the figure object gets closed
    plt.close()

    # verify the actual ticks/labels match the expected
    assert_allclose(actual_yticks, expected_yticks, atol=1e-14, rtol=1e-17)
    assert_allclose(actual_yticklabels, expected_yticklabels, atol=1e-14, rtol=1e-17)


@pytest.mark.parametrize(
    "filepath",
    [
        "examples/example-logs/ior_hdf5_example.darshan",
        "examples/example-logs/dxt.darshan",
        "tests/input/sample-dxt-simple.darshan",
    ],
)
def test_remove_marginal_graph_ticks_and_labels(filepath):
    # regression test ensuring the marginal x/y bar graphs do
    # not have any x/y tick labels or frames

    jgrid = plot_dxt_heatmap.plot_heatmap(
        log_path=filepath, mods=["DXT_POSIX"], ops=["read", "write"], xbins=100
    )

    # verify the heatmap axis is on
    assert jgrid.ax_joint.axison
    # verify the marginal axes are turned off
    assert not jgrid.ax_marg_x.axison
    assert not jgrid.ax_marg_y.axison

    # make sure the label lists are empty
    assert jgrid.ax_marg_x.get_xticklabels() == []
    assert jgrid.ax_marg_x.get_yticklabels() == []
    assert jgrid.ax_marg_y.get_xticklabels() == []
    assert jgrid.ax_marg_y.get_yticklabels() == []

    # close the figure object
    plt.close()


@pytest.mark.parametrize(
    "filepath",
    [
        "examples/example-logs/ior_hdf5_example.darshan",
        "examples/example-logs/dxt.darshan",
        "tests/input/sample-dxt-simple.darshan",
    ],
)
def test_adjust_for_colorbar(filepath):
    # regression test for `plot_dxt_heatmap.adjust_for_colorbar()`

    jgrid = plot_dxt_heatmap.plot_heatmap(log_path=filepath)

    # the plot positions change based on the number of unique ranks.
    # If there is only 1 rank, there is no horizontal bar graph
    # so the x-axis values are scaled accordingly.

    # get heatmap positions
    hmap_positions = jgrid.ax_joint.get_position()
    assert hmap_positions.x0 == 0.1
    assert hmap_positions.y0 == 0.15000000000000002
    assert hmap_positions.y1 == 0.774390243902439
    if filepath == "examples/example-logs/ior_hdf5_example.darshan":
        # since `ior_hdf5_example.darshan` has 4 ranks, it has
        # different x max values because it needs room for
        # the colorbar on the outside of the horizontal bar graph
        assert hmap_positions.x1 == 0.7158709677419354
    else:
        assert hmap_positions.x1 == 0.7824516129032258

    # get vertical bar graph positions
    vert_bar_positions = jgrid.ax_marg_x.get_position()
    assert vert_bar_positions.x0 == 0.1
    assert vert_bar_positions.y0 == 0.7780487804878049
    assert vert_bar_positions.y1 == 0.9
    if filepath == "examples/example-logs/ior_hdf5_example.darshan":
        # since `ior_hdf5_example.darshan` has 4 ranks, the vertical
        # bar graph has a different x max value because it needs room for
        # the colorbar on the outside of the horizontal bar graph
        assert vert_bar_positions.x1 == 0.7158709677419354
    else:
        assert vert_bar_positions.x1 == 0.7824516129032258

    # get horizontal bar graph positions
    horiz_bar_positions = jgrid.ax_marg_y.get_position()
    assert horiz_bar_positions.y0 == 0.15000000000000002
    assert horiz_bar_positions.y1 == 0.774390243902439
    if filepath == "examples/example-logs/ior_hdf5_example.darshan":
        # since `ior_hdf5_example.darshan` has 4 ranks, the horizontal
        # bar graph has different x min/max values because it has to
        # make room for the colorbar
        assert horiz_bar_positions.x0 == 0.7206451612903225
        assert horiz_bar_positions.x1 == 0.84
    else:
        assert horiz_bar_positions.x0 == 0.7877419354838711
        assert horiz_bar_positions.x1 == 0.92

    # get the colorbar positions
    cbar_positions = jgrid.fig.axes[-1].get_position()
    assert cbar_positions.y0 == 0.15000000000000002
    assert cbar_positions.y1 == 0.774390243902439
    if filepath == "examples/example-logs/ior_hdf5_example.darshan":
        # since `ior_hdf5_example.darshan` has 4 ranks, the colorbar has
        # to go closer to the edge of the figure so the horizontal bar
        # graph can fit in the panel
        assert cbar_positions.x0 == 0.85
        assert cbar_positions.x1 == 0.8716135084427767
    else:
        assert cbar_positions.x0 == 0.82
        assert cbar_positions.x1 == 0.8416135084427767


@pytest.mark.parametrize(
    "filepath",
    [
        "examples/example-logs/ior_hdf5_example.darshan",
        "examples/example-logs/dxt.darshan",
        "tests/input/sample-dxt-simple.darshan",
    ],
)
@pytest.mark.parametrize("mods", [["DXT_POSIX"], ["DXT_MPIIO"]])
@pytest.mark.parametrize("ops", [["read", "write"], ["read"], ["write"]])
def test_plot_heatmap(filepath, mods, ops):
    # test the primary plotting function, `plot_dxt_heatmap.plot_heatmap()`

    if mods == ["DXT_MPIIO"]:
        # if the input module is not "DXT_POSIX" check
        # that we raise the appropriate error
        with pytest.raises(NotImplementedError, match="DXT_POSIX module is required."):
            jgrid = plot_dxt_heatmap.plot_heatmap(
                log_path=filepath, mods=mods, ops=ops, xbins=100
            )
    elif (filepath == "tests/input/sample-dxt-simple.darshan") & (ops == ["read"]):
        # this log file is known to not have any read data, so
        # make sure we raise a ValueError here
        expected_msg = (
            "No data available for selected module\\(s\\) and operation\\(s\\)."
        )
        with pytest.raises(ValueError, match=expected_msg):
            jgrid = plot_dxt_heatmap.plot_heatmap(
                log_path=filepath, mods=mods, ops=ops, xbins=100
            )
    else:
        jgrid = plot_dxt_heatmap.plot_heatmap(
            log_path=filepath, mods=mods, ops=ops, xbins=100
        )

        # verify the margins for all plots
        assert jgrid.ax_joint.margins() == (0.05, 0.05)
        assert jgrid.ax_marg_x.margins() == (0.05, 0.05)
        assert jgrid.ax_marg_y.margins() == (0.05, 0.05)

        # ensure the heatmap spines are all visible
        for _, spine in jgrid.ax_joint.spines.items():
            assert spine.get_visible()

        # for single-rank files, check that the
        # horizontal bar graph does not exist
        assert jgrid.ax_marg_x.has_data()
        assert jgrid.ax_joint.has_data()
        if filepath == "examples/example-logs/ior_hdf5_example.darshan":
            # verify the horizontal bar graph contains data since there
            # are multiple ranks for this case
            assert jgrid.ax_marg_y.has_data()
        else:
            # verify the horizontal bar graph does not contain data
            assert not jgrid.ax_marg_y.has_data()

        # check that the axis labels are as expected
        assert jgrid.ax_joint.get_xlabel() == "Time (s)"
        assert jgrid.ax_joint.get_ylabel() == "Rank"

    plt.close()
