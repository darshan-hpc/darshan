import pytest

from numpy.testing import assert_array_equal
import matplotlib.pyplot as plt

import darshan
from darshan.experimental.plots import plot_opcounts, plot_access_histogram

darshan.enable_experimental()


@pytest.mark.parametrize(
    "log_path, mod",
    [
        ("examples/example-logs/dxt.darshan", "POSIX"),
        ("examples/example-logs/ior_hdf5_example.darshan", "POSIX"),
        ("examples/example-logs/ior_hdf5_example.darshan", "MPI-IO"),
        ("examples/example-logs/sample-badost.darshan", "POSIX"),
        ("examples/example-logs/sample.darshan", "POSIX"),
        ("examples/example-logs/sample.darshan", "MPI-IO"),
    ],
)
@pytest.mark.parametrize(
    "func, expected_xticks, expected_xticklabels",
    [
        (
            plot_access_histogram,
            range(10),
            ["0-100", "101-1K", "1K-10K", "10K-100K", "100K-1M",
            "1M-4M", "4M-10M", "10M-100M", "100M-1G", "1G+"],
        ),
        (
            plot_opcounts,
            range(7),
            ["Read", "Write", "Open", "Stat", "Seek", "Mmap", "Fsync"],
        ),
    ],
)
def test_xticks_and_labels(log_path, func, expected_xticks, expected_xticklabels, mod):
    # check the x-axis tick mark locations and
    # labels
    report = darshan.DarshanReport(log_path)

    try:
        fig = func(report=report, mod=mod)
    except TypeError:
        fig = func(report=report)

    # retrieve the x-axis tick mark locations and labels
    # from the output figure object
    ax = fig.axes[0]
    actual_xticks = ax.get_xticks()
    actual_xticklabels = [tl.get_text() for tl in ax.get_xticklabels()]

    # the expected x-axis tick marks and locations
    # should be the same for all cases
    assert_array_equal(actual_xticks, expected_xticks)
    assert_array_equal(actual_xticklabels, expected_xticklabels)


@pytest.mark.parametrize(
    "log_path, mod, fig_func, expected_heights",
    [
        (
            "examples/example-logs/dxt.darshan",
            "POSIX",
            plot_access_histogram,
            [2289, 1193, 2470, 173, 1, 0, 0, 0, 0,
            0, 160, 12, 1325, 0, 0, 0, 0, 0, 0, 0],
        ),
        (
            "examples/example-logs/ior_hdf5_example.darshan",
            "POSIX",
            plot_access_histogram,
            [3, 17, 0, 0, 16, 0, 0, 0, 0, 0, 3, 4, 0, 0, 16, 0, 0, 0, 0, 0],
        ),
        (
            "examples/example-logs/ior_hdf5_example.darshan",
            "MPI-IO",
            plot_access_histogram,
            [3, 17, 0, 0, 16, 0, 0, 0, 0, 0, 3, 4, 0, 0, 16, 0, 0, 0, 0, 0],
        ),
        (
            "examples/example-logs/sample-badost.darshan",
            "POSIX",
            plot_access_histogram,
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 131072, 0, 0, 0, 0],
        ),
        # subtle case where "POSIX" and "MPIIO"
        # modules show different results
        (
            "tests/input/sample-dxt-simple.darshan",
            "POSIX",
            plot_access_histogram,
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0],
        ),
        (
            "tests/input/sample-dxt-simple.darshan",
            "MPI-IO",
            plot_access_histogram,
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0],
        ),
        # more obvious case where "POSIX" and
        # "MPIIO" modules show different results
        (
            None,
            "POSIX",
            plot_access_histogram,
            [18, 2492, 14679, 0, 50486, 186, 0, 0, 0,
            0, 43, 301, 2, 0, 50486, 0, 0, 0, 0, 0],
        ),
        (
            None,
            "MPI-IO",
            plot_access_histogram,
            [11, 2492, 2, 0, 0, 0, 0, 410, 86, 0, 2526,
            303, 2, 0, 97812, 396, 0, 410, 86, 0],
        ),
        (
            "examples/example-logs/dxt.darshan",
            None,
            plot_opcounts,
            [6126, 1497, 264, 1379, 5597, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 39, 0, 1, 0, 0, 0, 0],
        ),
        (
            "examples/example-logs/ior_hdf5_example.darshan",
            None,
            plot_opcounts,
            [36, 23, 22, 4, 53, 0, 0, 36, 23, 1, 0, 0, 0,
            0, 0, 0, 16, 0, 0, 0, 0, 0, 128, 1, 0, 0, 0, 9],
        ),
        (
            "examples/example-logs/sample-badost.darshan",
            None,
            plot_opcounts,
            [0, 131072, 2048, 2048, 131072, 0, 2048, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 34816, 97, 6144, 0, 0, 0, 2056],
        ),
        (
            "examples/example-logs/sample.darshan",
            None,
            plot_opcounts,
            [0, 16402, 2049, 0, 16404, 0, 0, 0, 18, 0, 0, 0, 0, 0,
            0, 16384, 2048, 0, 0, 0, 0, 0, 74, 129, 0, 0, 0, 0],
        ),
    ],
)
@pytest.mark.skipif(not pytest.has_log_repo, # type: ignore
                    reason="missing darshan_logs")
@pytest.mark.parametrize("filename", ["imbalanced-io.darshan"])
def test_bar_heights(log_path, mod, fig_func, expected_heights, select_log_repo_file):
    # check bar graph heights

    if log_path is None:
        # for logs repo cases
        log_path = select_log_repo_file

    report = darshan.DarshanReport(log_path)
    fig, ax = plt.subplots()
    try:
        fig_func(report=report, mod=mod, ax=ax)
    except TypeError:
        fig_func(report=report, ax=ax)

    # retrieve the bar graph heights
    actual_heights = []
    for ax in fig.axes:
        for patch in ax.patches:
            actual_heights.append(patch.get_height())

    assert_array_equal(actual_heights, expected_heights)
