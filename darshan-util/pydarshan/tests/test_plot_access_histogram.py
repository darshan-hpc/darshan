import pytest

from numpy.testing import assert_array_equal
import matplotlib.pyplot as plt

import darshan
from darshan.experimental.plots.plot_access_histogram import plot_access_histogram

darshan.enable_experimental()


class TestPlotAccessHistogram:
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
    def test_xticks_and_labels(self, log_path, mod):
        # check the x-axis tick mark locations and
        # labels for `plot_access_histogram()`

        report = darshan.DarshanReport(log_path)
        fig = plot_access_histogram(report=report, mod=mod)

        # retrieve the x-axis tick mark locations and labels
        # from the output figure object
        ax = fig.axes[0]
        actual_xticks = ax.get_xticks()
        actual_xticklabels = [tl.get_text() for tl in ax.get_xticklabels()]

        # the expected x-axis tick marks and locations
        # should be the same for all cases
        expected_xticks = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        expected_xticklabels = [
            "0-100", "101-1K", "1K-10K", "10K-100K", "100K-1M",
            "1M-4M", "4M-10M", "10M-100M", "100M-1G", "1G+",
        ]
        assert_array_equal(actual_xticks, expected_xticks)
        assert_array_equal(actual_xticklabels, expected_xticklabels)

    @pytest.mark.parametrize(
        "log_path, mod, expected_rd_vals, expected_wr_vals",
        [
            (
                "examples/example-logs/dxt.darshan",
                "POSIX",
                [2289, 1193, 2470, 173, 1, 0, 0, 0, 0, 0],
                [160, 12, 1325, 0, 0, 0, 0, 0, 0, 0],
            ),
            (
                "examples/example-logs/ior_hdf5_example.darshan",
                "POSIX",
                [3, 17, 0, 0, 16, 0, 0, 0, 0, 0],
                [3, 4, 0, 0, 16, 0, 0, 0, 0, 0],
            ),
            (
                "examples/example-logs/ior_hdf5_example.darshan",
                "MPI-IO",
                [3, 17, 0, 0, 16, 0, 0, 0, 0, 0],
                [3, 4, 0, 0, 16, 0, 0, 0, 0, 0],
            ),
            (
                "examples/example-logs/sample-badost.darshan",
                "POSIX",
                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                [0, 0, 0, 0, 0, 131072, 0, 0, 0, 0],
            ),
            (
                "examples/example-logs/sample.darshan",
                "POSIX",
                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                [4, 14, 0, 0, 0, 0, 0, 0, 16384, 0],
            ),
            (
                "examples/example-logs/sample.darshan",
                "MPI-IO",
                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                [4, 14, 0, 0, 0, 0, 0, 0, 16384, 0],
            ),
        ],
    )
    def test_bar_heights(self, log_path, mod, expected_rd_vals, expected_wr_vals):
        # check the bar graph heights for `plot_access_histogram`
        # since they represent the counts for each access size

        report = darshan.DarshanReport(log_path)
        fig, ax = plt.subplots()
        plot_access_histogram(report=report, mod=mod, ax=ax)

        # retrieve the read/write access bar graph heights
        actual_rd_vals = []
        actual_wr_vals = []
        for ax in fig.axes:
            for i, patch in enumerate(ax.patches):
                if i < 10:
                    actual_rd_vals.append(patch.get_height())
                else:
                    actual_wr_vals.append(patch.get_height())

        assert_array_equal(actual_rd_vals, expected_rd_vals)
        assert_array_equal(actual_wr_vals, expected_wr_vals)
