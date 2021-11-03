import pytest

from numpy.testing import assert_array_equal
import matplotlib.pyplot as plt

import darshan
from darshan.experimental.plots.plot_opcounts import plot_opcounts

darshan.enable_experimental()


class TestPlotOpCounts:

    @pytest.mark.parametrize(
        "log_path",
        [
            "examples/example-logs/dxt.darshan",
            "examples/example-logs/ior_hdf5_example.darshan",
            "examples/example-logs/sample-badost.darshan",
            "examples/example-logs/sample.darshan",
        ],
    )
    def test_xticks_and_labels(self, log_path):
        # check the x-axis tick mark locations and
        # labels for `plot_opcounts()`

        report = darshan.DarshanReport(log_path)
        fig = plot_opcounts(report=report)

        # retrieve the x-axis tick mark locations and labels
        # from the output figure object
        ax = fig.axes[0]
        actual_xticks = ax.get_xticks()
        actual_xticklabels = [tl.get_text() for tl in ax.get_xticklabels()]

        # the expected x-axis tick marks and locations
        # should be the same for all cases
        expected_xticks = [0, 1, 2, 3, 4, 5, 6]
        expected_xticklabels = ['Read', 'Write', 'Open', 'Stat', 'Seek', 'Mmap', 'Fsync']
        assert_array_equal(actual_xticks, expected_xticks)
        assert_array_equal(actual_xticklabels, expected_xticklabels)

    @pytest.mark.parametrize(
        """log_path, expected_POSIX_vals, expected_MPIIO_IND_vals,
        expected_MPIIO_COL_vals, expected_STDIO_vals""",
        [
            (
                "examples/example-logs/dxt.darshan",
                [6126, 1497, 264, 1379, 5597, 0, 0],
                [0, 0, 0, 0, 0, 0, 0],
                [0, 0, 0, 0, 0, 0, 0],
                [39, 0, 1, 0, 0, 0, 0],
            ),
            (
                "examples/example-logs/ior_hdf5_example.darshan",
                [36, 23, 22, 4, 53, 0, 0],
                [36, 23, 1, 0, 0, 0, 0],
                [0, 0, 16, 0, 0, 0, 0],
                [0, 128, 1, 0, 0, 0, 9],
            ),
            (
                "examples/example-logs/sample-badost.darshan",
                [0, 131072, 2048, 2048, 131072, 0, 2048],
                [0, 0, 0, 0, 0, 0, 0],
                [0, 0, 0, 0, 0, 0, 0],
                [34816, 97, 6144, 0, 0, 0, 2056],
            ),
            (
                "examples/example-logs/sample.darshan",
                [0, 16402, 2049, 0, 16404, 0, 0],
                [0, 18, 0, 0, 0, 0, 0],
                [0, 16384, 2048, 0, 0, 0, 0],
                [0, 74, 129, 0, 0, 0, 0],
            ),
        ],
    )
    def test_bar_heights(
        self,
        log_path,
        expected_POSIX_vals,
        expected_MPIIO_IND_vals,
        expected_MPIIO_COL_vals,
        expected_STDIO_vals,
    ):
        # check the operation counts for `plot_opcounts()`
        # by retrieving the heights for all bar graphs
        # and comparing them to the expected outcomes

        report = darshan.DarshanReport(log_path)
        fig, ax = plt.subplots()
        plot_opcounts(report=report, ax=ax)

        # retrieve the operation counts for each module
        actual_POSIX_vals = []
        actual_MPIIO_IND_vals = []
        actual_MPIIO_COL_vals = []
        actual_STDIO_vals = []
        for ax in fig.axes:
            for i, patch in enumerate(ax.patches):
                if i <= 6:
                    actual_POSIX_vals.append(patch.get_height())
                elif (i > 6) & (i <= 13):
                    actual_MPIIO_IND_vals.append(patch.get_height())
                elif (i > 13) & (i <= 20):
                    actual_MPIIO_COL_vals.append(patch.get_height())
                elif i > 20:
                    actual_STDIO_vals.append(patch.get_height())

        assert_array_equal(actual_POSIX_vals, expected_POSIX_vals)
        assert_array_equal(actual_MPIIO_IND_vals, expected_MPIIO_IND_vals)
        assert_array_equal(actual_MPIIO_COL_vals, expected_MPIIO_COL_vals)
        assert_array_equal(actual_STDIO_vals, expected_STDIO_vals)
