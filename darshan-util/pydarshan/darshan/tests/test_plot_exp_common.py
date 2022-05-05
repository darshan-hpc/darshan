import pytest

from numpy.testing import assert_array_equal
import matplotlib.pyplot as plt

import darshan
from darshan.experimental.plots import plot_opcounts, plot_access_histogram
from darshan.log_utils import get_log_path

darshan.enable_experimental()


@pytest.mark.parametrize(
    "log_path, mod, func, expected_xticklabels",
    [
        (
            "dxt.darshan",
            "POSIX",
            plot_access_histogram,
            ["0-100", "101-1K", "1K-10K", "10K-100K", "100K-1M",
            "1M-4M", "4M-10M", "10M-100M", "100M-1G", "1G+"]
        ),
        (
            "ior_hdf5_example.darshan",
            "POSIX",
            plot_access_histogram,
            ["0-100", "101-1K", "1K-10K", "10K-100K", "100K-1M",
            "1M-4M", "4M-10M", "10M-100M", "100M-1G", "1G+"]
        ),
        (
            "ior_hdf5_example.darshan",
            "MPI-IO",
            plot_access_histogram,
            ["0-100", "101-1K", "1K-10K", "10K-100K", "100K-1M",
            "1M-4M", "4M-10M", "10M-100M", "100M-1G", "1G+"]
        ),
        (
            "ior_hdf5_example.darshan",
            "H5D",
            plot_access_histogram,
            ["0-100", "101-1K", "1K-10K", "10K-100K", "100K-1M",
            "1M-4M", "4M-10M", "10M-100M", "100M-1G", "1G+"],
        ),
        (
            "sample-badost.darshan",
            "POSIX",
            plot_access_histogram,
            ["0-100", "101-1K", "1K-10K", "10K-100K", "100K-1M",
            "1M-4M", "4M-10M", "10M-100M", "100M-1G", "1G+"]
        ),
        (
            "shane_macsio_id29959_"
            "5-22-32552-7035573431850780836_1590156158.darshan",
            "POSIX",
            plot_access_histogram,
            ["0-100", "101-1K", "1K-10K", "10K-100K", "100K-1M",
            "1M-4M", "4M-10M", "10M-100M", "100M-1G", "1G+"],
        ),
        (
            "shane_macsio_id29959_"
            "5-22-32552-7035573431850780836_1590156158.darshan",
            "MPI-IO",
            plot_access_histogram,
            ["0-100", "101-1K", "1K-10K", "10K-100K", "100K-1M",
            "1M-4M", "4M-10M", "10M-100M", "100M-1G", "1G+"],
        ),
        (
            "dxt.darshan",
            "POSIX",
            plot_opcounts,
            ['Read', 'Write', 'Open', 'Stat', 'Seek', 'Mmap', 'Fsync'],
        ),
        (
            "ior_hdf5_example.darshan",
            "POSIX",
            plot_opcounts,
            ['Read', 'Write', 'Open', 'Stat', 'Seek', 'Mmap', 'Fsync'],
        ),
        (
            "ior_hdf5_example.darshan",
            "MPI-IO",
            plot_opcounts,
            ['Ind. Read', 'Ind. Write', 'Ind. Open',
            'Col. Read', 'Col. Write', 'Col. Open', 'Sync'],
        ),
        (
            "ior_hdf5_example.darshan",
            "STDIO",
            plot_opcounts,
            ['Read', 'Write', 'Open', 'Seek', 'Flush'],
        ),
        (
            "sample-badost.darshan",
            "POSIX",
            plot_opcounts,
            ['Read', 'Write', 'Open', 'Stat', 'Seek', 'Mmap', 'Fsync'],
        ),
        (
            "shane_macsio_id29959_"
            "5-22-32552-7035573431850780836_1590156158.darshan",
            "POSIX",
            plot_opcounts,
            ['Read', 'Write', 'Open', 'Stat', 'Seek', 'Mmap', 'Fsync'],
        ),
        (
            "shane_macsio_id29959_"
            "5-22-32552-7035573431850780836_1590156158.darshan",
            "MPI-IO",
            plot_opcounts,
            ['Ind. Read', 'Ind. Write', 'Ind. Open',
            'Col. Read', 'Col. Write', 'Col. Open', 'Sync'],
        ),
        (
            "ior_hdf5_example.darshan",
            "H5F",
            plot_opcounts,
            ['H5D Read', 'H5D Write', 'H5D Open',
            'H5D Flush', 'H5F Open', 'H5F Flush'],
        ),
        (
            "ior_hdf5_example.darshan",
            "H5D",
            plot_opcounts,
            ['H5D Read', 'H5D Write', 'H5D Open',
            'H5D Flush', 'H5F Open', 'H5F Flush'],
        ),
    ],
)
def test_xticks_and_labels(log_path, func, expected_xticklabels, mod):
    # check the x-axis tick mark locations and
    # labels
    log_path = get_log_path(log_path)
    report = darshan.DarshanReport(log_path)

    fig = func(report=report, mod=mod)

    # retrieve the x-axis tick mark locations and labels
    # from the output figure object
    ax = fig.axes[0]
    actual_xticks = ax.get_xticks()
    actual_xticklabels = [tl.get_text() for tl in ax.get_xticklabels()]

    expected_xticks = range(len(expected_xticklabels))

    assert_array_equal(actual_xticks, expected_xticks)
    assert_array_equal(actual_xticklabels, expected_xticklabels)


@pytest.mark.parametrize(
    "filename, mod, fig_func, expected_heights",
    [
        (
            "dxt.darshan",
            "POSIX",
            plot_access_histogram,
            [2289, 1193, 2470, 173, 1, 0, 0, 0, 0,
            0, 160, 12, 1325, 0, 0, 0, 0, 0, 0, 0],
        ),
        (
            "ior_hdf5_example.darshan",
            "POSIX",
            plot_access_histogram,
            [3, 17, 0, 0, 16, 0, 0, 0, 0, 0, 3, 4, 0, 0, 16, 0, 0, 0, 0, 0],
        ),
        (
            "ior_hdf5_example.darshan",
            "MPI-IO",
            plot_access_histogram,
            [3, 17, 0, 0, 16, 0, 0, 0, 0, 0, 3, 4, 0, 0, 16, 0, 0, 0, 0, 0],
        ),
        (
            "ior_hdf5_example.darshan",
            "H5D",
            plot_access_histogram,
            [0, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 0],
        ),
        (
            "sample-badost.darshan",
            "POSIX",
            plot_access_histogram,
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 131072, 0, 0, 0, 0],
        ),
        # subtle case where "POSIX" and "MPIIO"
        # modules show different results
        (
            "sample-dxt-simple.darshan",
            "POSIX",
            plot_access_histogram,
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0],
        ),
        (
            "sample-dxt-simple.darshan",
            "MPI-IO",
            plot_access_histogram,
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0],
        ),
        # more obvious case where "POSIX" and
        # "MPIIO" modules show different results
        (
            "imbalanced-io.darshan",
            "POSIX",
            plot_access_histogram,
            [18, 2492, 14679, 0, 50486, 186, 0, 0, 0,
            0, 43, 301, 2, 0, 50486, 0, 0, 0, 0, 0],
        ),
        (
            "imbalanced-io.darshan",
            "MPI-IO",
            plot_access_histogram,
            [11, 2492, 2, 0, 0, 0, 0, 410, 86, 0, 2526,
            303, 2, 0, 97812, 396, 0, 410, 86, 0],
        ),
        # "ground truth" log where 10 ranks wrote 1 byte each
        (
            "hdf5_diagonal_write_1_byte_dxt.darshan",
            "H5D",
            plot_access_histogram,
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        ),
        # "ground truth" log where 10 ranks wrote 10-100 bytes each.
        # Should be the same as `hdf5_diagonal_write_1_byte_dxt.darshan`
        # since the first bin includes 0-100
        (
            "hdf5_diagonal_write_bytes_range_dxt.darshan",
            "H5D",
            plot_access_histogram,
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        ),
        # "ground truth" log where 5 ranks wrote 1 byte each
        (
            "hdf5_diagonal_write_half_ranks_dxt.darshan",
            "H5D",
            plot_access_histogram,
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        ),
        (
            "dxt.darshan",
            "POSIX",
            plot_opcounts,
            [6126, 1497, 264, 1379, 5597, 0, 0],
        ),
        (
            "dxt.darshan",
            "STDIO",
            plot_opcounts,
            [39, 0, 1, 0, 0],
        ),
        (
            "ior_hdf5_example.darshan",
            "POSIX",
            plot_opcounts,
            [36, 23, 22, 4, 53, 0, 0],
        ),
        (
            "ior_hdf5_example.darshan",
            "MPI-IO",
            plot_opcounts,
            [36, 23, 1, 0, 0, 16, 0],
        ),
        (
            "ior_hdf5_example.darshan",
            "STDIO",
            plot_opcounts,
            [0, 128, 1, 0, 9],
        ),
        (
            "sample-badost.darshan",
            "POSIX",
            plot_opcounts,
            [0, 131072, 2048, 2048, 131072, 0, 2048],
        ),
        (
            "sample-badost.darshan",
            "STDIO",
            plot_opcounts,
            [34816, 97, 6144, 0, 2056],
        ),
        (
            "shane_macsio_id29959_5-22-32552-7035573431850780836_1590156158.darshan",
            "POSIX",
            plot_opcounts,
            [6, 7816, 51, 32, 4, 0, 0],
        ),
        (
            "shane_macsio_id29959_5-22-32552-7035573431850780836_1590156158.darshan",
            "MPI-IO",
            plot_opcounts,
            [0, 7695, 0, 0, 64, 16, 0],
        ),
        (
            "ior_hdf5_example.darshan",
            "H5F",
            plot_opcounts,
            [0, 0, 0, 0, 6, 0],
        ),
        (
            "ior_hdf5_example.darshan",
            "H5D",
            plot_opcounts,
            [12, 12, 6, 0, 6, 0],
        ),
        # "ground truth" log with only 3 `H5F` opens
        (
            "hdf5_file_opens_only.darshan",
            "H5F",
            plot_opcounts,
            [0, 0, 0, 0, 3, 0],
        ),
        # "ground truth" log where 10 files and datasets are opened
        # and each dataset is written to once
        (
            "hdf5_diagonal_write_1_byte_dxt.darshan",
            "H5D",
            plot_opcounts,
            [0, 10, 10, 0, 10, 0],
        ),
        # "ground truth" log where 10 files and datasets are opened
        # and each dataset is written to once. Should match
        # `hdf5_diagonal_write_1_byte_dxt.darshan` since they only
        # differ in the number of bytes written to each dataset
        (
            "hdf5_diagonal_write_bytes_range_dxt.darshan",
            "H5D",
            plot_opcounts,
            [0, 10, 10, 0, 10, 0],
        ),
        # "ground truth" log where 10 files and datasets are opened,
        # each dataset is written to once, and half of the files
        # call the flush operation
        (
            "hdf5_diagonal_write_half_flush_dxt.darshan",
            "H5D",
            plot_opcounts,
            [0, 10, 10, 0, 10, 5],
        ),
        # "ground truth" log where 10 files are opened and 5
        # datasets are opened and written to
        (
            "hdf5_diagonal_write_half_ranks_dxt.darshan",
            "H5D",
            plot_opcounts,
            [0, 5, 5, 0, 10, 0],
        ),
    ],
)
def test_bar_heights(filename, mod, fig_func, expected_heights):
    # check bar graph heights
    log_path = get_log_path(filename)
    report = darshan.DarshanReport(log_path)
    fig, ax = plt.subplots()

    fig_func(report=report, mod=mod, ax=ax)

    # retrieve the bar graph heights
    actual_heights = []
    for ax in fig.axes:
        for patch in ax.patches:
            actual_heights.append(patch.get_height())

    plt.close(fig)

    assert_array_equal(actual_heights, expected_heights)
