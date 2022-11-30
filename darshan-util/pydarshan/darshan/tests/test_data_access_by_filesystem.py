import os

import numpy as np
from numpy.testing import assert_allclose
import pytest
import pandas as pd
from pandas.testing import assert_series_equal
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import darshan
from darshan.experimental.plots import data_access_by_filesystem
from darshan.log_utils import get_log_path

@pytest.mark.parametrize("series, expected_series", [
    # a Series with a single filesystem root path
    # but the other root paths are absent
    (pd.Series([1], index=['/yellow']),
    # we expect the missing filesystem roots to get
    # added in with values of 0
    pd.Series([1, 0, 0], index=['/yellow', '/tmp', '/home'], dtype=np.float64)
    ),
    # a Series with two filesystem root paths,
    # but the other root path is absent
    (pd.Series([1, 3], index=['/yellow', '/tmp']),
    # we expect the single missing root path to get
    # added in with a value of 0
    pd.Series([1, 3, 0], index=['/yellow', '/tmp', '/home'], dtype=np.float64),
    ),
    # a Series with all filesystem root paths
    # present
    (pd.Series([1, 3, 2], index=['/yellow', '/tmp', '/home']),
    # if all root paths are already accounted for in the
    # Series, it will be just fine for plotting so can remain
    # unchanged
    pd.Series([1, 3, 2], index=['/yellow', '/tmp', '/home'], dtype=np.float64),
    ),
    # a Series with only the final filesystem root path
    (pd.Series([2], index=['/home']),
    # we expect the order of the indices to be
    # preserved from the filesystem_roots provided
    # and 0 values filled in where needed
    pd.Series([0, 0, 2], index=['/yellow', '/tmp', '/home'], dtype=np.float64),
    ),
    ])
def test_empty_series_handler(series, expected_series):
    # the empty_series_handler() function should
    # add indices for any filesystems that are missing
    # from a given Series, along with values of 0 for
    # each of those indices (i.e., no activity for that
    # missing filesystem)--this is mostly to enforce
    # consistent plotting behavior
    filesystem_roots = ['/yellow', '/tmp', '/home']
    actual_series = data_access_by_filesystem.empty_series_handler(series=series,
                                                                   filesystem_roots=filesystem_roots)

    assert_series_equal(actual_series, expected_series)

@pytest.mark.parametrize("file_path, expected_root_path", [
    ("/scratch1/scratchdirs/glock/testFile.00000046",
     "/scratch1"),
    ])
def test_convert_file_path_to_root_path(file_path, expected_root_path):
    actual_root_path = data_access_by_filesystem.convert_file_path_to_root_path(file_path=file_path)
    assert actual_root_path == expected_root_path

@pytest.mark.parametrize("input_id, file_id_dict, expected_file_path", [
    (9.457796068806373e+18,
    {210703578647777632: '/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/test.out.locktest.0',
     9457796068806373448: '/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/test.out'},
    '/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/test.out'
    ),
    # intentionally use an ID that is absent
    # in the dictionary
    (9.357796068806371e+18,
    {210703578647777632: '/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/test.out.locktest.0',
     9457796068806373448: '/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/test.out'},
     None
    ),
    ])
def test_convert_file_id_to_path(input_id, file_id_dict, expected_file_path):
    file_id_hash_arr, file_path_arr = data_access_by_filesystem.convert_id_dict_to_arrays(file_id_dict=file_id_dict)
    actual_file_path = data_access_by_filesystem.convert_file_id_to_path(input_id=input_id,
                                                                         file_hashes=file_id_hash_arr,
                                                                         file_paths=file_path_arr)
    assert actual_file_path == expected_file_path

@pytest.mark.parametrize("verbose", [True, False])
@pytest.mark.parametrize("file_id_dict, expected_root_paths", [
    ({210703578647777632: '/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/test.out.locktest.0',
      14388265063268455899: '/tmp/ompi.sn176.28751/jf.29186/1/test.out_cid-0-3400.sm'},
     ['/yellow', '/tmp']),
    ])
def test_identify_filesystems(capsys, file_id_dict, expected_root_paths, verbose):
    actual_root_paths = data_access_by_filesystem.identify_filesystems(file_id_dict=file_id_dict,
                                                                       verbose=verbose)
    assert actual_root_paths == expected_root_paths
    captured = capsys.readouterr()
    if verbose:
        # check that the same root paths
        # are also printed
        for root_path in actual_root_paths:
            assert root_path in captured.out
    else:
        # nothing should be printed
        assert len(captured.out) == 0

@pytest.mark.parametrize("""log_path,
                          expected_df_reads_shape,
                          expected_df_writes_shape""", [
    (get_log_path("sample.darshan"),
     (0, 87),
     (3, 87),
    ),
    (get_log_path("sample-dxt-simple.darshan"),
     (0, 73),
     (2, 73),
    ),
    ])
def test_rec_to_rw_counter_dfs_with_cols(log_path,
                                         expected_df_reads_shape,
                                         expected_df_writes_shape):
    # check basic shape expectations on the dataframes
    # produced by rec_to_rw_counter_dfs_with_cols()
    with darshan.DarshanReport(log_path) as report:
        file_id_dict = report.data["name_records"]
        actual_df_reads, actual_df_writes = data_access_by_filesystem.rec_to_rw_counter_dfs_with_cols(report=report,
                                                                                                      file_id_dict=file_id_dict,
                                                                                                      mod='POSIX')
    assert actual_df_reads.shape == expected_df_reads_shape
    assert actual_df_writes.shape == expected_df_writes_shape

@pytest.mark.parametrize("read_groups, write_groups, filesystem_roots, expected_read_groups, expected_write_groups", [
       (pd.Series([0, 1, 7], index=['/root', '/tmp', '/yellow']),
        pd.Series([5, 5], index=['/root', '/tmp']),
        ['/root', '/tmp', '/yellow', '/usr', '/scratch1'],
        pd.Series([0, 1, 7, 0, 0], index=['/root', '/tmp', '/yellow', '/usr', '/scratch1'], dtype=np.float64),
        pd.Series([5, 5, 0, 0, 0], index=['/root', '/tmp', '/yellow', '/usr', '/scratch1'], dtype=np.float64),
        ),
        ])
def test_check_empty_series(read_groups,
                            write_groups,
                            filesystem_roots,
                            expected_read_groups,
                            expected_write_groups):
    # check that the reindex operation happened as
    # expected
    actual_read_groups, actual_write_groups = data_access_by_filesystem.check_empty_series(read_groups=read_groups,
                                                                                           write_groups=write_groups,
                                                                                           filesystem_roots=filesystem_roots)
    assert_series_equal(actual_read_groups, expected_read_groups)
    assert_series_equal(actual_write_groups, expected_write_groups)

@pytest.mark.parametrize("df_reads, df_writes, expected_read_groups, expected_write_groups", [
    (pd.DataFrame({'filesystem_root': ['/yellow', '/tmp', '/yellow'],
                   'POSIX_BYTES_READ': [3, 5, 90],
                   'POSIX_BYTES_WRITTEN': [0, 9, 0],
                   'COLUMN3': [np.nan, 5, 8],
                   'COLUMN4': ['a', 'b', 'c']}),
    pd.DataFrame({'filesystem_root': ['/yellow', '/tmp', '/tmp'],
                   'POSIX_BYTES_READ': [1, 11, 17],
                   'POSIX_BYTES_WRITTEN': [2098, 9, 20],
                   'COLUMN3': [np.nan, 5, 1],
                   'COLUMN4': ['a', 'b', 'd']}),
    pd.Series([5, 93], index=pd.Index(['/tmp', '/yellow'], name="filesystem_root"), name='BYTES_READ'),
    pd.Series([29, 2098], index=pd.Index(['/tmp', '/yellow'], name="filesystem_root"), name='BYTES_WRITTEN'),
            ),
        ])
def test_process_byte_counts(df_reads, df_writes, expected_read_groups, expected_write_groups):
    actual_read_groups, actual_write_groups = data_access_by_filesystem.process_byte_counts(df_reads=df_reads,
                                                                                            df_writes=df_writes)
    assert_series_equal(actual_read_groups, expected_read_groups)
    assert_series_equal(actual_write_groups, expected_write_groups)

@pytest.mark.parametrize("df_reads, df_writes, expected_read_groups, expected_write_groups", [
    (pd.DataFrame({'filesystem_root': ['/yellow', '/tmp', '/yellow'],
                   'filepath': ['/yellow/file1', '/tmp/file2', '/yellow/file3'],
                   'POSIX_BYTES_READ': [3, 5, 90],
                   'POSIX_BYTES_WRITTEN': [0, 9, 0],
                   'COLUMN3': [np.nan, 5, 8],
                   'COLUMN4': ['a', 'b', 'c']}),
    pd.DataFrame({'filesystem_root': ['/yellow', '/tmp', '/tmp'],
                   'filepath': ['/yellow/file4', '/tmp/file5', '/tmp/file19'],
                   'POSIX_BYTES_READ': [1, 11, 17],
                   'POSIX_BYTES_WRITTEN': [2098, 9, 20],
                   'COLUMN3': [np.nan, 5, 1],
                   'COLUMN4': ['a', 'b', 'd']}),
    pd.Series([1, 2], index=pd.Index(['/tmp', '/yellow'], name="filesystem_root"), name='filepath'),
    pd.Series([2, 1], index=pd.Index(['/tmp', '/yellow'], name="filesystem_root"), name='filepath'),
            ),
        ])
def test_process_unique_files(df_reads, df_writes, expected_read_groups, expected_write_groups):
    actual_read_groups, actual_write_groups = data_access_by_filesystem.process_unique_files(df_reads=df_reads,
                                                                                             df_writes=df_writes)
    assert_series_equal(actual_read_groups, expected_read_groups)
    assert_series_equal(actual_write_groups, expected_write_groups)

@pytest.mark.parametrize("mod", ["POSIX", "OTHER"])
@pytest.mark.parametrize("verbose", [True, False])
@pytest.mark.parametrize("""log_path,
                            processing_func,
                            expected_read_groups,
                            expected_write_groups""", [
    (get_log_path("sample.darshan"),
     data_access_by_filesystem.process_unique_files,
     pd.Series([0.0, 0.0, 0.0, 0.0], index=pd.Index(['<STDIN>', '<STDOUT>', '<STDERR>', '/scratch2'], name='filesystem_root'), name='filepath'),
     pd.Series([0.0, 1.0, 1.0, 1.0], index=pd.Index(['<STDIN>', '<STDOUT>', '<STDERR>', '/scratch2'], name='filesystem_root'), name='filepath')),
    ])
def test_unique_fs_rw_counter(log_path,
                              processing_func,
                              verbose,
                              expected_read_groups,
                              expected_write_groups,
                              mod):
    with darshan.DarshanReport(log_path) as report:
        file_id_dict = report.data["name_records"]
        filesystem_roots = data_access_by_filesystem.identify_filesystems(report.data["name_records"])
        if mod == "POSIX":
                actual_read_groups, actual_write_groups = data_access_by_filesystem.unique_fs_rw_counter(report=report,
                                                                                                         filesystem_roots=filesystem_roots,
                                                                                                         file_id_dict=file_id_dict,
                                                                                                         processing_func=processing_func,
                                                                                                         mod=mod,
                                                                                                         verbose=verbose)
                assert_series_equal(actual_read_groups, expected_read_groups)
                assert_series_equal(actual_write_groups, expected_write_groups)
        else:
            with pytest.raises(NotImplementedError):
                data_access_by_filesystem.unique_fs_rw_counter(report=report,
                                                               filesystem_roots=filesystem_roots,
                                                               file_id_dict=file_id_dict,
                                                               processing_func=processing_func,
                                                               mod=mod,
                                                               verbose=verbose)


@pytest.mark.parametrize("""file_rd_series,
                            file_wr_series,
                            bytes_rd_series,
                            bytes_wr_series,
                            filesystem_roots
                         """, [

     (pd.Series([3.0], index=pd.Index(['/p'], name='filesystem_root'), name='filepath'),
      pd.Series([14.0], index=pd.Index(['/p'], name='filesystem_root'), name='filepath'),
      pd.Series([2.145206e+09], index=pd.Index(['/p'], name='filesystem_root'), name='POSIX_BYTES_READ'),
      pd.Series([1.010878e+12], index=pd.Index(['/p'], name='filesystem_root'), name='POSIX_BYTES_WRITTEN'),
      ['/p'],
         ),
       ])
def test_plot_data(file_rd_series, file_wr_series, bytes_rd_series, bytes_wr_series, filesystem_roots):
    # test a few basic properties of the main plotting function
    fig = plt.figure()
    data_access_by_filesystem.plot_data(fig=fig,
                                        file_rd_series=file_rd_series,
                                        file_wr_series=file_wr_series,
                                        bytes_rd_series=bytes_rd_series,
                                        bytes_wr_series=bytes_wr_series,
                                        filesystem_roots=filesystem_roots)
    axes = fig.gca()
    children = axes.get_children()
    actual_list_text_in_fig = []

    # accumulate text added via ax.text()
    # by the function
    for child in children:
        if isinstance(child, matplotlib.text.Text):
            actual_list_text_in_fig.append(child.get_text())

    for expected_text_entry in [matplotlib.text.Text(0, 1, ' # files read (3.00E+00)'),
                                matplotlib.text.Text(0, 0, ' # files written (1.40E+01)')]:
        assert expected_text_entry.get_text() in actual_list_text_in_fig

    # enforce invisibile right-side spine so that
    # there is no overlap between value labels and
    # the plot frame on the right side
    for ax in fig.axes:
        spines = ax.spines
        right_spine_visibility = spines['right'].get_visible()
        assert not right_spine_visibility


def test_empty_data_posix_y_axis_annot_position():
    # the y-axis filesystem annotations were observed
    # to cross the left side spine and overlap onto the plot
    # proper in gh-397, when using a log file that lacks
    # POSIX data
    # verify that this is handled/resolved
    log_file_path = get_log_path('noposixopens.darshan')
    with darshan.DarshanReport(log_file_path) as report:
        actual_fig = data_access_by_filesystem.plot_with_report(report=report)
    # check that the y annotation font sizes have been
    # adjusted based on the length of the strings
    axes = actual_fig.axes
    for ax in axes:
        for child in ax.get_children():
            if isinstance(child, matplotlib.text.Annotation):
                actual_text = child.get_text()
                actual_fontsize = child.get_fontsize()
                if len(actual_text) <= 8:
                    assert actual_fontsize == 18
                else:
                    assert actual_fontsize == 12

@pytest.mark.parametrize("log_file_name, expected_text_labels", [
    ('noposixopens.darshan', ['/global', 'anonymized']),
    ('sample.darshan', ['/scratch2', '<STDERR>', '<STDOUT>']),
    # test case for gh-678
    ('mpi-io-test.darshan', ['/global', '<STDOUT>']),
    ])
def test_cat_labels_std_streams(log_file_name, expected_text_labels):
    # for an anonymized log file that operates on STDIO, STDERR
    # and STDIN, we want appropriate labels to be used instead of confusing
    # integers on y axis; for the same scenario without anonymization,
    # the STD.. stream label seem appropriate
    log_file_path = get_log_path(log_file_name)
    actual_text_labels = []
    with darshan.DarshanReport(log_file_path) as report:
        actual_fig = data_access_by_filesystem.plot_with_report(report=report)
    axes = actual_fig.axes
    for ax in axes:
        for child in ax.get_children():
            if isinstance(child, matplotlib.text.Annotation):
                actual_text = child.get_text()
                actual_text_labels.append(actual_text)
                if 'STD' in actual_text:
                    # format the STD.. streams properly
                    actual_fontsize = child.get_fontsize()
                    assert actual_fontsize == 12

    assert actual_text_labels == expected_text_labels

def test_empty_data_posix_text_position():
    # the bytes and files read/written text labels
    # were observed to be too far to the right in the
    # subplots for a log file lacking POSIX activity
    # in gh-397; regression test this issue
    log_file_path = get_log_path('noposixopens.darshan')
    with darshan.DarshanReport(log_file_path) as report:
        actual_fig = data_access_by_filesystem.plot_with_report(report=report)
    axes = actual_fig.axes
    for ax in axes:
        for child in ax.get_children():
            if isinstance(child, matplotlib.text.Text):
                actual_text = child.get_text()
                # check for correct axis coordinate
                # positions
                if 'read' in actual_text:
                    assert_allclose(child.get_position(), (0, 0.75))
                elif 'written' in actual_text:
                    assert_allclose(child.get_position(), (0, 0.25))


@pytest.mark.parametrize("""file_rd_series,
                            file_wr_series,
                            bytes_rd_series,
                            bytes_wr_series,
                            filesystem_roots
                         """, [

     (pd.Series([1], index=pd.Index(['/p'], name='filesystem_root'), name='filepath'),
      pd.Series([1], index=pd.Index(['/p'], name='filesystem_root'), name='filepath'),
      pd.Series([1.049e+6], index=pd.Index(['/p'], name='filesystem_root'), name='POSIX_BYTES_READ'),
      pd.Series([1.049e+6], index=pd.Index(['/p'], name='filesystem_root'), name='POSIX_BYTES_WRITTEN'),
      ['/p'],
         ),
     # test case where files read/written are zero
     (pd.Series([0], index=pd.Index(['/p'], name='filesystem_root'), name='filepath'),
      pd.Series([0], index=pd.Index(['/p'], name='filesystem_root'), name='filepath'),
      # NOTE: very strange to be able to read/write bytes to 
      # a filesystem and yet have no files read or written
      # to on that filesystem (this might be an error someday?)
      # see comment:
      # https://github.com/darshan-hpc/darshan/pull/397#discussion_r683621305
      pd.Series([1.049e+6], index=pd.Index(['/p'], name='filesystem_root'), name='POSIX_BYTES_READ'),
      pd.Series([1.049e+6], index=pd.Index(['/p'], name='filesystem_root'), name='POSIX_BYTES_WRITTEN'),
      ['/p'],
         ),
       ])
def test_plot_data_labels(file_rd_series, file_wr_series, bytes_rd_series, bytes_wr_series, filesystem_roots):
    # regression test for label spacing in plot
    # based on review comment in gh-397
    fig = plt.figure()
    data_access_by_filesystem.plot_data(fig=fig,
                                        file_rd_series=file_rd_series,
                                        file_wr_series=file_wr_series,
                                        bytes_rd_series=bytes_rd_series,
                                        bytes_wr_series=bytes_wr_series,
                                        filesystem_roots=filesystem_roots)
    for ax in fig.axes:
        for child in ax.get_children():
            if isinstance(child, matplotlib.text.Text):
                actual_text = child.get_text()
                if actual_text not in ['/p', '']:
                    # count the leading spaces for each label
                    leading_spaces = len(actual_text) - len(actual_text.lstrip(' '))
                    # check there is always 1 leading space for each label
                    assert leading_spaces == 1



def test_plot_data_shared_x_axis():
    # regression test for case described here:
    # https://github.com/darshan-hpc/darshan/pull/397#pullrequestreview-717403104
    # https://github.com/darshan-hpc/darshan/pull/397#issuecomment-889504530
    filesystem_roots = ['/usr', '/yellow', '/green', '/global']
    rd_bytes = [1e7, 1e8, 1e9, 1e10]
    wr_bytes = [1e8, 1e9, 1e10, 1e11]
    rd_file_cts = [1e3, 1e4, 1e5, 1e6]
    wr_file_cts = [1e2, 1e3, 1e4, 1e5]
    # multiply by the MiB conversion factor
    factor = 1048576
    bytes_rd_series = pd.Series(data=rd_bytes, index=filesystem_roots) * factor
    bytes_wr_series = pd.Series(data=wr_bytes, index=filesystem_roots) * factor
    file_rd_series = pd.Series(data=rd_file_cts, index=filesystem_roots)
    file_wr_series = pd.Series(data=wr_file_cts, index=filesystem_roots)
    fig = plt.figure()
    data_access_by_filesystem.plot_data(fig,
                                       file_rd_series,
                                       file_wr_series,
                                       bytes_rd_series,
                                       bytes_wr_series,
                                       filesystem_roots)
    # enforce shared log x axes in a given column
    bytes_column_x_axis_limits = []
    files_column_x_axis_limits = []
    for i, ax in enumerate(fig.axes):
        if i % 2 == 0:
            bytes_column_x_axis_limits.append(ax.get_xlim())
        else:
            files_column_x_axis_limits.append(ax.get_xlim())

        # also check for absence of ticklabels
        for label in ax.get_xticklabels(which='both'):
            assert len(label.get_text()) == 0

        for label in ax.get_yticklabels(which='both'):
            assert len(label.get_text()) == 0

    for limits in [bytes_column_x_axis_limits,
                   files_column_x_axis_limits]:
        # matching axes:
        diff = np.diff(limits, axis=0)
        assert_allclose(diff, 0)

    # log scale values:
    assert_allclose(np.array(bytes_column_x_axis_limits)[..., 1], 3.89496945e+11)
    assert_allclose(np.array(files_column_x_axis_limits)[..., 1], 2190302.282682)

    # check for log scaling in both columns
    for i, axis in enumerate(fig.axes):
        if i in [6, 7]:
            assert 'symmetric log scaled' in axis.get_xlabel()
        else:
            assert axis.get_xlabel() == ''


@pytest.mark.parametrize('filename', ['imbalanced-io.darshan'])
def test_log_scale_display(filename):
    # plot columns that are log scaled should be
    # labelled appropriately
    log_path = get_log_path(filename)
    with darshan.DarshanReport(log_path) as report:
        fig = data_access_by_filesystem.plot_with_report(report=report)
    # only index 8 should have the log axis label
    for i, axis in enumerate(fig.axes):
        if i == 8:
            assert 'symmetric log scaled' in axis.get_xlabel()
        else:
            assert axis.get_xlabel() == ''


@pytest.mark.parametrize('filename, expected_dims, num_cats',
                         [('imbalanced-io.darshan', [12, 16], None),
                          ('imbalanced-io.darshan', [12, 16], 3),
                          ('imbalanced-io.darshan', [12, 16], 1),
                          ('snyder_acme.exe_id1253318_9-27-24239-1515303144625770178_2.darshan',
                           [12, 16], None)])
def test_vertical_resize(filename, expected_dims, num_cats):
    # ensure that plots are expanded vertically to
    # match the number of filesystems plotted
    log_path = get_log_path(filename)
    with darshan.DarshanReport(log_path) as report:
        fig = data_access_by_filesystem.plot_with_report(report=report,
                                                         num_cats=num_cats)
    actual_dims = fig.get_size_inches()
    assert_allclose(actual_dims, expected_dims)


@pytest.mark.parametrize("logname", [
    "mpi-io-test.darshan",
    "treddy_mpi-io-test_id4373053_6-2-60198-9815401321915095332_1.darshan",
    ])
def test_annotate_center_align(logname):
    # for review comment here:
    # https://github.com/darshan-hpc/darshan/pull/397#discussion_r690847889
    logpath = get_log_path(logname)
    with darshan.DarshanReport(logpath) as report:
        fig = data_access_by_filesystem.plot_with_report(report=report)
    axes = fig.axes
    for ax in axes:
        for child in ax.get_children():
            if isinstance(child, matplotlib.text.Annotation):
                assert child.get_verticalalignment() == "center"


@pytest.mark.parametrize("logname", [
    "imbalanced-io.darshan",
    "mpi-io-test.darshan",
    ])
def test_text_center_align(logname):
    # for review comment here:
    # https://github.com/darshan-hpc/darshan/pull/397#discussion_r690755364
    logpath = get_log_path(logname)
    with darshan.DarshanReport(logpath) as report:
        fig = data_access_by_filesystem.plot_with_report(report=report)
    axes = fig.axes
    for ax in axes:
        for child in ax.get_children():
            if isinstance(child, matplotlib.text.Text):
                actual_text = child.get_text()
                if "read" in actual_text or "written" in actual_text:
                    assert child.get_verticalalignment() == "center"


@pytest.mark.parametrize("logname", [
    "nonmpi_dxt_anonymized.darshan",
    "partial_data_stdio.darshan",
    "treddy_mpi-io-test_id4373053_6-2-60198-9815401321915095332_1.darshan",
    ])
@pytest.mark.parametrize("num_cats", [2, 8])
def test_subplot_restriction(logname, num_cats):
    # for review comment here:
    # https://github.com/darshan-hpc/darshan/pull/397#discussion_r779176814
    # the number of subplots in a figure should
    # be consistent (<=) with the number of category
    # rows requested, and avoid a collapsed layout
    expected_axes_limit = num_cats * 2
    log_path = get_log_path(logname)
    with darshan.DarshanReport(log_path) as report:
        fig = data_access_by_filesystem.plot_with_report(report=report,
                                                         num_cats=num_cats)
    actual_axes = fig.get_axes()
    assert len(actual_axes) <= expected_axes_limit
    max_y1 = 0
    min_y1 = np.inf
    for ax in actual_axes:
        y1 = ax.get_position().y1
        if y1 > max_y1:
            max_y1 = y1
        if y1 < min_y1:
            min_y1 = y1
    # this spread helps ensure avoidance of
    # a collapsed layout of subplots
    assert (max_y1 - min_y1) > 0.2


@pytest.mark.parametrize("logname", [
    "partial_data_dxt.darshan",
    "partial_data_stdio.darshan",
    ])
def test_plot_with_report_no_file(tmpdir, logname):
    # plot_with_report should only return a figure, and
    # not generate a `.png` file
    # see review comment:
    # https://github.com/darshan-hpc/darshan/pull/397#discussion_r689859765
    with tmpdir.as_cwd():
        log_path = get_log_path(logname)
        with darshan.DarshanReport(log_path) as report:
            fig = data_access_by_filesystem.plot_with_report(report=report,
                                                             num_cats=6)
        files_in_tmp = os.listdir(".")
        assert not files_in_tmp


@pytest.mark.parametrize("logname, top_cat_name, third_cat_name", [
    # spot check the 1st and 3rd most active
    # categories for each case
    ("imbalanced-io.darshan", "/lus", "anonymized"),
    ("nonmpi_dxt_anonymized.darshan", "/", "anonymized"),
    ])
def test_plot_with_report_proper_sort(logname, top_cat_name, third_cat_name):
    # we want to sort categories in descending order of activity
    # (bytes read + bytes written), which is especially important
    # when using `num_cats` for `plot_with_report()`, otherwise
    # we could end up with only i.e., inactive categories/filesystems
    # displayed
    # see review comment:
    # https://github.com/darshan-hpc/darshan/pull/397#discussion_r769186581
    log_path = get_log_path(logname)
    with darshan.DarshanReport(log_path) as report:
        fig = data_access_by_filesystem.plot_with_report(report=report,
                                                         num_cats=6)
    actual_axes = fig.get_axes()
    for i, ax in enumerate(actual_axes):
        for child in ax.get_children():
            if isinstance(child, matplotlib.text.Annotation):
                if i == 0:
                    assert child.get_text() == top_cat_name
                elif i == 4:
                    assert child.get_text() == third_cat_name


@pytest.mark.parametrize("logname", [
    "imbalanced-io.darshan",
    "nonmpi_dxt_anonymized.darshan",
    ])
def test_plot_with_report_root_files(logname):
    # regression test for a bug that resulted in several
    # categories that started with "//" for root-mounted
    # files
    log_path = get_log_path(logname)
    with darshan.DarshanReport(log_path) as report:
        fig = data_access_by_filesystem.plot_with_report(report=report)
    actual_axes = fig.get_axes()
    for i, ax in enumerate(actual_axes):
        for child in ax.get_children():
            if isinstance(child, matplotlib.text.Annotation):
                assert not child.get_text().startswith("//")


@pytest.mark.parametrize("""logname,
                          expected_file_rd_series,
                          expected_file_wr_series,
                          expected_bytes_rd_series,
                          expected_bytes_wr_series""", [
    ("ior_hdf5_example.darshan",
      pd.Series({"<STDIN>": 0.0,
                 "<STDOUT>": 0.0,
                 "<STDERR>": 0.0,
                 "/global": 1.0}),
      pd.Series({"<STDIN>": 0.0,
                 "<STDOUT>": 1.0,
                 "<STDERR>": 0.0,
                 "/global": 1.0}),
      pd.Series({"<STDIN>": 0.0,
                 "<STDOUT>": 0.0,
                 "<STDERR>": 0.0,
                 "/global": 4202504.0}),
      pd.Series({"<STDIN>": 0.0,
                 "<STDOUT>": 2421.0,
                 "<STDERR>": 0.0,
                 "/global": 4195800.0}),
    ),
    ])
def test_stdio_basic_inclusion(logname,
                               expected_file_rd_series,
                               expected_file_wr_series,
                               expected_bytes_rd_series,
                               expected_bytes_wr_series):
    for series in [expected_file_rd_series,
                   expected_file_wr_series,
                   expected_bytes_rd_series,
                   expected_bytes_wr_series]:
        series.index.name = "filesystem_root"
        series.name = "filepath"
    expected_bytes_rd_series.name = "BYTES_READ"
    expected_bytes_wr_series.name = "BYTES_WRITTEN"

    # test for the inclusin of STDIO module
    # data in the accounting of files/bytes read/written
    # (the original "data access by category" implementation
    # was POSIX-only)


    # follow the basic setup in plot_with_report()
    log_path = get_log_path(logname)
    with darshan.DarshanReport(log_path) as report:
        file_id_dict = report.data["name_records"]
        filesystem_roots = data_access_by_filesystem.identify_filesystems(file_id_dict=file_id_dict)

        # now, we expect the files and bytes data structures
        # to properly account for STDIO + POSIX data

        file_rd_series, file_wr_series = data_access_by_filesystem.unique_fs_rw_counter(report=report,
                                                              filesystem_roots=filesystem_roots,
                                                              file_id_dict=file_id_dict,
                                                              processing_func=data_access_by_filesystem.process_unique_files,
                                                              mod='POSIX')
        bytes_rd_series, bytes_wr_series = data_access_by_filesystem.unique_fs_rw_counter(report=report,
                                                                filesystem_roots=filesystem_roots,
                                                                file_id_dict=file_id_dict,
                                                                processing_func=data_access_by_filesystem.process_byte_counts,
                                                                mod='POSIX')

    assert_series_equal(file_rd_series, expected_file_rd_series)
    assert_series_equal(file_wr_series, expected_file_wr_series)
    assert_series_equal(bytes_rd_series, expected_bytes_rd_series)
    assert_series_equal(bytes_wr_series, expected_bytes_wr_series)
