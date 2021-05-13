"""
Draft code for the `data access by filesystem` section
of Phil's hand drawing of future report layout.
"""

import os
import pathlib
import collections

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import darshan


def convert_file_path_to_root_path(file_path):
    path_parts = pathlib.Path(file_path).parts
    filesystem_root = ''.join(path_parts[:2])
    return filesystem_root

def convert_file_id_to_path(input_id, file_id_dict):
    result_found = False
    for file_id_hash, file_path in file_id_dict.items():
        if np.allclose(input_id, file_id_hash):
            result_found = True
            return file_path
    if not result_found:
        msg = f'could not find path for file ID: {input_id}'
        raise ValueError(msg)

def identify_filesystems(file_id_dict, verbose=False):
    # file_id_dict is from report.data["name_records"]

    # the function returns a list of unique filesystems
    # (path roots)

    filesystem_roots = []
    excluded = ['<STDIN>', '<STDOUT>', '<STDERR>']
    for file_id_hash, file_path in file_id_dict.items():
        filesystem_root = convert_file_path_to_root_path(file_path=file_path)
        if filesystem_root not in filesystem_roots:
            if filesystem_root not in excluded:
                filesystem_roots.append(filesystem_root)
    if verbose:
        print("filesystem_roots:", filesystem_roots)
    return filesystem_roots

def per_filesystem_unique_file_read_write_counter_posix(report, filesystem_roots, verbose=False):
    # we are interested in finding all unique files that we have read
    # at least 1 byte from, or written at least 1 byte to
    # and then summing those counts per filesystem

    # report is a darshan.DarshanReport()
    # filesystem_roots is a list of unique filesystem root paths
    # from identify_filesystems()

    # returns: tuple
    # (read_groups, write_groups)
    # where each element of the tuple is a pandas
    # Series object with a format like the one shown below

    # filesystem_root
    # /tmp       1
    # /yellow    1

    # the int64 values in the Series are the counts
    # of unique files to which a single byte has been read
    # (or written) on a given filesystem (index)

    report.mod_read_all_records('POSIX', dtype='pandas')
    rec_counters = report.records['POSIX'][0]['counters']
    
    # first, filter to produce a dataframe where POSIX_BYTES_READ >= 1
    # for each row (tracked event for a given rank or group of ranks)
    df_reads = rec_counters.loc[rec_counters['POSIX_BYTES_READ'] >= 1]

    # similar filter for writing
    df_writes = rec_counters.loc[rec_counters['POSIX_BYTES_WRITTEN'] >= 1]

    # add column with filepaths for each event
    df_reads = df_reads.assign(filepath=df_reads['id'].map(lambda a: convert_file_id_to_path(a, file_id_dict)))
    df_writes = df_writes.assign(filepath=df_writes['id'].map(lambda a: convert_file_id_to_path(a, file_id_dict)))

    # add column with filesystem root paths for each event
    df_reads = df_reads.assign(filesystem_root=df_reads['filepath'].map(lambda path: convert_file_path_to_root_path(path)))
    df_writes = df_writes.assign(filesystem_root=df_writes['filepath'].map(lambda path: convert_file_path_to_root_path(path)))

    # groupby filesystem root and filepath, then get sizes
    # these are Pandas series objects where the index
    # is the name of the filesystem_root and the value
    # is the number of unique files counted per above criteria
    read_groups = df_reads.groupby('filesystem_root')['filepath'].nunique()
    write_groups = df_writes.groupby('filesystem_root')['filepath'].nunique()

    # if either of the Series are effectively empty we want
    # to produce a new Series with the filesystem_root indices
    # and count values of 0 (for plotting purposes)
    if read_groups.size == 0:
        d = {}
        keys = filesystem_roots
        for key in keys:
            d[key] = 0
        read_groups = pd.Series(data=d, index=keys)

    if verbose:
        print("read_groups:\n", read_groups)
        print("write_groups:\n", write_groups)
    return (read_groups, write_groups)

def plot_series_files_rw(file_rd_series, file_wr_series, ax, log_filename):
    # plot the number of unique files per filesystem to which a single
    # byte has been read or written

    # file_rd_series and file_wr_series are pandas
    # Series objects with filesystems for indices
    # and int64 counts of unique files for values

    # ax is a matplotlib axis object

    file_rd_series.plot(ax=ax, kind='barh', xlabel=None, ylabel=None, color='red', alpha=0.5, width=0.1)
    file_wr_series.plot(ax=ax, kind='barh', xlabel=None, ylabel=None, color='blue', alpha=0.5, width=0.1)
    ax.set_xlabel('# unique files')
    ax.set_ylabel('')
    ax.legend(['read', 'write'])

if __name__ == '__main__':
    # produce sample plots for some of the logs
    # available in our test suite
    root_path = 'tests/input'
    log_files = ['sample-dxt-simple.darshan', 'sample.darshan', 'sample-goodost.darshan']
    for idx, log_file in enumerate(log_files):
        fig = plt.figure()
        fig.suptitle(f"Data Access by Filesystem for log file: '{log_file}'")
        ax_bytes = fig.add_subplot(1, 2, 1)
        ax_files = fig.add_subplot(1, 2, 2)
        log_path = os.path.join(root_path, log_file)
        filename = os.path.basename(log_path)
        report = darshan.DarshanReport(log_path, read_all=True)
        file_id_dict = report.data["name_records"]
        filesystem_roots = identify_filesystems(file_id_dict=file_id_dict, verbose=True)
        file_rd_series, file_wr_series = per_filesystem_unique_file_read_write_counter_posix(report=report, filesystem_roots=filesystem_roots, verbose=True)
        plot_series_files_rw(file_rd_series=file_rd_series,
                             file_wr_series=file_wr_series,
                             ax=ax_files,
                             log_filename=log_file)

        fig.set_size_inches(12, 4)
        fig.tight_layout()
        fig.savefig(f'{log_file}_data_access_by_filesystem.png', dpi=300)
