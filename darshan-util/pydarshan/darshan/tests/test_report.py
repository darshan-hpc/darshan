#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `pydarshan` package."""

import re
import copy
import pickle

import pytest
import numpy as np
from numpy.testing import assert_allclose, assert_array_less
import pandas as pd
from pandas.testing import assert_frame_equal

import darshan
import darshan.backend.cffi_backend as backend
from darshan.report import DarshanRecordCollection
from darshan.log_utils import get_log_path, _provide_logs_repo_filepaths
from darshan.experimental.plots import heatmap_handling


@pytest.fixture
def response():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """
    pass


@pytest.mark.skipif(not pytest.has_log_repo,
                    reason="missing darshan_logs")
@pytest.mark.parametrize("log_filepath",
        _provide_logs_repo_filepaths()
        )
def test_jobid_type_all_logs_repo_files(log_filepath):
    # test for the expected jobid type in each of the
    # log files in the darshan_logs package;
    # this is primarily intended as a demonstration of looping
    # through all logs repo files in a test
    if "e3sm_io_heatmap_and_dxt" in log_filepath:
        pytest.xfail(reason="large memory requirements")
    with darshan.DarshanReport(log_filepath) as report:
        assert isinstance(report.metadata['job']['jobid'], int)

def test_job():
    """Sample for expected job data."""
    with darshan.DarshanReport(get_log_path("sample.darshan")) as report:
        assert report.metadata["job"]["log_ver"] == "3.10"

def test_metadata():
    """Sample for an expected property in counters."""

    with darshan.DarshanReport(get_log_path("sample.darshan")) as report:
        # check a metadata field
        assert 4478544 == report.metadata['job']['jobid']


def test_modules():
    """Sample for an expected number of modules."""

    with darshan.DarshanReport(get_log_path("sample.darshan")) as report:
        # check if number of modules matches
        assert 4 == len(report.modules)
        assert 154 == report.modules['MPI-IO']['len']
        assert False == report.modules['MPI-IO']['partial_flag']

def test_load_records():
    """Test if loaded records match."""

    with darshan.DarshanReport(get_log_path("sample.darshan")) as report:
        report.mod_read_all_records("POSIX")
        assert 1 == len(report.data['records']['POSIX'])


@pytest.mark.parametrize("unsupported_record",
        ["DXT_POSIX", "DXT_MPIIO", "LUSTRE", "APMPI", "APXC"]
        )
def test_unsupported_record_load(caplog, unsupported_record):
    # check for appropriate logger warning when attempting to
    # load unsupported record
    with darshan.DarshanReport(get_log_path("sample.darshan")) as report:
        report.mod_read_all_records(mod=unsupported_record)

    for record in caplog.records:
        assert 'Currently unsupported' in record.message
        assert unsupported_record in record.message

@pytest.mark.parametrize("mod,expected", 
    [("BUGGER", "Log does not contain data for mod"), 
    ("POSIX", "Unsupported module:")])
def test_dxt_mod(caplog, mod: str, expected: str):
    """Invalid/unsupported dxt module cases"""
    with darshan.DarshanReport(get_log_path("sample-dxt-simple.darshan")) as report:
        report.mod_read_all_dxt_records(mod, warnings=True)
    
    for record in caplog.records:
        assert expected in record.message

def test_internal_references():
    """
    Test if the reference ids match. This tests mainly serves to make
    regressions verbose when the behavior is changed.
    """

    with darshan.DarshanReport() as report:
        # check the convienience refs are working fine
        check = id(report.records) == id(report.data['records'])
        assert check is True

def test_info_contents(capsys):
    # regression guard for the output from the info()
    # method of DarshanReport
    with darshan.DarshanReport(get_log_path("sample.darshan")) as report:
        report.info()
    captured = capsys.readouterr()
    expected_keys = ['Times',
                     'Executable',
                     'Processes',
                     'JobID',
                     'UID',
                     'Modules in Log',
                     'Loaded Records',
                     'Name Records',
                     'Darshan/Hints',
                     'DarshanReport']

    expected_values = ['2048', '4478544', '69615']
    expected_strings = expected_keys + expected_values

    for expected_string in expected_strings:
        assert expected_string in captured.out

@pytest.mark.parametrize("invalid_filepath", [
    # messy path that does not exist
    '#!$%',
    # path that exists but has no
    # actual log file
    '.',
    ]
    )
def test_report_invalid_file(invalid_filepath):
    # verify appropriate error handling for
    # provision of an invalid file path to
    # DarshanReport

    with pytest.raises(RuntimeError, match='Failed to open file'):
        with darshan.DarshanReport(invalid_filepath) as report:
            pass

def test_json_fidelity():
    # regression test for provision of appropriate
    # data by to_json() method of DarshanReport class
    with darshan.DarshanReport(get_log_path("sample.darshan")) as report:
        actual_json = report.to_json()

    for expected_key in ["version",
                         "metadata",
                         "job",
                         "uid",
                         "start_time",
                         "end_time",
                         "nprocs"]:
        assert expected_key in actual_json

    for expected_value in ['69615',
                           '1490000867',
                           '1490000983',
                           '2048',
                           'lustre',
                           'dvs',
                           'rootfs']:
        assert expected_value in actual_json

@pytest.mark.parametrize("key", ['POSIX', 'MPI-IO', 'STDIO'])
@pytest.mark.parametrize("subkey", ['counters', 'fcounters'])
def test_deepcopy_fidelity_darshan_report(key, subkey):
    # regression guard for the __deepcopy__() method
    # of DarshanReport class
    # note that to_numpy() also performs a deepcopy
    with darshan.DarshanReport(get_log_path("sample.darshan")) as report:
        report_deepcopy = copy.deepcopy(report)
        # the deepcopied records should be identical
        # within floating point tolerance
        assert_allclose(report_deepcopy.data['records'][key].to_numpy()[0][subkey],
                        report.data['records'][key].to_numpy()[0][subkey])
        # a deepcopy should not share memory bounds
        # with the original object (or deepcopies thereof)
        assert not np.may_share_memory(report_deepcopy.data['records'][key].to_numpy()[0][subkey],
                                       report.data['records'][key].to_numpy()[0][subkey])


class TestDarshanRecordCollection:

    @pytest.mark.parametrize("mod",
        ["POSIX", "MPI-IO", "STDIO", "H5F", "H5D", "DXT_POSIX", "DXT_MPIIO", "LUSTRE"]
    )
    @pytest.mark.parametrize("attach", ["default", ["id"], ["rank"], None])
    def test_to_df_synthetic(self, mod, attach):
        # test for `DarshanRecordCollection.to_df()`
        # builds `DarshanRecordCollection`s from scratch, injects 
        # random numpy data, then verifies the data is conserved

        # for reproducibility, initialize a seeded random number generator
        rng = np.random.default_rng(seed=0)

        # adjust "attach" so it's easier to handle downstream
        if attach == "default":
            attach = ["rank", "id"]

        # generate arrays for generating synthetic records
        rank_data = np.arange(0, 20, 4, dtype=np.int64)
        id_data = np.array(
            [
                14734109647742566553,
                15920181672442173319,
                7238257241479193519,
                10384774853006289996,
                9080082818774558450,
            ],
            dtype=np.uint64,
        )

        # use an arbitrary log to generate an empty DarshanRecordCollection
        with darshan.DarshanReport(get_log_path("ior_hdf5_example.darshan")) as report:
            collection = DarshanRecordCollection(report=report, mod=mod)

        if "DXT_" in mod:
            # generate some random arrays to use for the synthetic DXT records
            start_data = rng.random(size=(5, 8))
            # to keep the values realistic, just add 1 to the start times
            end_data = start_data + 1

            expected_records = []
            for id, rank, start, end in zip(id_data, rank_data, start_data, end_data):
                # each DXT record contains a dictionary for both read
                # and write segments
                rd_dict = {
                    "offset": rng.integers(low=0, high=1000, size=(8,)),
                    "length": rng.integers(low=0, high=100000, size=(8,)),
                    "start_time": start,
                    "end_time": end,
                }
                # add an arbitrary number so the values
                # are unique for each record
                wr_dict = {
                    "offset": rng.integers(low=0, high=1000, size=(8,)),
                    "length": rng.integers(low=0, high=100000, size=(8,)),
                    "start_time": start + 10,
                    "end_time": end + 10,
                }
                # use the read/write dictionaries to make a record
                rec = {
                    "id": id,
                    "rank": rank,
                    "read_segments": rd_dict,
                    "write_segments": wr_dict,
                }
                # When `to_df()` is called, the only thing that changes
                # is the dictionaries are turned into pandas dataframes
                rec_df = {
                    "id": id,
                    "rank": rank,
                    "read_segments": pd.DataFrame(rd_dict),
                    "write_segments": pd.DataFrame(wr_dict),
                }
                collection.append(rec)
                expected_records.append(rec_df)

        elif mod == "LUSTRE":
            # retrieve the counter column names
            counter_cols = backend.counter_names(mod)
            n_ct_cols = len(counter_cols)
            # use the column counts to generate random arrays
            # and generate the counter and fcounter dataframes
            counter_data = rng.integers(low=0, high=100, size=(5, n_ct_cols))
            # the ost ids are of variable length,
            # so create a ragged list of integer arrays
            ost_id_data = []
            for i in range(n_ct_cols):
                # generate a random number between 1 and 10
                # to decide how many ost ids will be generated
                arr_length = rng.integers(low=1, high=10, size=1)
                # generate a set of ost ids (of length `arr_length`)
                # between 0 and 100
                ost_id_arr = rng.integers(low=0, high=100, size=arr_length)
                ost_id_data.append(ost_id_arr)

            expected_ct_df = pd.DataFrame(counter_data, columns=counter_cols)
            expected_ct_df["ost_ids"] = ost_id_data

            # if attach is specified, the expected
            # dataframes have to be modified
            if attach:
                if "id" in attach:
                    expected_ct_df.insert(0, "id", id_data)
                if "rank" in attach:
                    expected_ct_df.insert(0, "rank", rank_data)

            # use the same data to generate the synthetic records
            # with the default data structures
            for rank, id, ct_row, ost_row in zip(rank_data, id_data, counter_data, ost_id_data):
                rec = {"rank": rank, "id": id, "counters": ct_row, "ost_ids": ost_row}
                collection.append(rec)

        else:
            # retrieve the counter/fcounter column names
            counter_cols = backend.counter_names(mod)
            fcounter_cols = backend.fcounter_names(mod)
            # count the number of column names
            n_ct_cols = len(counter_cols)
            n_fct_cols = len(fcounter_cols)
            # use the column counts to generate random arrays
            # and generate the counter and fcounter dataframes
            counter_data = rng.integers(low=0, high=100, size=(5, n_ct_cols))
            fcounter_data = rng.random(size=(5, n_fct_cols))
            expected_ct_df = pd.DataFrame(
                counter_data,
                columns=counter_cols,
            )
            expected_fct_df = pd.DataFrame(
                fcounter_data,
                columns=fcounter_cols,
            )

            # if attach is specified, the expected
            # dataframes have to be modified
            if attach:
                if "id" in attach:
                    # if the ids are attached, build a dataframe for them
                    # then prepend it to the original expected dataframe
                    id_df = pd.DataFrame(id_data, columns=["id"])
                    expected_ct_df = pd.concat([id_df, expected_ct_df], axis=1)
                    expected_fct_df = pd.concat([id_df, expected_fct_df], axis=1)
                if "rank" in attach:
                    # if the ranks are attached, prepend them as well
                    rank_df = pd.DataFrame(rank_data, columns=["rank"])
                    expected_ct_df = pd.concat([rank_df, expected_ct_df], axis=1)
                    expected_fct_df = pd.concat([rank_df, expected_fct_df], axis=1)

            # use the same data to generate the synthetic records
            # with the default data structures
            for rank, id, ct_row, fct_row in zip(rank_data, id_data, counter_data, fcounter_data):
                rec = {"rank": rank, "id": id, "counters": ct_row, "fcounters": fct_row}
                collection.append(rec)

        actual_records = collection.to_df(attach=attach)
        if "DXT_" in mod:
            for actual, expected in zip(actual_records, expected_records):
                assert actual["id"] == expected["id"]
                assert actual["rank"] == expected["rank"]
                assert_frame_equal(actual["read_segments"], expected["read_segments"])
                assert_frame_equal(actual["write_segments"], expected["write_segments"])
        elif mod == "LUSTRE":
            actual_ct_df = actual_records["counters"]
            assert_frame_equal(actual_ct_df, expected_ct_df)
        else:
            actual_ct_df = actual_records["counters"]
            actual_fct_df = actual_records["fcounters"]
            assert_frame_equal(actual_ct_df, expected_ct_df)
            assert_frame_equal(actual_fct_df, expected_fct_df)

def test_file_closure():
    # regression test for gh-578
    with darshan.DarshanReport(get_log_path("ior_hdf5_example.darshan")) as report:
        # you cannot serialize a report object
        # until after you close its associated
        # file handle; check that __del__ cleans up
        # poperly
        report.__del__()
        pickle.dumps(report)
        # let's also check that we can properly exit
        # context when report.log has been removed
        # (happens with pytest sometimes)
        del(report.log)


@pytest.mark.parametrize("logname, hmap_types, nbins, bin_width, shape",
        [("runtime_and_dxt_heatmaps_diagonal_write_only.darshan",
          ["POSIX"],
          34,
          0.1,
          (32, 34),
          ),
         ("e3sm_io_heatmap_only.darshan",
          ["POSIX", "MPIIO", "STDIO"],
          114,
          6.4,
          (512, 114),
          ),
        ])
def test_runtime_heatmap_retrieval(capsys, logname, hmap_types, nbins, bin_width, shape):
    with darshan.DarshanReport(get_log_path(logname), read_all=True) as report:
        assert list(report.heatmaps.keys()) == hmap_types
        for heatmap in report.heatmaps.values():
            assert heatmap._nbins == nbins
            assert heatmap._bin_width_seconds == bin_width
            assert heatmap.to_df(["read"]).shape == shape
            assert heatmap.to_df(["write"]).shape == shape
            for str_element in ["Heatmap", "mod", "nbins", "bin_width"]:
                assert str_element in repr(heatmap)

            heatmap.info()
            captured = capsys.readouterr()
            expected_elements = ["Module:",
                                 "Ranks",
                                 "Num. bins",
                                 "Bin width (s)",
                                 "Num. recs",
                                 ]
            for element in expected_elements:
                assert element in captured.out


def test_runtime_dxt_heatmap_similarity():
    # this log file should have a similar "diagonal"
    # data structure in both DXT and runtime HEATMAP forms;
    # only write activity should be present

    # the expected diagonal data structures
    #       |
    # ranks V
    #          bins ->
    #    x
    #   x
    #  x
    # x
    expected_active_ranks = np.arange(32)
    expected_active_bins = expected_active_ranks
    log_path = get_log_path("runtime_and_dxt_heatmaps_diagonal_write_only.darshan")
    with darshan.DarshanReport(log_path) as report:

        # runtime HEATMAP:
        runtime_heatmap_df = list(report.heatmaps.values())[0].to_df(["write"])
        nonzero_rows_runtime, nonzero_columns_runtime = np.nonzero(runtime_heatmap_df.to_numpy())

        # DXT heatmap:
        agg_df = heatmap_handling.get_aggregate_data(report=report, ops=["write"])
        dxt_heatmap_df = heatmap_handling.get_heatmap_df(agg_df=agg_df, xbins=32, nprocs=32)
        nonzero_rows_dxt, nonzero_columns_dxt = np.nonzero(dxt_heatmap_df.to_numpy())

        # runtime HEATMAP vs. expected diagonal structure
        assert_allclose(nonzero_rows_runtime, expected_active_ranks)
        assert_allclose(nonzero_columns_runtime, expected_active_bins)

        # runtime HEATMAP vs. DXT heatmap
        # because of differing resolutions, there is actually
        # a small difference in the diagonal data structure
        # that we will allow
        assert_allclose(nonzero_rows_runtime, nonzero_rows_dxt)
        assert_allclose(nonzero_columns_runtime[:23], nonzero_columns_dxt[:23])
        # if we allow a resolution gap of 1 bin, they should match on full length
        assert_array_less(np.abs(nonzero_columns_runtime - nonzero_columns_dxt), 1.0001)


@pytest.mark.parametrize(
    "mod, expected_counts",
        [
            # expected counts are the number of non-zero bins for
            # the read, write, and read+write dataframes, respectively
            # for all 3 modules, the third count value (read + write)
            # is the the sum of the first 2 values (read, write)
            ('POSIX', [512, 6105, 6617]),
            ('MPIIO', [512, 56320, 56832]),
            ('STDIO', [0, 1, 1]),
        ]
    )
def test_heatmap_operations(mod, expected_counts):
    # test `Heatmap.to_df()` method by checking the generated
    # read, write, and read+write dataframes against each other

    log_path = get_log_path("e3sm_io_heatmap_only.darshan")
    with darshan.DarshanReport(log_path) as report:

        rd_df = report.heatmaps[mod].to_df(ops=["read"])
        wr_df = report.heatmaps[mod].to_df(ops=["write"])
        rd_wr_df = report.heatmaps[mod].to_df(ops=["read", "write"])

    for df, expected_count in zip((rd_df, wr_df, rd_wr_df), expected_counts):
        # check that the non-zero element counts are correct
        actual_count = np.nonzero(df.to_numpy())[0].shape[0]
        # compare the respective counts for each dataframe
        assert actual_count == expected_count

    assert_frame_equal(rd_df + wr_df, rd_wr_df)


def test_heatmap_df_invalid_operation():
    # check that when invalid operations (anything other than "read"
    # or "write") are passed to `Heatmap.to_df()` a `ValueError` is raised
    log_path = get_log_path("e3sm_io_heatmap_only.darshan")
    with darshan.DarshanReport(log_path) as report:
        with pytest.raises(ValueError, match="invalid_op not in heatmap"):
            report.heatmaps["POSIX"].to_df(ops=["invalid_op"])


def test_pnetcdf_hdf5_match():
    # test for some equivalent (f)counters between similar
    # HDF5 and PNETCDF-enabled runs of ior
    with darshan.DarshanReport(get_log_path("shane_ior-PNETCDF_id438100-438100_11-9-41525-10280033558448664385_1.darshan")) as pnetcdf_ior_report:
        with darshan.DarshanReport(get_log_path("shane_ior-HDF5_id438090-438090_11-9-41522-17417065676046418211_1.darshan")) as hdf5_ior_report:
            pnetcdf_ior_report.mod_read_all_records("PNETCDF_FILE")
            pnetcdf_ior_report.mod_read_all_records("PNETCDF_VAR")
            hdf5_ior_report.mod_read_all_records("H5F")
            hdf5_ior_report.mod_read_all_records("H5D")
            pnetcdf_file_data_dict = pnetcdf_ior_report.data['records']["PNETCDF_FILE"].to_df()
            pnetcdf_var_data_dict = pnetcdf_ior_report.data['records']["PNETCDF_VAR"].to_df()
            h5f_data_dict = hdf5_ior_report.data['records']["H5F"].to_df()
            h5d_data_dict = hdf5_ior_report.data['records']["H5D"].to_df()
    prog = re.compile(r"(PNETCDF_FILE|PNETCDF_VAR)")
    # we compare:
    # PNETCDF_FILE vs. H5F modules
    # PNETCDF_VAR vs. H5D modules
    equiv_val_counts = 0
    # some fields don't match
    exception_strings = ["rank",
                         "DEFS",
                         "WAIT",
                         "SYNC",
                         "TIME",
                         "id",
                         "PNETCDF_VAR_ACCESS1_LENGTH",
                         "PNETCDF_VAR_ACCESS1_STRIDE",
                         "PNETCDF_VAR_DATATYPE_SIZE",
                         "FASTEST",
                         "SLOWEST"]
    for pnetcdf_dict, hdf5_dict in ([pnetcdf_file_data_dict, h5f_data_dict],
                                    [pnetcdf_var_data_dict, h5d_data_dict]):
        for counter_type in ["counters", "fcounters"]:
            for pnetcdf_column_name in pnetcdf_dict[counter_type].columns:
                if pnetcdf_column_name == "PNETCDF_FILE_CREATES" or pnetcdf_column_name == "PNETCDF_FILE_OPENS":
                    # different counter granularity here it seems
                    pnetcdf_data = (pnetcdf_dict[counter_type]["PNETCDF_FILE_OPENS"] +
                                    pnetcdf_dict[counter_type]["PNETCDF_FILE_CREATES"])
                    hdf5_column_name =  "H5F_OPENS"
                    hdf5_data = hdf5_dict[counter_type][hdf5_column_name]
                elif any(x in pnetcdf_column_name for x in exception_strings):
                    continue
                else:
                    pnetcdf_data = pnetcdf_dict[counter_type][pnetcdf_column_name]
                    match = prog.search(pnetcdf_column_name)
                    if match is not None:
                        mod_str = match.group(1)
                        if "FILE" in mod_str:
                            hdf5_column_name = pnetcdf_column_name.replace(mod_str, "H5F")
                        else:
                            hdf5_column_name = pnetcdf_column_name.replace(mod_str, "H5D")
                    else:
                        hdf5_column_name = pnetcdf_column_name

                    try:
                        hdf5_data = hdf5_dict[counter_type][hdf5_column_name]
                    except KeyError:
                        # PNETCDF_FILE will have many keys that don't exist
                        # in H5F
                        continue

                assert_allclose(pnetcdf_data.values, hdf5_data.values)
                equiv_val_counts += 1
    # we also require a certain number of equivalent counters
    # to help avoid regressions:
    assert equiv_val_counts == 65
    # PNETCDF_FILE captures some extra file-format related IO
    # activity vs. the user-level "dataset" IO proper:
    assert pnetcdf_file_data_dict["counters"]["PNETCDF_FILE_BYTES_READ"].values > pnetcdf_var_data_dict["counters"]["PNETCDF_VAR_BYTES_READ"].values
