#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `pydarshan` package."""

import copy

import pytest
import numpy as np
from numpy.testing import assert_allclose
import pandas as pd
from pandas.testing import assert_frame_equal # type: ignore

import darshan
import darshan.backend.cffi_backend as backend
from darshan.report import DarshanRecordCollection


@pytest.fixture
def response():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """
    pass


@pytest.mark.skipif(not pytest.has_log_repo, # type: ignore
                    reason="missing darshan_logs")
def test_jobid_type_all_logs_repo_files(log_repo_files):
    # test for the expected jobid type in each of the
    # log files in the darshan_logs package;
    # this is primarily intended as a demonstration of looping
    # through all logs repo files in a test
    for log_filepath in log_repo_files:
        report = darshan.DarshanReport(log_filepath)
        assert isinstance(report.metadata['job']['jobid'], int)

def test_job():
    """Sample for expected job data."""
    report = darshan.DarshanReport("tests/input/sample.darshan")

    assert report.metadata["job"]["log_ver"] == "3.10"

def test_metadata():
    """Sample for an expected property in counters."""

    report = darshan.DarshanReport("tests/input/sample.darshan")

    # check a metadata field
    assert 4478544 == report.metadata['job']['jobid']


def test_modules():
    """Sample for an expected number of modules."""

    report = darshan.DarshanReport("tests/input/sample.darshan")

    # check if number of modules matches
    assert 4 == len(report.modules)
    assert 154 == report.modules['MPI-IO']['len']
    assert False == report.modules['MPI-IO']['partial_flag']

def test_load_records():
    """Test if loaded records match."""

    report = darshan.DarshanReport("tests/input/sample.darshan")

    report.mod_read_all_records("POSIX")

    assert 1 == len(report.data['records']['POSIX'])


@pytest.mark.parametrize("unsupported_record",
        ["DXT_POSIX", "DXT_MPIIO", "LUSTRE", "APMPI", "APXC"]
        )
def test_unsupported_record_load(caplog, unsupported_record):
    # check for appropriate logger warning when attempting to
    # load unsupported record
    report = darshan.DarshanReport("tests/input/sample.darshan")
    report.mod_read_all_records(mod=unsupported_record)
    for record in caplog.records:
        assert 'Currently unsupported' in record.message
        assert unsupported_record in record.message

@pytest.mark.parametrize("mod,expected", 
    [("BUGGER", "Log does not contain data for mod"), 
    ("POSIX", "Unsupported module:")])
def test_dxt_mod(caplog, mod: str, expected: str):
    """Invalid/unsupported dxt module cases"""
    report = darshan.DarshanReport("tests/input/sample-dxt-simple.darshan")

    report.mod_read_all_dxt_records(mod, warnings=True)
    
    for record in caplog.records:
        assert expected in record.message

def test_internal_references():
    """
    Test if the reference ids match. This tests mainly serves to make
    regressions verbose when the behavior is changed.
    """

    report = darshan.DarshanReport()

    # check the convienience refs are working fine
    check = id(report.records) == id(report.data['records'])
    assert check is True

def test_info_contents(capsys):
    # regression guard for the output from the info()
    # method of DarshanReport
    report = darshan.DarshanReport("tests/input/sample.darshan")
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
        darshan.DarshanReport(invalid_filepath)

def test_json_fidelity():
    # regression test for provision of appropriate
    # data by to_json() method of DarshanReport class
    report = darshan.DarshanReport("tests/input/sample.darshan")
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
    report = darshan.DarshanReport("tests/input/sample.darshan")
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
        ["POSIX", "MPI-IO", "STDIO", "H5F", "H5D", "DXT_POSIX", "DXT_MPIIO"]
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
        report = darshan.DarshanReport("examples/example-logs/ior_hdf5_example.darshan")
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
        else:
            actual_ct_df = actual_records["counters"]
            actual_fct_df = actual_records["fcounters"]
            assert_frame_equal(actual_ct_df, expected_ct_df)
            assert_frame_equal(actual_fct_df, expected_fct_df)
