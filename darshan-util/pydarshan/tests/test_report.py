#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `pydarshan` package."""

import copy

import pytest
import numpy as np
from numpy.testing import assert_allclose

import darshan


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
