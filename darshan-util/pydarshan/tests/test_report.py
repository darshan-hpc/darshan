#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `pydarshan` package."""

import pytest

import darshan


@pytest.fixture
def response():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """
    pass


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
                     'Executeable',
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
