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


def test_internal_references():
    """
    Test if the reference ids match. This tests mainly serves to make
    regressions verbose when the behavior is changed.
    """

    report = darshan.DarshanReport()

    # check the convienience refs are working fine
    check = id(report.records) == id(report.data['records'])
    assert check is True

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
