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
