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
