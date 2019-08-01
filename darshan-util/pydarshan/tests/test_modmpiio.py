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


def test_counters():
    """Sample for an expected property in counters."""

    log = darshan.log_open("tests/input/sample.darshan")

    rec = darshan.log_get_mpiio_record(log)
    assert rec['counters'][1] == 2048


def test_fcounters():
    """Sample for an expected property in fcounters."""

    log = darshan.log_open("tests/input/sample.darshan")

    rec = darshan.log_get_mpiio_record(log)
    assert rec['fcounters'][1] == 2.04800000e+03


def test_repeated_access():
    """ Check if repeated access is working."""

    log = darshan.log_open("tests/input/sample.darshan")

    rec1 = darshan.log_get_mpiio_record(log)
    rec2 = darshan.log_get_mpiio_record(log)

    assert rec1 == rec2




def test_ishouldrun():
    assert 1
