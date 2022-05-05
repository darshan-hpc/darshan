#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `pydarshan` package."""

import pytest

import darshan.backend.cffi_backend as backend
from darshan.log_utils import get_log_path


@pytest.fixture
def response():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """
    pass


def test_counters():
    """Sample for an expected property in counters."""

    log = backend.log_open(get_log_path("sample.darshan"))

    rec = backend.log_get_record(log, "MPI-IO")
    assert rec['counters'][1] == 2048


def test_fcounters():
    """Sample for an expected property in fcounters."""

    log = backend.log_open(get_log_path("sample.darshan"))

    rec = backend.log_get_record(log, "MPI-IO")
    assert rec['fcounters'][0] == 3.912783145904541


def test_repeated_access():
    """ Check if repeated access is working."""

    log = backend.log_open(get_log_path("sample.darshan"))

    rec = backend.log_get_record(log, "MPI-IO")
    rec = backend.log_get_record(log, "MPI-IO")     # fetch next

    assert rec is None




def test_ishouldrun():
    assert 1
