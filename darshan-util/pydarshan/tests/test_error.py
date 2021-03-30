#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `pydarshan` package."""

import pytest

import darshan.backend.cffi_backend as backend


@pytest.fixture
def response():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """
    pass


def test_cannotopen():
    """Ensure we do not dump core on error."""

    log = backend.log_open("fake/tooth-fairy.darshan")

    rec = backend.log_get_record(log, "MPI-IO")
    assert rec['counters'][1] == 2048


def test_ishouldrun():
    assert 1
