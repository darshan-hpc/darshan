#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `pydarshan` package."""

import pytest

import darshan.backend.cffi_backend as backend

def test_cannotopen():
    """Ensure we do not dump core on error."""

    # assert fake log file name comes back with a NULL file handle
    log = backend.log_open("fake/tooth-fairy.darshan")
    c_file_handle_repr = log['handle'].__repr__()
    assert 'NULL' in c_file_handle_repr

    # assert no record is returned using above NULL file handle
    rec = backend.log_get_record(log, "MPI-IO")
    assert rec == None
