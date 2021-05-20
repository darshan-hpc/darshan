# miscellaneous tests for the CFFI backend
# that are not specific to any particular
# mod

import re

import pytest
import darshan.backend.cffi_backend as backend

def test_get_lib_version():
    # check for a reasonable version string
    # returned by get_lib_version()
    actual_version = backend.get_lib_version()
    # must be a string
    assert isinstance(actual_version, str)
    # two periods in semantic version num
    assert actual_version.count('.') == 2
    # stricter regular expression match on
    # the semantic version number
    prog = re.compile(r"^\d+\.\d+\.\d+(-.+)?$")
    match = prog.fullmatch(actual_version)
    assert match is not None
    assert match.group(0) == actual_version
