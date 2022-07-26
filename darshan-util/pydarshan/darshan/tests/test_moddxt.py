import os

import pytest
import numpy as np
from numpy.testing import assert_allclose
import darshan.backend.cffi_backend as backend
from darshan.log_utils import get_log_path

@pytest.mark.parametrize("logfile", [
    # an incredibly simple darshan DXT trace
    # of a small sample C MPI-IO program from
    # https://wgropp.cs.illinois.edu/courses/cs598-s15/lectures/lecture32.pdf
    "sample-dxt-simple.darshan",
    ])
@pytest.mark.parametrize("mod, expected_dict", [
    ('DXT_POSIX', {'id': 14388265063268455899,
                   'rank': 0,
                   'hostname': 'sn176.localdomain',
                   'write_count': 1,
                   'read_count': 0,
                   'write_segments': [{'offset': 0,
                                       'length': 40,
                                       'start_time': 0.10337884305045009,
                                       'end_time': 0.10338771319948137}],
                   'read_segments': []}),
    ('DXT_MPIIO', {'id': 9457796068806373448,
                   'rank': 0,
                   'hostname': 'sn176.localdomain',
                   'write_count': 1,
                   'read_count': 0,
                   'write_segments': [{'offset': 0, 
                                       'length': 4000,
                                       'start_time': 0.10368914622813463,
                                       'end_time': 0.1053433942142874}], 
                   'read_segments': []})])
def test_dxt_records(logfile, mod, expected_dict):
    # regression guard for DXT records values
    # write_segments and read_segments are now NumPy
    # recarrays, to save considerable memory
    # per gh-779
    # TODO: refactor for simplicity--we can probably
    # just initialize the expected values via
    # np.array() with the appropriate structured dtypes
    expected_write_segs = np.recarray(1, dtype=[("offset", int),
                                                ("length", int),
                                                ("start_time", float),
                                                ("end_time", float)])
    expected_read_segs = np.recarray(1, dtype=[("offset", int),
                                               ("length", int),
                                               ("start_time", float),
                                               ("end_time", float)])
    if expected_dict["write_segments"]:
        expected_write_segs.offset = expected_dict["write_segments"][0]["offset"]
        expected_write_segs.length = expected_dict["write_segments"][0]["length"]
        expected_write_segs.start_time = expected_dict["write_segments"][0]["start_time"]
        expected_write_segs.end_time = expected_dict["write_segments"][0]["end_time"]
    else:
        expected_write_segs = np.recarray(0, dtype=[("offset", int),
                                                    ("length", int),
                                                    ("start_time", float),
                                                    ("end_time", float)])
    if expected_dict["read_segments"]:
        expected_read_segs.offset = expected_dict["read_segments"][0]["offset"]
        expected_read_segs.length = expected_dict["read_segments"][0]["length"]
        expected_read_segs.start_time = expected_dict["read_segments"][0]["start_time"]
        expected_read_segs.end_time = expected_dict["read_segments"][0]["end_time"]
    else:
        expected_read_segs = np.recarray(0, dtype=[("offset", int),
                                                    ("length", int),
                                                    ("start_time", float),
                                                    ("end_time", float)])
    expected_dict["write_segments"] = expected_write_segs
    expected_dict["read_segments"] = expected_read_segs

    logfile = get_log_path(logfile)
    log = backend.log_open(logfile)
    rec = backend.log_get_record(log, mod)
    for key in expected_dict.keys():
        if "segments" in key:
            # careful, can't use assert_allclose directly
            # on recarrays
            assert_allclose(rec[key].offset, expected_dict[key].offset)
            assert_allclose(rec[key].length, expected_dict[key].length)
            assert_allclose(rec[key].start_time, expected_dict[key].start_time)
            assert_allclose(rec[key].end_time, expected_dict[key].end_time)
        else:
            assert rec[key] == expected_dict[key]
