import os

import pytest
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
    logfile = get_log_path(logfile)
    log = backend.log_open(logfile)
    rec = backend.log_get_record(log, mod)
    assert rec == expected_dict
