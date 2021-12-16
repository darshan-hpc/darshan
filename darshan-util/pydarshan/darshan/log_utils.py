"""
Module for log handling utilities.
"""

import platform
python_version = platform.python_version_tuple()
# shim for convenient Python 3.9 importlib.resources
# interface
if int(python_version[1]) < 9 and int(python_version[0]) == 3:
    import importlib_resources
else:
    # see: https://github.com/python/mypy/issues/1153
    import importlib.resources as importlib_resources # type: ignore

import os
import glob
from typing import Optional, Any

if "PYTEST_CURRENT_TEST" in os.environ:
    # only import pytest if used in a testing context
    import pytest
    pytest_session = True
else:
    pytest_session = False

try:
    import darshan_logs
    has_log_repo = True
except ImportError:
    has_log_repo = False


def _locate_log(filename: str, project: str) -> Optional[str]:
    """Locates a log in a project."""
    p = importlib_resources.files(project) # type: Any
    if project == "darshan":
        # move up 2 directories for now
        p = p.parent.parent
    darshan_logs_paths = [str(p) for p in p.glob('**/*.darshan')]
    for log_path in darshan_logs_paths:
        if filename in log_path:
            return log_path
    # if log is not found
    return None

def get_log_path(filename: str) -> str:
    """
    Utility function for locating logs either
    locally or in the darshan-logs repo.

    Parameters
    ----------
    filename: filename of Darshan log to locate
    in PyDarshan or darshan-logs repo.

    Returns
    -------
    log_path: absolute path to the darshan log matching the input filename.

    Raises
    ------
    FileNotFoundError: if a log cannot be found that matches the input filename.

    Notes
    -----
    If used in the context of a pytest, `pytest.skip()` will
    be used if both A.) a local log cannot be found, and
    B.) the darshan-logs repo is unavailable.

    """
    # try local paths
    log_path = _locate_log(filename=filename, project='darshan')
    if log_path:
        return log_path
    # try the logs repo
    if has_log_repo:
        log_path = _locate_log(filename=filename, project='darshan_logs')
        if log_path:
            return log_path
        else:
            err_msg = f"File {filename} could not be found in any available resources."
            raise FileNotFoundError(err_msg)
    else:
        err_msg = f"File {filename} not found locally and darshan-logs repo unavailable."
        if pytest_session:
            pytest.skip(err_msg)
        else:
            raise FileNotFoundError(err_msg)
