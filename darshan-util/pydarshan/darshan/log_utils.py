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

import functools
import sys
import os
import glob
from typing import Optional, Any

if "pytest" in sys.modules:
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


def _provide_logs_repo_filepaths():
    if has_log_repo:
        return _produce_log_dict("darshan_logs").values()
    return []


@functools.lru_cache(maxsize=4)
def _produce_log_dict(project):
    p = importlib_resources.files(project) # type: Any
    darshan_log_dict = {p.name:str(p) for p in p.glob('**/*.darshan')}
    return darshan_log_dict


def _locate_log(filename: str, project: str) -> Optional[str]:
    """Locates a log in a project."""
    try:
        path = _produce_log_dict(project)[filename]
        return path
    except KeyError:
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
    If used in the context of a pytest run, `pytest.skip()` will
    be used if both A) a local log cannot be found, and
    B) the darshan-logs repo is unavailable.

    This function should never be used in a pytest decorator/
    parametrization mark. While it is possible for the function
    to retrieve log file paths in such scenarios, the imperative
    skip is not tolerated at collection time when the logs repo is absent.

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
