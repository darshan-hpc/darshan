import platform
python_version = platform.python_version_tuple()

# shim for convenient Python 3.9 importlib.resources
# interface
if int(python_version[1]) < 9 and int(python_version[0]) == 3:
    import importlib_resources
else:
    # see: https://github.com/python/mypy/issues/1153
    import importlib.resources as importlib_resources # type: ignore


import pytest

try:
    import darshan_logs
    has_log_repo = True
except ImportError:
    has_log_repo = False

def pytest_configure():
    pytest.has_log_repo = has_log_repo

@pytest.fixture
def log_repo_files():
    # provide a convenient way to access the list
    # of all *.darshan log files in the logs repo,
    # returning a list of absolute file paths to
    # the logs
    p = importlib_resources.files('darshan_logs')
    return [str(p) for p in p.glob('**/*.darshan')]
