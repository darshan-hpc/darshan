import platform
python_version = platform.python_version_tuple()

# shim for convenient Python 3.9 importlib.resources
# interface
if int(python_version[1]) < 9 and int(python_version[0]) == 3:
    import importlib_resources
else:
    import importlib.resources as importlib_resources


import pytest

try:
    import darshan_logs
    has_log_repo = True
except ImportError:
    has_log_repo = False

def pytest_configure():
    pytest.has_log_repo = has_log_repo
