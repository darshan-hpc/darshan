import pytest

try:
    import darshan_logs
    has_log_repo = True
except ImportError:
    has_log_repo = False

def pytest_configure():
    pytest.has_log_repo = has_log_repo
