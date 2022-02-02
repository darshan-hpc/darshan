import os

def installed_path(filename):
    cwd = os.path.dirname(__file__)
    return os.path.join(cwd, filename)

test_data_files_dxt = {
    "sample-dxt-simple.darshan": installed_path("sample-dxt-simple.darshan"),
}

test_data_files = {
    "sample-badost.darshan": installed_path("sample-badost.darshan")
}
