import os

def installed_path(filename):
    cwd = os.path.dirname(__file__)
    return os.path.join(cwd, filename)

example_data_files_dxt = {
    "ior_hdf5_example.darshan": installed_path("ior_hdf5_example.darshan"),
    "dxt.darshan": installed_path("ior_hdf5_example.darshan"),
}

