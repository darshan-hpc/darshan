# -*- coding: utf-8 -*-

"""Auxiliary to discover darshan-util install directory."""


import os




def check_version(ffi=None, libdutil=None):
    """
    Get version from pkg-config and return info.

    :return: Path to a darshan-util installation.
    """
    lib_version = None

    # query library directly (preferred)
    if ffi is not None and libdutil is not None:
        ver = ffi.new("char **")
        ver = libdutil.darshan_log_get_lib_version()
        lib_version = ffi.string(ver).decode("utf-8")

    # pkgconfig fallback
    if lib_version is None:
        print("WARNING: Using pk-config fallback.")
        import subprocess
        args = ['pkg-config', '--modversion', 'darshan-util']
        p = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd='.')
        out,err = p.communicate()
        retval = p.wait()
        lib_version = out.decode('ascii').strip()


    import darshan
   
    package_version = darshan.__version__.split(".")
    lib_version = lib_version.split(".")
    
    if package_version[0:3] != lib_version[0:3]:
        from darshan.error import DarshanVersionError
        raise DarshanVersionError(
                target_version = ".".join(package_version[0:3]),
                provided_version = ".".join(lib_version), 
                msg="This version of PyDarshan")


    return lib_version



def discover_darshan_pkgconfig():
    """
    Discovers an existing darshan-util installation and returns the appropriate
    path to a shared object for use with Python's CFFI.

    :return: Path to a darshan-util installation.
    """

    import subprocess

    args = ['pkg-config', '--path', 'darshan-util']
    p = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd='.')
    out,err = p.communicate()
    retval = p.wait()

    path = os.path.dirname(out.decode('ascii').strip())

    if path:
        return os.path.realpath(path + '/../../')
    else:
        raise RuntimeError('Could not discover darshan! Is darshan-util installed?')


def discover_darshan_shutil():
    """
    Discovers an existing darshan-util installation and returns the appropriate
    path to a shared object for use with Python's CFFI.

    :return: Path to a darshan-util installation.
    """
    
    import shutil    
    path = shutil.which('darshan-parser')
   
    if path:
        return os.path.realpath(path + '/../../')
    else:
        raise RuntimeError('Could not discover darshan! Is darshan-util installed and set in your PATH?')




def find_utils(ffi, libdutil):
    if libdutil is None:
        try:
            libdutil = ffi.dlopen("libdarshan-util.so")
        except:
            libdutil = None

    if libdutil is None:
        try:
            DARSHAN_PATH = discover_darshan_shutil()
            libdutil = ffi.dlopen(DARSHAN_PATH + "/lib/libdarshan-util.so")
        except:
            libdutil = None
            print("shutil failed")

    if libdutil is None:
        try:
            DARSHAN_PATH = discover_darshan_pkgconfig()
            libdutil = ffi.dlopen(DARSHAN_PATH + "/lib/libdarshan-util.so")
        except:
            libdutil = None

    if libdutil is None:
        raise RuntimeError('Could not find libdarshan-util.so! Is darshan-util installed? Please ensure one of the the following: 1) export LD_LIBRARY_PATH=<path-to-libdarshan-util.so>, or 2) darshan-parser can found using the PATH variable, or 3) pkg-config can resolve pkg-config --path darshan-util')

    return libdutil



def load_darshan_header():
    """
    Returns a CFFI compatible header for darshan-utlil as a string.

    :return: String with a CFFI compatible header for darshan-util.
    """

    curdir, curfile = os.path.split(__file__)
    filepath = os.path.join(curdir, 'data', 'darshan-api.h')
    # filepath = os.path.join(curdir, 'data', 'generated.h')

    print(filepath)

    with open(filepath, 'r') as f:
        try:
            return f.read()
        except IOError:
            raise RuntimeError('Failed to read API definition header for darshan.')
