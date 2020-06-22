# -*- coding: utf-8 -*-

"""Auxiliary to discover darshan-util install directory."""


import os




def check_version():
    """
    Get version from pkg-config and return info.

    :return: Path to a darshan-util installation.
    """

    import subprocess

    args = ['pkg-config', '--modversion', 'darshan-util']
    p = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd='.')
    out,err = p.communicate()
    retval = p.wait()

    return out.decode('ascii').strip()


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
            print("dlopen ok")
        except:
            print("dlopen failed")
            libdutil = None

    if libdutil is None:
        try:
            DARSHAN_PATH = discover_darshan_shutil()
            print(DARSHAN_PATH)
            libdutil = ffi.dlopen(DARSHAN_PATH + "/lib/libdarshan-util.so")
            print("shutil ok")
        except:
            libdutil = None
            print("shutil failed")

    if libdutil is None:
        try:
            DARSHAN_PATH = discover_darshan_pkgconfig()
            print(DARSHAN_PATH)
            libdutil = ffi.dlopen(DARSHAN_PATH + "/lib/libdarshan-util.so")
            print("pkg-config ok")
        except:
            libdutil = None
            print("pkg-config failed")

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
