# -*- coding: utf-8 -*-

"""Auxiliary to discover darshan-util install directory."""


import os




def darshanutils_version():
    """
    Discovers an existing darshan-util installation and returns the appropriate
    path to a shared object for use with Python's CFFI.

    :return: Path to a darshan-util installation.
    """

    import subprocess

    args = ['pkg-config', '--modversion', 'darshan-util']
    p = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd='.')
    out,err = p.communicate()
    retval = p.wait()

    return retval

    if darshan_config:
        return os.path.realpath(darshan_config + '/../../')
    else:
        raise RuntimeError('Could not discover darshan! Is darshan-util installed?')


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

    print(retval)

    if darshan_config:
        return os.path.realpath(darshan_config + '/../../')
    else:
        raise RuntimeError('Could not discover darshan! Is darshan-util installed?')


def discover_darshan_shutil():
    """
    Discovers an existing darshan-util installation and returns the appropriate
    path to a shared object for use with Python's CFFI.

    :return: Path to a darshan-util installation.
    """
    
    import shutil    
    darshan_config = shutil.which('darshan-parser')
   
    # alternatively via
    #pkg-config --path darshan-util

    if darshan_config:
        return os.path.realpath(darshan_config + '/../../')
    else:
        raise RuntimeError('Could not discover darshan! Is darshan-util installed and set in your PATH?')


def discover_darshan():
    """
    Discovers an existing darshan-util installation and returns the appropriate
    path to a shared object for use with Python's CFFI.

    :return: Path to a darshan-util installation.
    """
    
    return discover_darshan_shutil()


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
