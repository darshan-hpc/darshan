# -*- coding: utf-8 -*-

"""Auxiliary to discover a darshan-util installation."""


import os

import logging
logger = logging.getLogger(__name__)


verbose_discovery = False
if verbose_discovery:
    logging.basicConfig()           # ensure out streams exist
    logger.setLevel(logging.DEBUG)  # set log-level



class DarshanVersionError(NotImplementedError):
    """
    Raised when using a feature which is not provided by libdarshanutil.
    """
    def __init__(self, target_version, provided_version, msg="Feature"):
        self.msg = msg
        self.target_version = target_version
        self.provided_version = provided_version

    def __repr__(self):
        return "DarshanVersionError('%s')" % str(self)

    def __str__(self):
        return "%s requires libdarshan-util %s, but found %s" % (self.msg, self.target_version, self.provided_version)


def check_version(ffi=None, libdutil=None):
    """
    Get version from shared library or pkg-config and return info.

    :return: Path to a darshan-util installation.
    """
    lib_version = None


    # query library directly (preferred)
    if ffi is not None and libdutil is not None:
        ver = ffi.new("char **")
        ver = libdutil.darshan_log_get_lib_version()
        lib_version = ffi.string(ver).decode("utf-8")
        logger.debug(f" Found lib_version={lib_version} via ffi.")

    # pkgconfig fallback
    if lib_version is None:
        logger.warning("WARNING: Using pk-config fallback.")
        import subprocess
        args = ['pkg-config', '--modversion', 'darshan-util']
        p = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd='.')
        out,err = p.communicate()
        retval = p.wait()
        lib_version = out.decode('ascii').strip()
        logger.debug(f" Found lib_version={lib_version} via pkgconfig.")


    import darshan
   
    package_version = darshan.__darshanutil_version__.split(".")
    lib_version = lib_version.split(".")
    
    if package_version[0:3] != lib_version[0:3]:
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


def discover_darshan_wheel():
    """
    Discovers darshan-util if installed as as part of the wheel.

    :return: Path to a darshan-util installation.
    """

    path = os.path.dirname(__file__)

    if path:
        return os.path.realpath(path + '/../darshan.libs')
    else:
        raise RuntimeError('Could not discover darshan! Is darshan-util installed and set in your PATH?')


def discover_darshan_pyinstaller():
    """
    Discovers darshan-util if installed as as part of a pyinstaller bundle.

    :return: Path to a darshan-util installation.
    """

    path = os.path.dirname(__file__)

    if path:
        return os.path.realpath(path + '/../')
    else:
        raise RuntimeError('Could not discover darshan! Is darshan-util installed and set in your PATH?')



def find_utils(ffi, libdutil):
    """
    Try different methods to discover darshan-util:

    Precedence:
    1) Try if the current environment allows dlopen to load libdarshan-util
    2) Try if darshan-parser is exposed via PATH, and attempt loading relative to it.
    3) Try if darshan is exposed via pkgconfig
    4) Fallback on binary distributed along with Python package

    Args:
        ffi: existing ffi instance to use
        libdutil: reference to libdutil to populate

    """
    if libdutil is None:
        try:
            libdutil = ffi.dlopen("libdarshan-util.so")
        except:
            libdutil = None

    if libdutil is None:
        try:
            libdutil = ffi.dlopen("libdarshan-util.dylib")
        except:
            libdutil = None

    if libdutil is None:
        try:
            library_path = discover_darshan_shutil()
            logger.debug(f"Attempting library_path={library_path} via shutil discovery.")
            libdutil = ffi.dlopen(library_path + "/lib/libdarshan-util.so")
        except:
            libdutil = None

    if libdutil is None:
        try:
            library_path = discover_darshan_pkgconfig()
            logger.debug(f"Attempting library_path={library_path} via pkgconfig discovery.")
            libdutil = ffi.dlopen(library_path + "/lib/libdarshan-util.so")
        except:
            libdutil = None

    if libdutil is None:
        try:
            darshan_path = discover_darshan_wheel()
            import glob
            library_path = glob.glob(f'{darshan_path}/libdarshan-util*.so*')[0]
            logger.debug(f"Attempting library_path={library_path} in case of binary wheel.")
            save = os.getcwd()
            os.chdir(darshan_path)
            libdutil = ffi.dlopen(library_path)
            os.chdir(save)
        except:
            libdutil = None
    
    if libdutil is None:
        try:
            darshan_path = discover_darshan_pyinstaller()
            import glob
            library_path = glob.glob(f'{darshan_path}/libdarshan-util*.so*')[0]
            logger.debug(f"Attempting library_path={library_path} for pyinstaller bundles.")
            save = os.getcwd()
            os.chdir(darshan_path)
            libdutil = ffi.dlopen(library_path)
            os.chdir(save)
        except:
            libdutil = None
  
    
    
    if libdutil is None:
        raise RuntimeError('Could not find libdarshan-util.so! Is darshan-util installed? Please ensure one of the the following: 1) export LD_LIBRARY_PATH=<path-to-libdarshan-util.so>, or 2) darshan-parser can found using the PATH variable, or 3) pkg-config can resolve pkg-config --path darshan-util, or 4) install a wheel that includes darshan-utils via pip.')

    return libdutil



def load_darshan_header():
    """
    Returns a CFFI compatible header for darshan-utlil as a string.

    :return: String with a CFFI compatible header for darshan-util.
    """

    curdir, curfile = os.path.split(__file__)
    filepath = os.path.join(curdir, 'data', 'darshan-api.h')
    # filepath = os.path.join(curdir, 'data', 'generated.h')

    logger.debug(f" load_darshan_header using filepath={filepath}")

    with open(filepath, 'r') as f:
        try:
            return f.read()
        except IOError:
            raise RuntimeError('Failed to read API definition header for darshan.')
