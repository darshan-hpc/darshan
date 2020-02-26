# -*- coding: utf-8 -*-

"""Top-level package for pydarshan."""

__version__ = '0.1.0'


#from darshan.backend.cffi_backend import *
from darshan.report import DarshanReport



def enable_experimental(verbose=True):
    """
    Enable experimental features such as aggregation methods for reports.

    Args:
        verbose (bool): Display log of enabled features. (Default: True)

    """
    import os
    import glob
    import importlib
    import darshan    

    paths = glob.glob(darshan.__path__[0] + "/experimental/aggregators/*.py")
    for path in paths:
        base = os.path.basename(path)
        name = os.path.splitext(base)[0]
        
        if name == "__init__":
            continue

        mod = importlib.import_module('darshan.experimental.aggregators.{0}'.format(name))
        setattr(DarshanReport, name, getattr(mod, name))
    
        if verbose:
            print("Added method {} to DarshanReport.".format(name))
