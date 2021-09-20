"""
PyDarshan provides direct log access for reading binary Darshan logs.
PyDarshan also provides a suite of analysis utilities.
"""

__version__ = '3.3.1.0'
__darshanutil_version__ = '3.3.1'

import logging
logger = logging.getLogger(__name__)


options = {} # type: ignore


#from darshan.backend.cffi_backend import *
from darshan.report import DarshanReport



def enable_experimental(verbose=False):
    """
    Enable experimental features such as aggregation methods for reports.

    Args:
        verbose (bool): Display log of enabled features. (Default: True)

    """
    import os
    import glob
    import importlib
    import darshan    

    for subdir in ['aggregators', 'operations']:
        paths = glob.glob(darshan.__path__[0] + f"/experimental/{subdir}/*.py")
        for path in paths:
            base = os.path.basename(path)
            name = os.path.splitext(base)[0]
            
            if name == "__init__":
                continue

            mod = importlib.import_module(f"darshan.experimental.{subdir}.{name}")
            setattr(DarshanReport, name, getattr(mod, name))
        
            if verbose:
                print(f"Added method {mod.__name__} to DarshanReport.")
