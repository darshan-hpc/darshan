#!/usr/bin/env python

import sys

DEBUG = False

def debug_print(string):
    """
    Print debug messages if the module's global debug flag is enabled.
    """
    if DEBUG:
        sys.stderr.write(string + "\n")
    return

def error(string):
    """
    Handle errors generated within TOKIO.  Currently just a passthrough to
    stderr; should probably provide exceptions later on.
    """
    sys.stderr.write(string + "\n")

def warning(string):
    """
    Handle warnings generated within TOKIO.  Currently just a passthrough to
    stderr; should probably provide a more rigorous logging/reporting
    interface later on.
    """
    sys.stderr.write(string + "\n")
