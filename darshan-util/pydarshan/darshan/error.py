""" Darshan Error classes and functions. """



class DarshanBaseError(Exception):
    """
    Base exception class for Darshan errors in Python.
    """
    pass


class DarshanVersionError(NotImplementedError):
    """
    Raised when using a feature which is not provided by libdarshanutil.
    """
    min_version = None

    def __init__(self, min_version, msg="Feature"):
        self.msg = msg
        self.min_version = min_version
        self.version = "0.0.0"

    def __repr__(self):
        return "DarshanVersionError('%s')" % str(self)

    def __str__(self):
        return "%s requires libdarshanutil >= %s, have %s" % (self.msg, self.min_version, self.version)
