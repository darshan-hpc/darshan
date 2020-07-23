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
    def __init__(self, target_version, provided_version, msg="Feature"):
        self.msg = msg
        self.target_version = target_version
        self.provided_version = provided_version

    def __repr__(self):
        return "DarshanVersionError('%s')" % str(self)

    def __str__(self):
        return "%s requires libdarshan-util %s, but found %s" % (self.msg, self.target_version, self.provided_version)
