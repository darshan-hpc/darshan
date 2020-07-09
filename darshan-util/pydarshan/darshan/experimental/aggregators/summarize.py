from darshan.report import *

def summarize(self):
    """
    Compiles a report summary of the records present in the report object.

    Args:
        None

    Return:
        None
    """

    if self.converted_records == True:
        raise('convert_records() was called earlier on this report. ' +
                'Can not aggregate non-numpy arrays. '+
                '(TODO: Consider back-conversion.)')


    self.mod_agg_iohist("MPI-IO")
    self.mod_agg_iohist("POSIX")

    self.agg_ioops()


    pass

