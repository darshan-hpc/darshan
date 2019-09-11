#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import darshan.parser_cffi as parser
import json
import numpy as np

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


class DarshanReport(object):

    def __init__(self, filename):
        self.filename = filename

        self.report = {}
        self.filehashes = {}

        self.log = parser.log_open(self.filename)


    def read_all(self):
        """ Read all available information from darshan log and return das dictionary.
        """

        self.report["job"] = parser.log_get_job(self.log)
        self.report["exe"] = parser.log_get_exe(self.log)
        self.report["mounts"] = parser.log_get_mounts(self.log)
        self.report["modules"] = parser.log_get_modules(self.log)

        self.report['records'] = {}
        for mod in self.report['modules']:
            self.mod_read_all_records(mod)

        pass



    def mod_read_all_records(self, mod, mode='numpy'):
        """Reads all records for module

            Modes:
                numpy
                dict
        """

        structdefs = {
            "BG/Q": "struct darshan_bgq_record **",
            "HDF5": "struct darshan_hdf5_file **",
            "MPI-IO": "struct darshan_mpiio_file **",
            "PNETCDF": "struct darshan_pnetcdf_file **",
            "POSIX": "struct darshan_posix_file **",
            "STDIO": "struct darshan_stdio_file **",
            "DECAF": "struct darshan_decaf_record **"
        }

        self.report['records'][mod] = []

        rec = parser.log_get_generic_record(self.log, mod, structdefs[mod])
        while rec != None:
            # TODO: performance hog and hacky ;)
            recs = json.dumps(rec, cls=NumpyEncoder)
            rec = json.loads(recs)

            self.report['records'][mod].append(rec)

            # fetch next
            rec = parser.log_get_generic_record(self.log, mod, structdefs[mod])

        pass





    def summarize():
        """
        """
        pass



    def report_summary():
        """
        """
        pass
