#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
The darshan.repport module provides the DarshanReport class for convienient
interaction and aggregation of Darshan logs using Python.
"""


import darshan.backend.cffi_backend as backend
import json
import numpy as np
import re
import copy
import datetime



class NumpyEncoder(json.JSONEncoder):
    """
    Helper class for JSON serialization if the report contains numpy
    log records, which are not handled by the default JSON encoder.
    """
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)



class DarshanReport(object):
    """
    The DarshanReport class provides a convienient wrapper to access darshan
    logs, which also caches already fetched information. In addition to that
    a number of common aggregations can be performed.
    """

    def __init__(self, filename=None, data_format='numpy', automatic_summary=False):
        self.filename = filename

        # options
        self.data_format = data_format    # Experimental: preferred internal representation: numpy useful for aggregations, dict good for export/REST
                                          # might require alternative granularity: e.g., records, vs summaries?
        self.automatic_summary = automatic_summary

        # state dependent book-keeping
        self.converted_records = False    # true if convert_records() was called (unnumpyfy)


        # initialize data namespace
        self.data_revision = 0          # counter for consistency checks
        self.data = {'version': 1}
        self.data['records'] = {}
        self.data['metadata'] = {}


        # initialize report/summary namespace
        self.summary_revision = 0       # counter to check if summary needs update
        self.summary = {}



        # when using report algebra this log allows to untangle potentially
        # unfair aggregations (e.g., double accounting)
        self.provenance_log = []
        self.provenance_reports = {}


        if filename:
            self.log = backend.log_open(self.filename)
            self.read_metadata()
            self.data["name_records"] = backend.log_get_name_records(self.log)





    def __add__(self, other):
        nr = DarshanReport()
        nr.provenance_reports[self.filename] = copy.copy(self)
        nr.provenance_reports[other.filename] = copy.copy(other)
        nr.provenance_log.append(("add", self, other, datetime.datetime.now()))

        # pull in records



        return nr


    def read_all(self):
        self.read_all_generic_records()
        self.read_all_dxt_records()
        return


    def read_all_generic_records(self):
        """
        Read all available information from darshan log and return as dictionary.

        Args:
            None

        Return:
            None
        """

        for mod in self.data['modules']:
            self.mod_read_all_records(mod)

        pass


    def read_all_dxt_records(self):
        """
        Read all available information from darshan log and return as dictionary.

        Args:
            None

        Return:
            None
        """

        for mod in self.data['modules']:
            self.mod_read_all_dxt_records(mod)

        pass


    def read_metadata(self):
        """
        Read metadata such as the job, the executables and available modules.

        Args:
            None

        Return:
            None

        """
        self.data['metadata']['job'] = backend.log_get_job(self.log)
        self.data['metadata']['exe'] = backend.log_get_exe(self.log)
        self.data['metadata']['mounts'] = backend.log_get_mounts(self.log)

        self.data['modules'] = backend.log_get_modules(self.log)


    def mod_read_all_records(self, mod, mode='numpy'):
        """
        Reads all generic records for module

        Args:
            mod (str): Identifier of module to fetch all records
            mode (str): 'numpy' for ndarray (default), 'dict' for python dictionary

        Return:
            None

        """
        unsupported =  ['DXT_POSIX', 'DXT_MPIIO', 'LUSTRE']
        unsupported.append('STDIO')   # TODO: reenable when segfault resolved

        if mod in unsupported:
            print("Skipping. Currently unsupported:", mod)
            # skip mod
            return 


        structdefs = {
            "BG/Q": "struct darshan_bgq_record **",
            "HDF5": "struct darshan_hdf5_file **",
            "MPI-IO": "struct darshan_mpiio_file **",
            "PNETCDF": "struct darshan_pnetcdf_file **",
            "POSIX": "struct darshan_posix_file **",
            "STDIO": "struct darshan_stdio_file **",
            "DECAF": "struct darshan_decaf_record **",
            "DXT_POSIX": "struct dxt_file_record **",
        }


        self.data['records'][mod] = []

        cn = backend.counter_names(mod)
        fcn = backend.fcounter_names(mod)

        self.data['modules'][mod]['counters'] = cn 
        self.data['modules'][mod]['fcounters'] = fcn
        self.data['modules'][mod]['num_records'] = 0



        rec = backend.log_get_generic_record(self.log, mod, structdefs[mod])
        while rec != None:
            # TODO: performance hog and hacky ;)
            #recs = json.dumps(rec, cls=NumpyEncoder)
            #rec = json.loads(recs)

            if mode == 'numpy': 
                self.data['records'][mod].append(rec)
            else:
                c = dict(zip(cn, rec['counters']))
                fc = dict(zip(fcn, rec['fcounters']))
                self.data['records'][mod].append([c, fc])


            self.data['modules'][mod]['num_records'] += 1

            # fetch next
            rec = backend.log_get_generic_record(self.log, mod, structdefs[mod])

        pass


    def mod_read_all_dxt_records(self, mod, mode='numpy'):
        """
        Reads all dxt records for provided module.

        Args:
            mod (str): Identifier of module to fetch all records
            mode (str): 'numpy' for ndarray (default), 'dict' for python dictionary

        Return:
            None

        """

        if mod not in self.data['modules']:
            print("Skipping. Log does not contain data for mod:", mod)
            return


        supported =  ['DXT_POSIX', 'DXT_MPIIO']

        if mod not in supported:
            print("Skipping. Currently unsupported:", mod)
            # skip mod
            return 


        structdefs = {
            "DXT_POSIX": "struct dxt_file_record **",
            "DXT_MPIIO": "struct dxt_file_record **",
        }


        self.data['records'][mod] = []
        self.data['modules'][mod]['num_records'] = 0



        rec = backend.log_get_dxt_record(self.log, mod, structdefs[mod])
        while rec != None:
            # TODO: performance hog and hacky ;)
            #recs = json.dumps(rec, cls=NumpyEncoder)
            #rec = json.loads(recs)

            if mode == 'numpy': 
                self.data['records'][mod].append(rec)
            else:
                print("Not implemented.")
                exit(1)

                #c = dict(zip(cn, rec['counters']))
                #fc = dict(zip(fcn, rec['fcounters']))
                #self.data['records'][mod].append([c, fc])
                pass


            self.data['modules'][mod]['num_records'] += 1

            # fetch next
            rec = backend.log_get_dxt_record(self.log, mod, structdefs[mod])

        pass


    def mod_agg(self, mod, ranks=None, files=None, preserve_rank=False, preserve_file=False):
        """
        Aggregate counters for a given module name and return updated dictionary.
        
        Args:
            mod (str): Name of the mod to aggregate.
            ranks (int or list): Only aggregate if rank is matched
            files (int or list): Only aggregate if file is matched
            preserve_rank: do not collapse ranks into single value
            preserve_file: do not collapse files into single value

        Return:
            List of aggregated records
        """


        # TODO: assert

        c = None
        fc = None

        # aggragate
        for rec in recs[mod]:
            if mod not in ctx:
                c = rec['counters']
                fc = rec['counters']
            else:
                c = np.add(ctx[mod], rec['counters'])
                fc = np.add(ctx[mod], rec['fcounters'])

        return {'counters': c, 'fcounter': fc}


    def convert_records(self):
        """
        Helper that converts all records to lists instead of numpy arrays.

        Args:
            None

        Return:
            None
        """

        recs = self.data['records']

        for mod in recs:
            for i, rec in enumerate(self.data['records'][mod]):
                recs[mod][i]['counters'] = rec['counters'].tolist()
                recs[mod][i]['fcounters'] = rec['fcounters'].tolist()

        self.converted_records = True


