#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
The darshan.repport module provides the DarshanReport class for convienient
interaction and aggregation of Darshan logs using Python.
"""


import darshan.backend.cffi_backend as backend

import json
import re
import copy
import datetime
import sys

import numpy as np
import pandas as pd


class DarshanReportJSONEncoder(json.JSONEncoder):
    """
    Helper class for JSON serialization if the report contains, for example,
    numpy or dates records, which are not handled by the default JSON encoder.
    """
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
    
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()

        return json.JSONEncoder.default(self, obj)



class DarshanReport(object):
    """
    The DarshanReport class provides a convienient wrapper to access darshan
    logs, which also caches already fetched information. In addition to that
    a number of common aggregations can be performed.
    """

    def __init__(self, filename=None, data_format='pandas', automatic_summary=False,
            read_all=True, lookup_name_records=True):
        self.filename = filename

        # options
        self.data_format = data_format  # Experimental: preferred internal representation: pandas/numpy useful for aggregations, dict good for export/REST
                                        # might require alternative granularity: e.g., records, vs summaries?
                                        # vs dict/pandas?  dict/native?
        self.automatic_summary = automatic_summary
        self.lookup_name_records = lookup_name_records


        # state dependent book-keeping
        self.converted_records = False  # true if convert_records() was called (unnumpyfy)

        # 
        self.start_time = float('inf')
        self.end_time = float('-inf')

        # initialize data namespaces
        self.metadata = {}
        self.modules = {}
        self.counters = {}
        self.records = {}
        self.mounts = {}
        self.name_records = {}

        # initialize report/summary namespace
        self.summary_revision = 0       # counter to check if summary needs update
        self.summary = {}


        # legacy references (deprecate before 1.0?)
        self.data_revision = 0          # counter for consistency checks
        self.data = {'version': 1}
        self.data['metadata'] = self.metadata
        self.data['records'] = self.records
        self.data['summary'] = self.summary
        self.data['modules'] = self.modules
        self.data['counters'] = self.counters
        self.data['name_records'] = self.name_records



        # when using report algebra this log allows to untangle potentially
        # unfair aggregations (e.g., double accounting)
        self.provenance_enabled = True
        self.provenance_log = []
        self.provenance_reports = {}


        if filename:
            self.open(filename, read_all=read_all)    


    def open(self, filename, read_all=False):
        """
        Open log file via CFFI backend.

        Args:
            filename (str): filename to open (optional)
            read_all (bool): whether to read all records for log

        Return:
            None

        """

        self.filename = filename

        if filename:
            self.log = backend.log_open(self.filename)
            self.read_metadata(read_all=read_all)

            if read_all:
                self.read_all()



    def __add__(self, other):
        """
        Allow reports to be merged using the addition operator.
        """

        return self.merge(other)


    def __deepcopy__(self, memo):
        """
        Creates a deepcopy of report.

        NOTE: Needed to purge reference to self.log as Cdata can not be pickled:
            TypeError: can't pickle _cffi_backend.CData objects
        """

        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k in ["log"]:
                # blacklist of members not to copy
                continue
            setattr(result, k, copy.deepcopy(v, memo))
        return result

        # TODO: might consider treating self.log as list of open logs to not deactivate load functions?

        return result


    def read_metadata(self, read_all=False):
        """
        Read metadata such as the job, the executables and available modules.

        Args:
            None

        Return:
            None

        """
        self.metadata['job'] = backend.log_get_job(self.log)
        self.metadata['exe'] = backend.log_get_exe(self.log)

        self.start_time = datetime.datetime.fromtimestamp(self.metadata['job']['start_time'])
        self.end_time = datetime.datetime.fromtimestamp(self.metadata['job']['end_time'])

        self.data['mounts'] = backend.log_get_mounts(self.log)
        self.mounts = self.data['mounts']

        self.data['modules'] = backend.log_get_modules(self.log)
        self.modules = self.data['modules']

        if read_all == True:
            self.data["name_records"] = backend.log_get_name_records(self.log)
            self.name_records = self.data['name_records']


    def update_name_records(self, mod=None):
        """
        Update (and prune unused) name records from resolve table.

        First reindexes all used name record identifiers and then queries 
        darshan-utils library to compile filtered list of name records.

        Args:
            None

        Return:
            None

        """

        # sanitize inputs
        mods = mod
        if mods is None:
            mods = self.records
        else:
            mods = [mod]

        
        # state
        ids = set()

        for mod in mods:
            for rec in self.records[mod]:
                ids.add(rec['id'])


        self.name_records = backend.log_lookup_name_records(self.log, ids)
        




    def read_all(self):
        """
        Read all available records from darshan log and return as dictionary.

        Args:
            None

        Return:
            None
        """
        self.read_all_generic_records()
        self.read_all_dxt_records()
        return


    def read_all_generic_records(self, counters=True, fcounters=True):
        """
        Read all generic records from darshan log and return as dictionary.

        Args:
            None

        Return:
            None
        """

        for mod in self.data['modules']:
            self.mod_read_all_records(mod, warnings=False)

        pass


    def read_all_dxt_records(self, reads=True, writes=True):
        """
        Read all dxt records from darshan log and return as dictionary.

        Args:
            None

        Return:
            None
        """

        for mod in self.data['modules']:
            self.mod_read_all_dxt_records(mod, warnings=False, reads=reads, writes=writes)

        pass


    def mod_read_all_records(self, mod, mode='numpy', warnings=True):
        """
        Reads all generic records for module

        Args:
            mod (str): Identifier of module to fetch all records
            mode (str): 'numpy' for ndarray (default), 'dict' for python dictionary

        Return:
            None

        """
        unsupported =  ['DXT_POSIX', 'DXT_MPIIO', 'LUSTRE']

        if mod in unsupported:
            if warnings:
                print("Skipping. Currently unsupported:", mod, "in mod_read_all_records().", file=sys.stderr)
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


        self.modules[mod]['num_records'] = 0

        
        if mod not in self.counters:
            self.counters[mod] = {}
        self.counters[mod]['counters'] = cn 
        self.counters[mod]['fcounters'] = fcn



        rec = backend.log_get_generic_record(self.log, mod, structdefs[mod])
        while rec != None:
            if mode == 'pandas':
                self.records[mod].append(rec)
            if mode == 'numpy': 
                self.records[mod].append(rec)
            else:
                c = dict(zip(cn, rec['counters']))
                fc = dict(zip(fcn, rec['fcounters']))
                self.records[mod].append([c, fc])


            self.modules[mod]['num_records'] += 1

            # fetch next
            rec = backend.log_get_generic_record(self.log, mod, structdefs[mod])

        pass


    def mod_read_all_dxt_records(self, mod, mode='numpy', warnings=True, reads=True, writes=True):
        """
        Reads all dxt records for provided module.

        Args:
            mod (str): Identifier of module to fetch all records
            mode (str): 'numpy' for ndarray (default), 'dict' for python dictionary

        Return:
            None

        """

        if mod not in self.data['modules']:
            if warnings:
                print("Skipping. Log does not contain data for mod:", mod, file=sys.stderr)
            return


        supported =  ['DXT_POSIX', 'DXT_MPIIO']

        if mod not in supported:
            if warnings:
                print("Skipping. Currently unsupported:", mod, 'in mod_read_all_dxt_records().', file=sys.stderr)
            # skip mod
            return 


        structdefs = {
            "DXT_POSIX": "struct dxt_file_record **",
            "DXT_MPIIO": "struct dxt_file_record **",
        }


        self.records[mod] = []
        self.modules[mod]['num_records'] = 0


        if mod not in self.counters:
            self.counters[mod] = {}


        rec = backend.log_get_dxt_record(self.log, mod, structdefs[mod])
        while rec != None:
            if mode == 'numpy': 
                self.records[mod].append(rec)
            else:
                print("Not implemented.")
                exit(1)

                #c = dict(zip(cn, rec['counters']))
                #fc = dict(zip(fcn, rec['fcounters']))
                #self.data['records'][mod].append([c, fc])
                pass


            self.data['modules'][mod]['num_records'] += 1

            # fetch next
            rec = backend.log_get_dxt_record(self.log, mod, structdefs[mod], reads=reads, writes=writes)

        pass


    def info(self, metadata=False):
        """
        Print information about the record for inspection.

        Args:
            None

        Return:
            None
        """

        print("Filename:       ", self.filename, sep="")

        tdelta = self.end_time - self.start_time
        print("Times:          ", self.start_time, " to ", self.end_time, " (Duration ", tdelta, ")", sep="")

        if 'exe' in self.metadata:
            print("Executeable:    ", self.metadata['exe'], sep="")

        if 'job' in self.metadata:
            print("Processes:      ", self.metadata['job']['nprocs'], sep="")
            print("JobID:          ", self.metadata['job']['jobid'], sep="")
            print("UID:            ", self.metadata['job']['uid'], sep="")
            print("Modules in Log: ", list(self.modules.keys()), sep="")

        loaded = {}
        for mod in self.records:
            loaded[mod] = len(self.records[mod])
        print("Loaded Records: ", loaded, sep="")

        print("Name Records:   ", len(self.name_records), sep="")
        
        if 'job' in self.metadata:
            print("Darshan/Hints:  ", self.metadata['job']['metadata'], sep="")
        print("DarshanReport:  id(", id(self), ") (tmp)", sep="")


        if metadata:
            for key, val in self.metadata.items():
                if key == "job":
                    for key2, val2 in self.metadata[key].items():
                        print("metadata['", key ,"']['", key2, "'] = ", val2, sep="")
                else:
                    print("metadata['", key, "'] = ", val, sep="")
    
    
        #def get_size(obj, seen=None):
        #    """Recursively finds size of objects"""
        #    size = sys.getsizeof(obj)
        #    if seen is None:
        #        seen = set()
        #    obj_id = id(obj)
        #    if obj_id in seen:
        #        return 0
        #    # Important mark as seen *before* entering recursion to gracefully handle
        #    # self-referential objects
        #    seen.add(obj_id)
        #    if isinstance(obj, dict):
        #        size += sum([get_size(v, seen) for v in obj.values()])
        #        size += sum([get_size(k, seen) for k in obj.keys()])
        #    elif hasattr(obj, '__dict__'):
        #        size += get_size(obj.__dict__, seen)
        #    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        #        size += sum([get_size(i, seen) for i in obj])
        #    return size

        #print("Memory:", get_size(self), 'bytes')


    def to_dict():
        """
        Return dictionary representation of report data.

        Args:
            None

        Return:
            dict
        """

        data = copy.deepcopy(self.data)

        recs = data['records']
        for mod in recs:
            for i, rec in enumerate(data['records'][mod]):
                recs[mod][i]['counters'] = rec['counters'].tolist()
                recs[mod][i]['fcounters'] = rec['fcounters'].tolist()

        return data


    def to_json(self):
        """
        Return JSON representation of report data as string.

        Args:
            None

        Return:
            JSON String
        """

        data = copy.deepcopy(self.data)

        recs = data['records']
        for mod in recs:
            for i, rec in enumerate(data['records'][mod]):
                recs[mod][i]['counters'] = rec['counters'].tolist()
                recs[mod][i]['fcounters'] = rec['fcounters'].tolist()

        return json.dumps(data, cls=DarshanReportJSONEncoder)
