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

import logging
logger = logging.getLogger(__name__)



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



_structdefs = {
    "BG/Q": "struct darshan_bgq_record **",
    "DXT_MPIIO": "struct dxt_file_record **",
    "DXT_POSIX": "struct dxt_file_record **",
    "H5F": "struct darshan_hdf5_file **",
    "H5D": "struct darshan_hdf5_dataset **",
    "LUSTRE": "struct darshan_lustre_record **",
    "MPI-IO": "struct darshan_mpiio_file **",
    "PNETCDF": "struct darshan_pnetcdf_file **",
    "POSIX": "struct darshan_posix_file **",
    "STDIO": "struct darshan_stdio_file **",
}



class DarshanReport(object):
    """
    The DarshanReport class provides a convienient wrapper to access darshan
    logs, which also caches already fetched information. In addition to that
    a number of common aggregations can be performed.
    """

    # a way to conser memory?
    #__slots__ = ['attr1', 'attr2']


    def __init__(self, 
            filename=None, dtype='pandas', 
            start_time=None, end_time=None,
            automatic_summary=False,
            read_all=True, lookup_name_records=True):
        """
        Args:
            filename (str): filename to open (optional)
            dtype (str): default dtype for internal structures
            automatic_summary (bool): automatically generate summary after loading
            read_all (bool): whether to read all records for log
            lookup_name_records (bool): lookup and update name_records as records are loaded

        Return:
            None

        """
        self.filename = filename

        # Behavioral Options
        self.dtype = dtype  # Experimental: preferred internal representation: pandas/numpy useful for aggregations, dict good for export/REST
                                        # might require alternative granularity: e.g., records, vs summaries?
                                        # vs dict/pandas?  dict/native?
        self.automatic_summary = automatic_summary
        self.lookup_name_records = lookup_name_records

        # State dependent book-keeping
        self.converted_records = False  # true if convert_records() was called (unnumpyfy)


        # Report Metadata
        self.start_time = start_time if start_time else float('inf')
        self.end_time = end_time if end_time else float('-inf')
        self.timebase = self.start_time

        # Initialize data namespaces
        self.metadata = {}
        self.modules = {}
        self.counters = {}
        self.records = {}
        self.mounts = {}
        self.name_records = {}

        # initialize report/summary namespace
        self.summary_revision = 0       # counter to check if summary needs update (see data_revision)
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
        self.provenance_graph = []
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
            if not bool(self.log['handle']):
                raise RuntimeError("Failed to open file.")

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

        .. note::
            Needed to purge reference to self.log as Cdata can not be pickled:
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
            logger.debug(f" Refreshing name_records for mod={mod}")
            for rec in self.records[mod]:
                ids.add(rec['id'])


        self.name_records.update(backend.log_lookup_name_records(self.log, ids))
        

    def read_all(self, dtype=None):
        """
        Read all available records from darshan log and return as dictionary.

        Args:
            None

        Return:
            None
        """

        self.read_all_generic_records(dtype=dtype)
        self.read_all_dxt_records(dtype=dtype)
        self.mod_read_all_lustre_records(dtype=dtype)
        
        return


    def read_all_generic_records(self, counters=True, fcounters=True, dtype=None):
        """
        Read all generic records from darshan log and return as dictionary.

        Args:
            None

        Return:
            None
        """

        dtype = dtype if dtype else self.dtype

        for mod in self.data['modules']:
            self.mod_read_all_records(mod, dtype=dtype, warnings=False)

        pass


    def read_all_dxt_records(self, reads=True, writes=True, dtype=None):
        """
        Read all dxt records from darshan log and return as dictionary.

        Args:
            None

        Return:
            None
        """

        dtype = dtype if dtype else self.dtype

        for mod in self.data['modules']:
            self.mod_read_all_dxt_records(mod, warnings=False, reads=reads, writes=writes, dtype=dtype)

        pass


    def mod_read_all_records(self, mod, dtype=None, warnings=True):
        """
        Reads all generic records for module

        Args:
            mod (str): Identifier of module to fetch all records
            dtype (str): 'numpy' for ndarray (default), 'dict' for python dictionary, 'pandas'

        Return:
            None

        """
        unsupported =  ['DXT_POSIX', 'DXT_MPIIO', 'LUSTRE']

        if mod in unsupported:
            if warnings:
                logger.warning(f" Skipping. Currently unsupported: {mod} in mod_read_all_records().")
            # skip mod
            return 


        # handling options
        dtype = dtype if dtype else self.dtype


        self.data['records'][mod] = []
        cn = backend.counter_names(mod)
        fcn = backend.fcounter_names(mod)

        # update module metadata
        self.modules[mod]['num_records'] = 0
        if mod not in self.counters:
            self.counters[mod] = {}
            self.counters[mod]['counters'] = cn 
            self.counters[mod]['fcounters'] = fcn


        # fetch records
        rec = backend.log_get_generic_record(self.log, mod, _structdefs[mod], dtype=dtype)
        while rec != None:
            if dtype == 'pandas':
                self.records[mod].append(rec)
            if dtype == 'numpy': 
                self.records[mod].append(rec)
            else:
                self.records[mod].append(rec)

            self.modules[mod]['num_records'] += 1

            # fetch next
            rec = backend.log_get_generic_record(self.log, mod, _structdefs[mod], dtype=dtype)


        if self.lookup_name_records:
            self.update_name_records()

        # process/combine records if the format dtype allows for this
        if dtype == 'pandas':
            combined_c = None
            combined_fc = None

            for rec in self.records[mod]:
                obj = rec['counters']
                #print(type(obj))
                #display(obj)
                
                if combined_c is None:
                    combined_c = rec['counters']
                else:
                    combined_c = pd.concat([combined_c, rec['counters']])
                    
                if combined_fc is None:
                    combined_fc = rec['fcounters']
                else:
                    combined_fc = pd.concat([combined_fc, rec['fcounters']])

            self.records[mod] = [{
                'rank': -1,
                'id': -1,
                'counters': combined_c,
                'fcounters': combined_fc
                }]

        pass


    def mod_read_all_dxt_records(self, mod, dtype=None, warnings=True, reads=True, writes=True):
        """
        Reads all dxt records for provided module.

        Args:
            mod (str): Identifier of module to fetch all records
            dtype (str): 'numpy' for ndarray (default), 'dict' for python dictionary

        Return:
            None

        """
        if mod not in self.data['modules']:
            if warnings:
                logger.warning(f"Skipping. Log does not contain data for mod: {mod}")
            return


        supported =  ['DXT_POSIX', 'DXT_MPIIO']

        if mod not in supported:
            if warnings:
                logger.warning(f" Skipping. Unsupported module: {mod} in in mod_read_all_dxt_records(). Supported: {supported}")
            # skip mod
            return 


        # handling options
        dtype = dtype if dtype else self.dtype


        self.records[mod] = []

        # update module metadata
        self.modules[mod]['num_records'] = 0
        if mod not in self.counters:
            self.counters[mod] = {}


        # fetch records
        rec = backend.log_get_dxt_record(self.log, mod, _structdefs[mod], dtype=dtype)
        while rec != None:
            if dtype == 'numpy': 
                self.records[mod].append(rec)
            else:
                self.records[mod].append(rec)

            self.data['modules'][mod]['num_records'] += 1

            # fetch next
            rec = backend.log_get_dxt_record(self.log, mod, _structdefs[mod], reads=reads, writes=writes, dtype=dtype)


        if self.lookup_name_records:
            self.update_name_records()

        pass



    def mod_read_all_lustre_records(self, mod="LUSTRE", dtype=None, warnings=True):
        """
        Reads all dxt records for provided module.

        Args:
            mod (str): Identifier of module to fetch all records
            dtype (str): 'numpy' for ndarray (default), 'dict' for python dictionary

        Return:
            None

        """
        if mod not in self.data['modules']:
            if warnings:
                logger.warning(f" Skipping. Log does not contain data for mod: {mod}")
            return


        supported =  ['LUSTRE']

        if mod not in supported:
            if warnings:
                logger.warning(f" Skipping. Unsupported module: {mod} in in mod_read_all_dxt_records(). Supported: {supported}")
            # skip mod
            return 


        # handling options
        dtype = dtype if dtype else self.dtype


        self.records[mod] = []
        cn = backend.counter_names(mod)

        # update module metadata
        self.modules[mod]['num_records'] = 0
        if mod not in self.counters:
            self.counters[mod] = {}
            self.counters[mod]['counters'] = cn 


        # fetch records
        rec = backend.log_get_record(self.log, mod, dtype=dtype)
        while rec != None:
            self.records[mod].append(rec)
            self.data['modules'][mod]['num_records'] += 1

            # fetch next
            rec = backend.log_get_record(self.log, mod, dtype=dtype)


        if self.lookup_name_records:
            self.update_name_records()

        # process/combine records if the format dtype allows for this
        if dtype == 'pandas':
            combined_c = None

            for rec in self.records[mod]:
                obj = rec['counters']
                #print(type(obj))
                #display(obj)
                
                if combined_c is None:
                    combined_c = rec['counters']
                else:
                    combined_c = pd.concat([combined_c, rec['counters']])
                    

            self.records[mod] = [{
                'rank': -1,
                'id': -1,
                'counters': combined_c,
                }]


        pass




    def mod_records(self, mod, 
                    dtype='numpy', warnings=True):
        """
        Return generator for lazy record loading and traversal.

        .. warning::
            Can't be used for now when alternating between different modules.
            A temporary workaround can be to open the same log multiple times,
            as this ways buffers are not shared between get_record invocations
            in the lower level library.


        Args:
            mod (str): Identifier of module to fetch records for
            dtype (str): 'numpy' for ndarray (default), 'dict' for python dictionary

        Return:
            None

        """
        cn = backend.counter_names(mod)
        fcn = backend.fcounter_names(mod)

        if mod not in self.counters:
            self.counters[mod] = {}
        self.counters[mod]['counters'] = cn 
        self.counters[mod]['fcounters'] = fcn

        rec = backend.log_get_generic_record(self.log, mod, _structdefs[mod], dtype=dtype)
        while rec != None:
            yield rec

            # fetch next
            rec = backend.log_get_generic_record(self.log, mod, _structdefs[mod])


    def info(self, metadata=False):
        """
        Print information about the record for inspection.

        Args:
            metadata (bool): show detailed metadata (default: False)

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


    ###########################################################################
    # Internal Organisation
    ###########################################################################
    def rebase_timestamps(records=None, inplace=False, timebase=False):
        """
        Updates all records in the report to use timebase (defaults: start_time).
        This might allow to conserve memory as reports are merged.

        Args:
            records (dict, list):  records to rebase
            inplace (bool): weather to merel return a copy or to update records
            timebase (datetime.datetime): new timebase to use

        Return:
            rebased_records (same type as provided to records)
        """
        rebase_records = copy.deepcopy(record)

        # TODO: apply timestamp rebase
        # TODO: settle on format

        return rebased_records

    ###########################################################################
    # Conversion 
    ###########################################################################
    def to_dict(self):
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
                try:
                    recs[mod][i]['counters'] = rec['counters'].tolist()
                except KeyError:
                    logger.debug(f" to_json: mod={mod} does not include counters")
                    pass
                    
                try: 
                    recs[mod][i]['fcounters'] = rec['fcounters'].tolist()
                except KeyError:
                    logger.debug(f" to_json: mod={mod} does not include fcounters")
                    pass

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
                try:
                    recs[mod][i]['counters'] = rec['counters'].tolist()
                except KeyError:
                    logger.debug(f" to_json: mod={mod} does not include counters")
                    pass
                    
                try: 
                    recs[mod][i]['fcounters'] = rec['fcounters'].tolist()
                except KeyError:
                    logger.debug(f" to_json: mod={mod} does not include fcounters")
                    pass

        return json.dumps(data, cls=DarshanReportJSONEncoder)




    @staticmethod
    def from_string(string):
        return DarshanReport()
