#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
The darshan.repport module provides the DarshanReport class for convienient
interaction and aggregation of Darshan logs using Python.
"""


import darshan.backend.cffi_backend as backend

from darshan.datatypes.heatmap import Heatmap

import json
import re
import copy
import datetime
import sys

import numpy as np
import pandas as pd

import collections.abc

import logging
logger = logging.getLogger(__name__)


class ModuleNotInDarshanLog(ValueError):
    """Raised when module is not present in Darshan log."""
    pass




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


class DarshanRecordCollection(collections.abc.MutableSequence):
    """
    Darshan log records may nest various properties (e.g., DXT, Lustre).
    As such they can not faithfully represented using only a single
    Numpy array or a Pandas dataframe.

    The DarshanRecordCollection is used as a wrapper to offer
    users a stable API to DarshanReports and contained records
    in various popular formats while allowing to optimize 
    memory and internal representations as necessary.
    """

    def __init__(self, mod=None, report=None):     
        super(DarshanRecordCollection, self).__init__()
        self.mod = mod             # collections should be homogenous in module type
        self.report = report       # reference the report offering lookup for, e.g., counter names

        self.rank = None           # if all records in collection share rank, save memory
        self.id = None             # if all records in collection share id/nrec, save memory

        self.timebase = None       # allow fast time rebase without touching every record
        self.start_time = None
        self.end_time = None

        self._type = "collection"  # collection => list(), single => [record], nested => [[], ... ,[]]
        self._records = list()     # internal format before user conversion
    
    def __len__(self):
        return len(self._records)
    
    def __setitem__(self, key, val):
        self._records[key] = val

    def __getitem__(self, key):
        if self._type == "record":
            if isinstance(key, collections.abc.Hashable):
                #TODO: might extend this style access to collection/nested type as well
                #      but do not want to offer an access which might not be feasible to maintain
                return self._records[0][key]
            else:
                return self._records[0]

        # Wrap single record in RecordCollection to attach conversions: to_json, to_dict, to_df, ...
        # This way conversion logic can be shared.
        record = DarshanRecordCollection(mod=self.mod, report=self.report)

        if isinstance(key, slice):
            record._type = "collection"
            record._records = self._records[key]
        else:
            record._type = "record"
            record.append(self._records[key])
        return record

    def __delitem__(self, key):
        del self._list[ii]

    def insert(self, key, val):
        self._records.insert(key, val)

    def append(self, val):
        self.insert(len(self._records), val)


    def __repr__(self):
        if self._type == "record":
            return self._records[0].__repr__()
        
        return object.__repr__(self)

    #def __repr__(self):
    #    print("DarshanRecordCollection.__repr__")
    #    repr = ""
    #    for rec in self._records:
    #        repr += f"{rec}\n"
    #    return repr

    def info(self, describe=False, plot=False):
        """
        Print information about the record for inspection.

        Args:
            describe (bool): show detailed summary and statistics (default: False)
            plot (bool): show plots for quick value overview for counters and fcounters (default: False)

        Return:
            None
        """
        mod = self.mod
        records = self._records

        print("Module:       ", mod, sep="")
        print("Records:      ", len(self), sep="")
        print("Coll. Type:   ", self._type, sep="")

        if mod in ['LUSTRE']:
            for i, rec in enumerate(records):
                pass
        elif mod in ['DXT_POSIX', 'DXT_MPIIO']:
            ids = set()
            ranks = set()
            hostnames = set()
            reads = 0
            writes = 0
            for i, rec in enumerate(records):
                ids.add(rec['id']) 
                ranks.add(rec['rank']) 
                hostnames.add(rec['hostname']) 
                reads += rec['read_count']
                writes += rec['write_count']
            print("Ranks:        ", str(ranks), sep="")
            print("Name Records: ", str(ids), sep="")
            print("Hostnames:    ", str(hostnames), sep="")
            print("Read Events:  ", str(reads), sep="")
            print("Write Events: ", str(writes), sep="")


            if describe or plot:
                logger.warn("No plots/descriptions defined for DXT records info.")

        else:
            ids = set()
            ranks = set()
            for i, rec in enumerate(records):
                ids.add(rec['id']) 
                ranks.add(rec['rank']) 
            print("Ranks:        ", str(ranks), sep="")
            print("Name Records: ", str(ids), sep="")


            if describe or plot:
                df = self.to_df(attach=None)
                pd_max_rows = pd.get_option('display.max_rows')
                pd_max_columns = pd.get_option('display.max_columns')
                pd.set_option('display.max_rows', None)

                if plot:
                    figw = 7
                    lh = 0.3    # lineheight
                    # get number of counters for plot height adjustment
                    nc = self[0]['counters'].size
                    nfc = self[0]['fcounters'].size

                    display(df['counters'].plot.box(vert=False, figsize=(figw, nc*lh)))
                    display(df['fcounters'].plot.box(vert=False, figsize=(figw, nfc*lh)))

                if describe:
                    display(df['counters'].describe().transpose())
                    display(df['fcounters'].describe().transpose())

                pd.set_option('display.max_rows', pd_max_rows)


    ###########################################################################
    # Export Conversions (following the pandas naming conventions)
    ###########################################################################
    def to_numpy(self):
        records = copy.deepcopy(self._records)
        return records

    def to_list(self):
        mod = self.mod
        records = copy.deepcopy(self._records)

        if mod in ['LUSTRE']:
            raise NotImplementedError
        elif mod in ['DXT_POSIX', 'DXT_MPIIO']:
            raise NotImplementedError
        else:
            for i, rec in enumerate(records):
                rec['counters'] = rec['counters'].tolist()
                rec['fcounters'] = rec['fcounters'].tolist()
        return records

    def to_dict(self):
        mod = self.mod
        records = copy.deepcopy(self._records)
        counters = self.report.counters[self.mod]
        if mod in ['LUSTRE']:
            raise NotImplementedError
        elif mod in ['DXT_POSIX', 'DXT_MPIIO']:
            # format already in a dict format, but may offer switches for expansion
            logger.warn("WARNING: The output of DarshanRecordCollection.to_dict() may change in the future.")
        else:
            for i, rec in enumerate(records):
                rec['counters'] = dict(zip(counters['counters'], rec['counters']))
                rec['fcounters'] = dict(zip(counters['fcounters'], rec['fcounters']))
        return records

    def to_json(self):
        records = self.to_list()
        return json.dumps(records, cls=DarshanReportJSONEncoder)

    def to_df(self, attach="default"):
        if attach == "default":
            attach = ['id', 'rank']

        mod = self.mod
        records = copy.deepcopy(self._records)

        if mod in ['LUSTRE']:
            # retrieve the counter column names
            c_cols = self.report.counters[mod]['counters']
            # create the counter dataframe and add a column for the OST ID's
            df_recs = pd.DataFrame.from_records(records)
            counter_df = pd.DataFrame(np.stack(df_recs.counters.to_numpy()), columns=c_cols)
            counter_df["ost_ids"] = df_recs.ost_ids

            if attach:
                if "id" in attach:
                    counter_df.insert(0, "id", df_recs["id"])
                if "rank" in attach:
                    counter_df.insert(0, "rank", df_recs["rank"])

            records = {"counters": counter_df}

        elif mod in ['DXT_POSIX', 'DXT_MPIIO']:
            for rec in records:
                rec['read_segments'] = pd.DataFrame(rec['read_segments'])
                rec['write_segments'] = pd.DataFrame(rec['write_segments'])
        else:
            df_recs = pd.DataFrame.from_records(records)
            # generic records have counter and fcounter arrays to collect
            counter_keys = ["counters", "fcounters"]

            df_list = []
            for ct_key in counter_keys:
                # build the dataframe for the given counter type
                cols = self.report.counters[mod][ct_key]
                df = pd.DataFrame(np.stack(df_recs[ct_key].to_numpy()), columns=cols)
                # attach the id/rank columns
                if attach:
                    if "id" in attach:
                        df.insert(0, "id", df_recs["id"])
                    if "rank" in attach:
                        df.insert(0, "rank", df_recs["rank"])
                df_list.append(df)

            records = {ct_key:df for ct_key, df in zip(counter_keys, df_list)}

        return records


class DarshanReport(object):
    """
    The DarshanReport class provides a convienient wrapper to access darshan
    logs, which also caches already fetched information. In addition to that
    a number of common aggregations can be performed.
    """

    # a way to conserve memory?
    #__slots__ = ['attr1', 'attr2']


    def __init__(self, 
            filename=None, dtype='numpy', 
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
        self.log = None

        # Behavioral Options
        self.dtype = dtype                                  # default dtype to return when viewing records
        self.automatic_summary = automatic_summary
        self.lookup_name_records = lookup_name_records

        # State dependent book-keeping
        self.converted_records = False  # true if convert_records() was called (unnumpyfy)


        # Report Metadata
        #
        # Start/End + Timebase are 
        self.start_time = start_time if start_time else float('inf')
        self.end_time = end_time if end_time else float('-inf')
        self.timebase = self.start_time

        # Initialize data namespaces
        self._metadata = {}
        self._modules = {}
        self._counters = {}
        self.records = {}
        self._mounts = {}
        self.name_records = {}
        self._heatmaps = {}

        # initialize report/summary namespace
        self.summary_revision = 0       # counter to check if summary needs update (see data_revision)
        self.summary = {}


        # legacy references (deprecate before 1.0?)
        self.data_revision = 0          # counter for consistency checks
        self.data = {'version': 1}
        self.data['metadata'] = self._metadata
        self.data['records'] = self.records
        self.data['summary'] = self.summary
        self.data['modules'] = self._modules
        self.data['counters'] = self.counters
        self.data['name_records'] = self.name_records


        # when using report algebra this log allows to untangle potentially
        # unfair aggregations (e.g., double accounting)
        self.provenance_enabled = True
        self.provenance_graph = []
        self.provenance_reports = {}


        if filename:
            self.open(filename, read_all=read_all)    


    @property
    def metadata(self):
        return self._metadata

    @property
    def modules(self):
        return self._modules

    @property
    def counters(self):
        return self._counters

    @property
    def heatmaps(self):
        return self._heatmaps

#    @property
#    def counters(self):
#        return self._counters
#
#    @property
#    def name_records(self):
#        return self._name_records
#
#
#    @property
#    def summary(self):
#        return self._summary
#   
      

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

        self.start_time = datetime.datetime.fromtimestamp(self.metadata['job']['start_time_sec'])
        self.end_time = datetime.datetime.fromtimestamp(self.metadata['job']['end_time_sec'])

        self.data['mounts'] = backend.log_get_mounts(self.log)
        self.mounts = self.data['mounts']

        self.data['modules'] = backend.log_get_modules(self.log)
        self._modules = self.data['modules']

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
        if "LUSTRE" in self.data['modules']:
            self.mod_read_all_lustre_records(dtype=dtype)
        if "APMPI" in self.data['modules']:
            self.mod_read_all_apmpi_records(dtype=dtype)
        if "APXC" in self.data['modules']:
            self.mod_read_all_apxc_records(dtype=dtype)
        if "HEATMAP" in self.data['modules']:
            self.read_all_heatmap_records()
        
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


    def read_all_heatmap_records(self):
        """
        Read all heatmap records from darshan log and return as dictionary.

        .. note::
            As the module is encoded in a name_record, all heatmap data is read
            and then exposed through the report.heatmaps property.

        Args:
            None

        Return:
            None
        """

        if "HEATMAP" not in self.data['modules']:
            raise ModuleNotInDarshanLog("HEATMAP")

        _nrecs_heatmap = {  
            16592106915301738621: "heatmap:POSIX",
            3989511027826779520: "heatmap:STDIO",
            3668870418325792824: "heatmap:MPIIO"
        }

        def heatmap_rec_to_module_name(rec, nrecs=None):
            if rec['id'] in nrecs:
                name = nrecs[rec['id']]
                mod = name.split(":")[1]
            else:
                mod = rec['id']
            return mod

        heatmaps = {}

        # fetch records
        rec = backend._log_get_heatmap_record(self.log)
        while rec is not None:            
            mod = heatmap_rec_to_module_name(rec, nrecs=_nrecs_heatmap)
            if mod not in heatmaps:
                heatmaps[mod] = Heatmap(mod)
            heatmaps[mod].add_record(rec)

            # fetch next
            rec = backend._log_get_heatmap_record(self.log)

        self._heatmaps = heatmaps


    def mod_read_all_records(self, mod, dtype=None, warnings=True):
        """
        Reads all generic records for module

        Args:
            mod (str): Identifier of module to fetch all records
            dtype (str): 'numpy' for ndarray (default), 'dict' for python dictionary, 'pandas'

        Return:
            None

        """
        unsupported =  ['DXT_POSIX', 'DXT_MPIIO', 'LUSTRE', 'APMPI', 'APXC', 'HEATMAP']

        if mod in unsupported:
            if warnings:
                logger.warning(f" Skipping. Currently unsupported: {mod} in mod_read_all_records().")
            # skip mod
            return 


        # handling options
        dtype = dtype if dtype else self.dtype


        self.records[mod] = DarshanRecordCollection(mod=mod, report=self)
        cn = backend.counter_names(mod)
        fcn = backend.fcounter_names(mod)

        # update module metadata
        self._modules[mod]['num_records'] = 0
        if mod not in self.counters:
            self.counters[mod] = {}
            self.counters[mod]['counters'] = cn 
            self.counters[mod]['fcounters'] = fcn


        # fetch records
        rec = backend.log_get_generic_record(self.log, mod, dtype=dtype)
        while rec != None:
            self.records[mod].append(rec)
            self._modules[mod]['num_records'] += 1

            # fetch next
            rec = backend.log_get_generic_record(self.log, mod, dtype=dtype)


        if self.lookup_name_records:
            self.update_name_records(mod=mod)

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


    def mod_read_all_apmpi_records(self, mod="APMPI", dtype=None, warnings=True):
        """ 
        Reads all APMPI records for provided module.

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


        supported =  ['APMPI'] 
        if mod not in supported:
            if warnings:
                logger.warning(f" Skipping. Unsupported module: {mod} in in mod_read_all_apmpi_records(). Supported: {supported}")
            # skip mod
            return

        # handling options
        dtype = dtype if dtype else self.dtype

        self.records[mod] = DarshanRecordCollection(mod=mod, report=self)

        # update module metadata
        self._modules[mod]['num_records'] = 0
        if mod not in self.counters:
            self.counters[mod] = {}

        # fetch records
        # fetch header record
        rec = backend.log_get_apmpi_record(self.log, mod, "HEADER", dtype=dtype)
        while rec != None:
            self.records[mod].append(rec)
            self.data['modules'][mod]['num_records'] += 1

            # fetch next
            rec = backend.log_get_apmpi_record(self.log, mod, "PERF", dtype=dtype)


        if self.lookup_name_records:
            self.update_name_records(mod=mod)


    def mod_read_all_apxc_records(self, mod="APXC", dtype=None, warnings=True):
        """ 
        Reads all APXC records for provided module.

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

        supported =  ['APXC'] 
        if mod not in supported:
            if warnings:
                logger.warning(f" Skipping. Unsupported module: {mod} in in mod_read_all_apxc_records(). Supported: {supported}")
            # skip mod
            return

        # handling options
        dtype = dtype if dtype else self.dtype

        self.records[mod] = DarshanRecordCollection(mod=mod, report=self)
        cn = backend.counter_names(mod)

        # update module metadata
        self._modules[mod]['num_records'] = 0
        if mod not in self.counters:
            self.counters[mod] = {}

        # fetch records
        # fetch header record
        rec = backend.log_get_apxc_record(self.log, mod, "HEADER", dtype=dtype)
        while rec != None:
            self.records[mod].append(rec)
            self.data['modules'][mod]['num_records'] += 1

            # fetch next
            rec = backend.log_get_apxc_record(self.log, mod, "PERF", dtype=dtype)

        if self.lookup_name_records:
            self.update_name_records(mod=mod)


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
                logger.warning(f" Skipping. Log does not contain data for mod: {mod}")
            return


        supported =  ['DXT_POSIX', 'DXT_MPIIO']

        if mod not in supported:
            if warnings:
                logger.warning(f" Skipping. Unsupported module: {mod} in in mod_read_all_dxt_records(). Supported: {supported}")
            # skip mod
            return 


        # handling options
        dtype = dtype if dtype else self.dtype


        self.records[mod] = DarshanRecordCollection(mod=mod, report=self)

        # update module metadata
        self._modules[mod]['num_records'] = 0
        if mod not in self.counters:
            self.counters[mod] = {}


        # fetch records
        rec = backend.log_get_dxt_record(self.log, mod, dtype=dtype)
        while rec != None:
            self.records[mod].append(rec)
            self.data['modules'][mod]['num_records'] += 1

            # fetch next
            rec = backend.log_get_dxt_record(self.log, mod, reads=reads, writes=writes, dtype=dtype)


        if self.lookup_name_records:
            self.update_name_records(mod=mod)




    def mod_read_all_lustre_records(self, mod="LUSTRE", dtype=None, warnings=True):
        """
        Reads all dxt records for provided module.

        Args:
            mod (str): Identifier of module to fetch all records
            dtype (str): 'numpy' for ndarray (default), 'dict' for python dictionary

        Return:
            None

        """
        if mod not in self.modules:
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


        self.records[mod] = DarshanRecordCollection(mod=mod, report=self)
        cn = backend.counter_names(mod)

        # update module metadata
        self._modules[mod]['num_records'] = 0
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
            self.update_name_records(mod=mod)

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





    def mod_records(self, mod, 
                    dtype='numpy', warnings=True):
        """
        Return generator for lazy record loading and traversal.

        .. warning::
            Can't be used for now when alternating between different modules.
            A temporary workaround can be to open the same log multiple times,
            as this way buffers are not shared between get_record invocations
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

        rec = backend.log_get_generic_record(self.log, mod, dtype=dtype)
        while rec != None:
            yield rec

            # fetch next
            rec = backend.log_get_generic_record(self.log, mod, dtype=dtype)


    def info(self, metadata=False):
        """
        Print information about the record for inspection.

        Args:
            metadata (bool): show detailed metadata (default: False)

        Return:
            None
        """
        print("Filename:       ", self.filename, sep="")

        if 'exe' in self.metadata:
            print("Executable:     ", self.metadata['exe'], sep="")

        print("Times:          ", self.start_time, " to ", self.end_time, sep="")

        if 'job' in self.metadata:
            print("Run time:       %.4f (s)" % self.metadata['job']['run_time'], sep="")
            print("Processes:      ", self.metadata['job']['nprocs'], sep="")
            print("JobID:          ", self.metadata['job']['jobid'], sep="")
            print("UID:            ", self.metadata['job']['uid'], sep="")
            print("Modules in Log: ", list(self._modules.keys()), sep="")

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
    # Export Conversions
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
            try:
                #recs[mod] = recs[mod].to_dict()
                recs[mod] = recs[mod].to_list()
            except:
                recs[mod] = "Not implemented."



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
            try:
                recs[mod] = recs[mod].to_list()
            except:
                recs[mod] = "Not implemented."

        return json.dumps(data, cls=DarshanReportJSONEncoder)


    def _cleanup(self):
        """
        Cleanup when deleting object.
        """
        # CFFI/C backend needs to be notified that it can close the log file.
        try:
            if self.log is not None:
                rec = backend.log_close(self.log)
                self.log = None
        except AttributeError:
            # we sometimes observe that i.e., pytest has problems
            # calling _cleanup() because self.log does not exist
            pass


    def __del__(self):
        """ Clean up when deleted or garbage collected (e.g., del-statement) """
        self._cleanup()


    def __enter__(self):
        """ Satisfy API for use with context manager (e.g., with-statement) """
        return self

    def __exit__(self, type, value, traceback):
        """ Cleanup when used by context manager (e.g., with-statement) """
        self._cleanup()
