#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import darshan.parser_cffi as parser
import json
import numpy as np
import re

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)



class DarshanReport(object):

    def __init__(self, filename):
        self.filename = filename

        self.report = {'version': 1}
        self.filehashes = {}

        self.log = parser.log_open(self.filename)


    def read_all(self):
        """
        Read all available information from darshan log and return das dictionary.
        """

        self.report["job"] = parser.log_get_job(self.log)
        self.report["exe"] = parser.log_get_exe(self.log)
        self.report["mounts"] = parser.log_get_mounts(self.log)
        self.report["modules"] = parser.log_get_modules(self.log)

        self.report["name_records"] = parser.log_get_name_records(self.log)


        self.report['records'] = {}
        for mod in self.report['modules']:
            self.mod_read_all_records(mod)


        self.summarize()


        self.convert_records()

        pass



    def mod_read_all_records(self, mod, mode='numpy'):
        """
        Reads all records for module

        Args:
            mod (str): Identifier of module to fetch all records
            mode (str): 'numpy' for ndarray (default), 'dict' for python dictionary

        Return:
            None

        """

        unsupported =  ['DXT_POSIX', 'DXT_MPIIO']
        if mod in unsupported:
            # skip mod
            return 

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

        cn = parser.counter_names(mod)
        fcn = parser.fcounter_names(mod)

        self.report['modules'][mod]['counters'] = cn 
        self.report['modules'][mod]['fcounters'] = fcn
        self.report['modules'][mod]['num_records'] = 0

        rec = parser.log_get_generic_record(self.log, mod, structdefs[mod])
        while rec != None:
            # TODO: performance hog and hacky ;)
            #recs = json.dumps(rec, cls=NumpyEncoder)
            #rec = json.loads(recs)

            if mode == 'numpy': 
                self.report['records'][mod].append(rec)
            else:
                c = dict(zip(cn, rec['counters']))
                fc = dict(zip(fcn, rec['fcounters']))
                self.report['records'][mod].append([c, fc])


            self.report['modules'][mod]['num_records'] += 1

            # fetch next
            rec = parser.log_get_generic_record(self.log, mod, structdefs[mod])

        pass




    def mod_agg_iohist(self, mod, mode='append'):
        """
        Generate aggregated histogram for mod_name.

        Args:
            mod_name (str): 

        Return:
            None
        """

        supported = ["POSIX", "MPI-IO"]
        if mod not in supported:
            raise Exception("Unsupported mod_name for aggregated iohist.")


        # convienience
        recs = self.report['records']
        ctx = {}



        # aggragate
        for rec in recs[mod]:
            if mod not in ctx:
                ctx[mod] = rec['counters']
            else:
                ctx[mod] = np.add(ctx[mod], rec['counters'])



        # cleanup and prepare for json serialization
        def fix_name(name):
            name = name.split("_")
            typ = "UNKNOWN"
            if "READ" in name:
                typ = "READ"
            elif "WRITE" in name:
                typ = "WRITE"
            name = "%s_%s_%s" % (typ, name[-2], name[-1])
            return name

        tmp = json.dumps(ctx[mod], cls=NumpyEncoder)
        tmp = json.loads(tmp)
        cn = parser.counter_names(mod)
        c = dict(zip(cn, tmp))
        c = {k:c[k] for k in c if re.match('.*?_SIZE_.*?', k)}
        c = {fix_name(k):v for k, v  in c.items()}
        ctx[mod] = c


        if mode == 'append':
            if 'agg_iohist' not in self.report:
                self.report['agg_iohist'] = {}
            self.report['agg_iohist'][mod] = ctx
        else:
            return ctx



    def agg_ioops(self, mode='append'):
        """
        Compile the I/O operations summary for the current report.

        Args:
            mode (str): Whether to 'append' (default) or to 'return' aggregation. 

        Return:
            None or dict: Depending on mode
        """

        series = [
            {'name': 'POSIX', 'type': 'bar', 'data': [0, 0, 0, 0, 0, 0, 0] }, 
            {'name': 'MPI-IO Indep.', 'type': 'bar', 'data': [0, 0, 0, 0, 0, 0, 0] }, 
            {'name': 'MPI-IO Coll.', 'type': 'bar', 'data': [0, 0, 0, 0, 0, 0, 0] },
            {'name': 'STDIO', 'type': 'bar', 'data': [0, 0, 0, 0, 0, 0, 0] }
        ]


        # convienience
        recs = self.report['records']
        ctx = {}

        # aggragate
        mods = ['MPI-IO', 'POSIX', 'STDIO']
        for mod in mods:
            agg = None
            for rec in recs[mod]:
                if agg is not None:
                    agg = np.add(agg, rec['counters'])
                else:
                    agg = rec['counters']


            # filter fields
            cn = parser.counter_names(mod)
            agg = dict(zip(cn, agg.tolist()))
            

            # append aggregated statistics for module to report
            if mod == 'MPI-IO':
                ctx[mod + ' Indep.'] = agg

                #agg_indep = {
                #    'Read':  agg['MPIIO_'],
                #    'Write': agg['MPIIO_'],
                #    'Open':  agg['MPIIO_'],
                #    'Stat':  agg['MPIIO_'],
                #    'Seek':  agg['MPIIO_'],
                #    'Mmap':  agg['MPIIO_'],
                #    'Fsync': agg['MPIIO_']
                #}

                #ctx[mod + ' Coll.'] = agg
                #agg_coll = {
                #    'Read':  agg['MPIIO_'],
                #    'Write': agg['MPIIO_'],
                #    'Open':  agg['MPIIO_'],
                #    'Stat':  agg['MPIIO_'],
                #    'Seek':  agg['MPIIO_'],
                #    'Mmap':  agg['MPIIO_'],
                #    'Fsync': agg['MPIIO_']
                #}

            else:
                tmp = {
                    'Read':  agg[mod + '_READS'],
                    'Write': agg[mod + '_WRITES'],
                    'Open':  agg[mod + '_OPENS'],
                    'Stat':  0,
                    'Seek':  agg[mod + '_SEEKS'],
                    'Mmap':  0,
                    'Fsync': 0
                }

                if mod == 'POSIX':
                    tmp['Stat']
                    tmp['Stat']
                    tmp['Stat']
                    pass    
                elif mod == 'STDIO':
                    tmp['Stat']
                    tmp['Mmap']
                    tmp['Fsync']
                    pass

                
                ctx[mod] = agg
                ctx[mod + '_final'] = tmp



        # cleanup and prepare for json serialization?
        tmp = json.dumps(ctx, cls=NumpyEncoder)
        ctx = json.loads(tmp)

        # reset summary target
        if mode == 'append':
            self.report['agg_ioops'] = ctx
        else:
            return ctx


    def summarize(self):
        """
        Compile summary 
        """

        self.mod_agg_iohist("MPI-IO")
        self.mod_agg_iohist("POSIX")

        self.agg_ioops()


        pass


    def convert_records(self):
        """
        """

        recs = self.report['records']

        for mod in recs:
            for i, rec in enumerate(self.report['records'][mod]):
                recs[mod][i]['counters'] = rec['counters'].tolist()
                recs[mod][i]['fcounters'] = rec['fcounters'].tolist()


    def report_summary():
        """
        """
        pass
