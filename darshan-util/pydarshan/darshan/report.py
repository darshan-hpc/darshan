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
    log records, which are not handled by the default json encoder.
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

    def __init__(self, filename, mode='dict'):
        self.filename = filename


        # initialize actual report dictionary
        self.report = {'version': 1}
        self.report['records'] = {}

        

        self.log = backend.log_open(self.filename)

        # state dependent book-keeping
        self.converted_records = False  # true if convert_records() was called (unnumpyfy)

        # when using report algebra this log allows to untangle potentially
        # unfair aggregations (e.g., double accounting)
        self.provenance_log = []



        self.read_metadata()
        self.report["name_records"] = backend.log_get_name_records(self.log)



    def __add__(self, other):
        new_report = self.copy()
        #new_report = copy.deepcopy(self)
        new_report.provenance_log.append(("add", self, other))

        return new_report


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

        for mod in self.report['modules']:
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

        for mod in self.report['modules']:
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
        self.report["job"] = backend.log_get_job(self.log)
        self.report["exe"] = backend.log_get_exe(self.log)
        self.report["mounts"] = backend.log_get_mounts(self.log)
        self.report["modules"] = backend.log_get_modules(self.log)




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


        self.report['records'][mod] = []

        cn = backend.counter_names(mod)
        fcn = backend.fcounter_names(mod)

        self.report['modules'][mod]['counters'] = cn 
        self.report['modules'][mod]['fcounters'] = fcn
        self.report['modules'][mod]['num_records'] = 0






        rec = backend.log_get_generic_record(self.log, mod, structdefs[mod])
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

        if mod not in self.report['modules']:
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


        self.report['records'][mod] = []
        self.report['modules'][mod]['num_records'] = 0






        rec = backend.log_get_dxt_record(self.log, mod, structdefs[mod])
        while rec != None:
            # TODO: performance hog and hacky ;)
            #recs = json.dumps(rec, cls=NumpyEncoder)
            #rec = json.loads(recs)

            if mode == 'numpy': 
                self.report['records'][mod].append(rec)
            else:
                print("Not implemented.")
                exit(1)

                #c = dict(zip(cn, rec['counters']))
                #fc = dict(zip(fcn, rec['fcounters']))
                #self.report['records'][mod].append([c, fc])
                pass


            self.report['modules'][mod]['num_records'] += 1

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


    def mod_agg_iohist(self, mod, mode='append'):
        """
        Generate aggregated histogram for mod_name.

        Args:
            mod_name (str): 

        Return:
            None
        """

        # convienience
        recs = self.report['records']
        ctx = {}


        supported = ["POSIX", "MPI-IO"]
        if mod not in supported:
            raise Exception("Unsupported mod_name for aggregated iohist.")


        # check records for module are present
        if mod not in recs:
            return


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
        cn = backend.counter_names(mod)
        c = dict(zip(cn, tmp))
        c = {k:c[k] for k in c if re.match('.*?_SIZE_.*?', k)}
        c = {fix_name(k):v for k, v  in c.items()}
        ctx = c


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

            # check records for module are present
            if mod not in recs:
                continue

            agg = None
            for rec in recs[mod]:
                if agg is not None:
                    agg = np.add(agg, rec['counters'])
                else:
                    agg = rec['counters']


            # filter fields
            cn = backend.counter_names(mod)
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
                # POSIX and STDIO share most counter names and are handled 
                # together for this reason, except for metadata/sync counter 
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
                ctx[mod + '_simple'] = tmp



        # cleanup and prepare for json serialization?
        tmp = json.dumps(ctx, cls=NumpyEncoder)
        ctx = json.loads(tmp)

        # reset summary target
        if mode == 'append':
            self.report['agg_ioops'] = ctx
        else:
            return ctx



    def create_timeline(self, group_by='rank'):
        """
        Generate/update a timeline from dxt tracing records of current report.

        Args:
            group_by (str): By which factor to group entries (default: rank)
                            Allowed Parameters: rank, filename
        """

        
        self.mod_read_all_dxt_records("DXT_POSIX")
        self.mod_read_all_dxt_records("DXT_MPIIO")


        self.report['timeline'] = {'groups': [], 'items': []}

        
        groups = self.report['timeline']['groups']
        items = self.report['timeline']['items']
        

        start_time = datetime.datetime.fromtimestamp( self.report['job']['start_time'] )



        def groupify(rec, mod):
            for seg in rec['write_segments']:
                seg.update( {'type': 'w'} )

            for seg in rec['read_segments']:
                seg.update( {'type': 'r'} )


            segments = rec['write_segments'] + rec['read_segments']
            segments = sorted(segments, key=lambda k: k['start_time'])
            
            
            start = float('inf')
            end = float('-inf')


            trace = []
            minsize = 0
            for seg in segments:
                trace += [ seg['type'], seg['offset'], seg['length'], seg['start_time'], seg['end_time'] ]

                seg_minsize = seg['offset'] + seg['length']
                if minsize < seg_minsize:
                    minsize = seg_minsize

                if start > seg['start_time']:
                    start = seg['start_time']

                if end < seg['end_time']:
                    end = seg['end_time']

            # reconstruct timestamps
            start = start_time + datetime.timedelta(seconds=start)
            end = start_time + datetime.timedelta(seconds=end)

            rid = "%s:%d:%d" % (mod, rec['id'], rec['rank'])

            item = {
                "id": rid,
                "rank": rec['rank'],
                "hostname": rec['hostname'],
                "filename": rec['filename'],

                "group": rid,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "limitSize": False,  # required to prevent rendering glitches
                "data": {
                    "duration": (end-start).total_seconds(),
                    "start": segments[0]['start_time'],
                    "size": minsize,       # minimal estimated filesize
                    "trace": trace, 
                }
            }

            items.append(item)


            group = {
                "id": rid,
                "content": "[%s] " % (mod) + rec['filename'][-84:],
                "order": seg['start_time']
            }
            groups.append(group)



        supported = ['DXT_POSIX', 'DXT_MPIIO']
        for mod in supported:
            if mod in self.report['records']:
                for rec in self.report['records'][mod]:
                    groupify(rec, mod)






#        self.report['timeline'] = {
# "groups": [
#
#  {                                                                             
#   "id": "17679126075047334459:m10805:0000",                                     
#   "content": "m10805:0000"                                                     
#  },                                                                            
#  {                                                                             
#   "id": "17679126075047334459",                                                 
#   "content": "ted_prog_vars_DOM01_ML_20130424T000430Z.nc [1R 62MiB]",          
#   "order": 36.3767,                                                            
#   "showNested": True,                                                         
#   "nestedGroups": [                                                            
#    "17679126075047334459:m10805:0000"                                           
#   ]                                                                            
#  }
#
#	],  
#
# "items": [
#  {                                                                             
#   "id": "17679126075047334459:m10805:0000",                                     
#   "group": "17679126075047334459:m10805:0000",                                  
#   "start": "2018-06-20T15:46:39.376700",                                       
#   "end": "2018-06-20T15:46:39.931800",                                         
#   "limitSize": False,
#   "data": {                                                                    
#    "duration": 0.5551,                                                         
#    "start": 36.3767,                                                           
#    "size": 65664716,                                                           
#    "trace": [                                                                  
#     "w",24,8,36.3767,36.3771,"w",0,732,36.3774,36.3774,"w",0,1212636,36.3781,36.3791,
#     "w",0,1213996,36.3792,36.3796,"w",0,1214584,36.3798,36.3802,"w",0,1215084,36.3812,36.3817,
#     "w",0,1215084,36.3817,36.3822,"w",0,1220188,36.3826,36.383,"w",0,4194304, 36.4375 ,36.4405,
#     "r",8388608,0,36.441,36.441,"w",4194304,4194304,36.4734,36.477,"r",12582912,0,36.4774,36.4775,
#     "w",20971520,4194304,36.6157,36.619,"r",29360128,0,36.6195,36.6196,"w",25165824,4194304,36.6513,36.6547,
#     "r",33554432,0,36.6552,36.6552,"w",29360128,4194304,36.6873,36.6911,"r",37748736,0,36.6916,36.6916,
#     "w",33554432,4194304,36.7222,36.7255,"r",41943040,0,36.726,36.7261,"w",37748736,4194304,36.7585,36.7618,
#     "r",46137344,0,36.7623,36.7623,"w",41943040,4194304,36.7933,36.7966,"r",50331648,0,36.7971,36.7971,
#     "w",46137344,4194304,36.8297,36.8331, "r",54525952,0,36.8335,36.8336,"w",50331648,4194304,36.8656,36.8689,
#     "r",58720256,0,36.8694,36.8694,"w",54525952,4194304,36.9009,36.9042,"r",62914560,0,36.9047,36.9047,"w",58720256,6944460,36.9255,36.9318
#    ]                                                                           
#   }                                                                            
#  },                                                                            
#  {                                                                             
#   "id": "17679126075047334459",                                                 
#   "group": "17679126075047334459",                                              
#   "content": "",                                                               
#   "start": "2018-06-20T15:46:39.376700",                                       
#   "end": "2018-06-20T15:46:39.931800",                                         
#   "limitSize": False,
#   "data": {                                                                    
#    "duration": 0.5551,                                                         
#    "start": 36.3767,                                                           
#    "size": 65664716,                                                           
#    "trace": [                                                                  
#     "w",24,8,36.3767,36.3771,"w",0,732,36.3774,36.3774,"w",0,1212636,36.3781,36.3791,
#     "w",0,1213996,36.3792,36.3796,"w",0,1214584,36.3798,36.3802,"w",0,1215084,36.3812,36.3817,
#     "w",0,1215084,36.3817,36.3822,"w",0,1220188,36.3826,36.383,"w",0,4194304,36.4375,36.4405,
#     "w",8388608,4194304,36.5089,36.5123,"r",16777216,0,36.5128,36.5129,"w",12582912,4194304,36.5436,36.5469,
#     "r",20971520,0,36.5474,36.5474,"w", 16777216,4194304,36.5806,36.5839,"r",25165824,0,36.5844,36.5844,
#     "w",20971520,4194304,36.6157,36.619,"r",29360128,0,36.6195,36.6196,"w",25165824,4194304,36.6513,36.6547,
#     "r",33554432,0,36.6552,36.6552,"w",29360128,4194304,36.6873,36.6911,"r",37748736,0,36.6916,36.6916,
#     "r",46137344,0,36.7623,36.7623,"w",41943040,4194304,36.7933,36.7966,"r",50331648,0,36.7971,36.7971,
#     "w",46137344,4194304,36.8297,36.8331, "r",54525952,0,36.8335,36.8336,"w",50331648,4194304,36.8656,36.8689,
#     "r",58720256,0,36.8694,36.8694,"w",54525952,4194304,36.9009,36.9042,"r",62914560,0,36.9047,36.9047,"w",58720256,6944460,36.9255,36.9318
#    ]                                                                           
#   }                                                                            
#  }
#
# ] 
#}



    def create_sankey(self):
        """
        Generate a summary that shows the dataflow between ranks, files and
        their mountpoints.
        """

        # convienience
        recs = self.report['records']
        nrecs = self.report['name_records']
        ctx = {}


        # check records for module are present
        if 'POSIX' not in recs:
            self.report['sankey'] = None
            return

        ranks = {}
        files = {}
        mounts = {}

        nodes = {}
        edges = {}


        # build mnt list
        mnts = []
        for mnt in self.report['mounts']:
            mnts.append(mnt[0])


        # collect records
        for rec in recs['POSIX']:
            
            rnk = "r_%d" % (rec['rank'])

            fnr = rec['id']
            fnr = nrecs[fnr]

            # determine mount point
            mnt = None
            for curr in mnts:
                if re.search(curr, fnr):
                    mnt = curr
                    break
            mnt = "m_%s" % mnt

           
            nodes[rnk] = {'name': rnk}
            nodes[fnr] = {'name': fnr}
            nodes[mnt] = {'name': mnt}

            #rnk2fnr += 1
            #fnr2mnt += 1


            rnk2fnr = "%s->%s" % (rnk, fnr)
            fnr2mnt = "%s->%s" % (fnr, mnt)

            if rnk2fnr not in edges:
                edges[rnk2fnr] = {"value": 0, "source": rnk, "target": fnr}
            edges[rnk2fnr]["value"] += 1


            if fnr2mnt not in edges:
                edges[fnr2mnt] = {"value": 0, "source": fnr, "target": mnt}
            edges[fnr2mnt]["value"] += 1


        ctx = {
            "nodes": list(nodes.values()),
            "links": list(edges.values())
        }


        tmp = json.dumps(ctx, cls=NumpyEncoder)
        tmp = json.loads(tmp)

        self.report['sankey'] = tmp




    def create_time_summary(self):
        """
        TODO: port to new object report

        """

        raise("Not implemented.")


        # Original, Target:
        ## <type>, <app time>, <read>, <write>, <meta>
        #POSIX, 98.837925, 0.150075, 0.5991, 0.4129
        #MPI-IO, 97.293875, 0.051575, 0.126525, 2.528025
        #STDIO, 99.261425, 0, 0.738575, 0

        # convienience links
        summary = logdata['summary']
        time_summary = logdata['time-summary.dat']

        runtime = float(logdata['runtime'])
        nprocs = int(logdata['nprocs'])

        for layer in ['POSIX', 'MPIIO', 'STDIO']:
            if (layer + '_OPENS') in summary or (layer + '_INDEP_OPENS') in summary :

                entry = {
                    'type': layer, 'app_time': None, 'read': None, 'write': None, 'meta': None
                    }

                io_time = 0.0
                for op in ['READ', 'WRITE', 'META']:
                    val = float(summary[layer + '_F_' + op + '_TIME'])
                    io_time += val
                    entry[op.lower()] = (val / (runtime * nprocs)) * 100

                entry['app_time'] = ((runtime * nprocs - io_time) / (runtime * nprocs)) * 100
                time_summary[layer] = entry






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


    def convert_records(self):
        """
        Helper that converts all records to lists instead of numpy arrays.

        Args:
            None

        Return:
            None
        """

        recs = self.report['records']

        for mod in recs:
            for i, rec in enumerate(self.report['records'][mod]):
                recs[mod][i]['counters'] = rec['counters'].tolist()
                recs[mod][i]['fcounters'] = rec['fcounters'].tolist()

        self.converted_records = True


