# -*- coding: utf-8 -*-

import re
import subprocess
import json
import pprint


CONFIG = {
    "PREFIX": "",
    "darshan-parser": "darshan-parser"
}


class AsciiDarshanLog(object):
    def __init__(self, logfilepath=None):

        self.setup()

        if logfilepath is not None:
            self.load(logfilepath)

    def load(self, logfilepath):
        # call darshan-parser to dump contents of binary log file
        cmd = [CONFIG['darshan-parser'], '--base', '--perf', logfilepath]
        p = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd='.')
        out,err = p.communicate()
        retval = p.wait()

        # parse darshan-parser output
        self.parse_output(out)

    def setup(self):
        self.logdata = {
            ###########################################################################
            # Data collected by original parser (with only a few minimal changes)
            ###########################################################################

            "cmdline": None,
            "nprocs": None,
            "runtime": None,
            "starttime": None,
            "endtime": None,        # unparsed in original
            "uid": None,
            "jobid": None,
            "version": None,

            "partial_flag": 0,

            # access aggregations
            "access_hash": {'POSIX': {}, 'MPIIO': {}},
            "access_size": {},
            "hash_files": {},

            # data structures for calculating performance
            "hash_unique_file_time": {},
            "shared_file_time": 0.0,
            "total_job_bytes": 0,

            "summary": {},   # Surprisingly, hash map is not defined before line 212 in original?

            # cumlative counters
            "cumul_read_indep": 0,
            "cumul_read_bytes_indep": 0,
            "cumul_read_shared": 0,
            "cumul_read_bytes_shared": 0,

            "cumul_write_indep": 0,
            "cumul_write_bytes_indep": 0,
            "cumul_write_shared": 0,
            "cumul_write_bytes_shared": 0,

            "cumul_meta_shared": 0,
            "cumul_meta_indep": 0,

            # Perf summaries per layer
            "perf_est": {},
            "perf_mbytes": {},

            "stdio_perf_est": 0.0,   # Not used by this parser: use perf_est['mylayername'] instead
            "stdio_perf_mbytes": 0,  # Not used by this parser: use perf_mbytes['mylayername'] instead

            "file_record_hash": {},

            "fs_data": {},
            "mt_data": {},      # JL: added afterwards e.g. for sankey, mt points
            "fstype_data": {},  # JL: added afterwards e.g. for sankey, fs_type


            ###########################################################################
            # Data which is originally written to seperate files (dat, tex tables, ...)
            ###########################################################################

            # 1) dat files used by gnuplot

            # start, rank, delta, end  +  use runtime field to set full timespan in plot
            #'file-access': {},
            'file-access-read.dat': [],
            'file-access-read-sh.dat': [],

            'file-access-write.dat': [],
            'file-access-write-sh.dat': [],

            'file-access-open.dat': [],      # JL: not collected by original summary
            'file-access-open-sh.dat': [],   # JL: not collected by original summary
            'file-access-close.dat': [],     # JL: not collected by original summary
            'file-access-close-sh.dat': [],  # JL: not collected by original summary


            # by operation type
            "time-summary.dat": {},

            # op counts
            'op_counts': {},
            'posix-op-counts.dat': None,
            'mpiio-op-counts.dat': None,
            'stdio-op-counts.dat': None,

            # access sizes
            'posix-access-hist.dat': {},
            'mpiio-access-hist.dat': {},

            # ops breakdown by read,write and total,conseq,sequential
            'pattern.dat': None,


            # more output files
            # 2) tex files for tables in latex
            'access-table.tex': {'fields': [], 'data': []},
            'file-count-table.tex': {'fields': [], 'data': []},

            'fs-data-table.tex': {'fields': [], 'data': []},
            'variance-table.tex': {'fields': [], 'data': []},


            ###########################################################################
            # Fields which were not collected by original parser
            ###########################################################################

            # Note: agg_perf_* were not collected by original parser
            # (except slowest with a different name -> perf_mbytes)
            "agg_perf_by_cumul": {},
            "agg_perf_by_open": {},
            "agg_perf_by_open_lastio": {},
            "agg_perf_by_slowest": {},

            "total_bytes": {},


            # All the partial records e.g., for easy insertion into RDMS e.g. sqlite?
            "records": {
                "fields": ["module", "rank", "record id", "counter", "value", "file name", "mount pt", "fs type"],
                "data": [],
            },


        }

        # helper/memory variables for parser
        self.current_module = None
        self.last_x_start = {'OPEN': None, 'READ': None, 'WRITE': None, 'CLOSE': None}

    def parse_info_line(self, line):
        """Parses info lines
        """
        current_module = self.current_module
        logdata = self.logdata

        fields = line.split(":")

        if re.match(r'^# exe: ', line):
            logdata['cmdline'] = fields[1].strip()

        elif re.match('^# nprocs: ', line):
            logdata['nprocs'] = int(fields[1])

        elif re.match('^# run time: ', line):
            logdata['runtime'] = fields[1].strip()

        elif re.match('^# start_time: ', line):
            logdata['starttime'] = int(fields[1])

        elif re.match('^# end_time: ', line):
            # Note: not included by original parser
            logdata['endtime'] = int(fields[1])

        elif re.match('^# uid: ', line):
            logdata['uid'] = fields[1].strip()

        elif re.match('^# jobid: ', line):
            logdata['jobid'] = fields[1].strip()

        elif re.match('^# darshan log version: ', line):
            logdata['version'] = fields[1].strip()

        elif re.match('^# agg_perf_by_cumul: ', line):
            # Note: not included by original parser
            logdata['agg_perf_by_cumul'][current_module] = float(fields[1])

        elif re.match('^# agg_perf_by_open: ', line):
            # Note: not included by original parser
            logdata['agg_perf_by_open'][current_module] = float(fields[1])

        elif re.match('^# agg_perf_by_open_lastio: ', line):
            # Note: not included by original parser
            logdata['agg_perf_by_open_lastio'][current_module] = float(fields[1])

        elif re.match('^# agg_perf_by_slowest: ', line):
            # JL: should the fields really be renamed?
            logdata['perf_est'][current_module] = fields[1].strip()
            logdata['agg_perf_by_slowest'][current_module] = float(fields[1])

        elif re.match('^# total_bytes: ', line):
            # JL: should the fields really be renamed?
            logdata['perf_mbytes'][current_module] = int(fields[1]) / 1024 / 1024
            logdata['total_bytes'][current_module] = int(fields[1])

        elif re.match('^# \*WARNING\*: .* contains incomplete data!', line):
            logdata['partion_flag'] = True

        elif re.match('^# (.+) module data', line):
            result = re.match('^# (.+) module data', line)
            current_module = result.group(1)

    def parse_record_timestamp(self, line, record):
        last_x_start = self.last_x_start
        logdata = self.logdata

        #<module>	<rank>	<record id>	<counter>	<value>	<file name>	<mount pt>	<fs type>
        module = record[0]
        rank = record[1]
        record_id = record[2]
        counter = record[3]
        value = record[4]
        file_name = record[5]
        mount_pt = record[6]
        fs_type = record[7]

        result = re.findall(r'(POSIX|STDIO)_F_(OPEN|READ|WRITE|CLOSE)_(START|END)_TIMESTAMP', counter)
        #                        0                   1                   2
        if len(result):
            layer = result[0][0]
            op_type = result[0][1]
            marker = result[0][2]

            if marker == 'START':
                last_x_start[op_type] = float(value)
            else:
                delta = float(value) - last_x_start[op_type]
                # start, rank, delta, end
                entry = [last_x_start[op_type], int(rank), delta, float(value)]

                if rank == -1:
                    logdata['file-access-' + op_type.lower() + '-sh.dat'].append(entry)
                else:
                    logdata['file-access-' + op_type.lower() + '.dat'].append(entry)


    def parse_record_access(self, line, record):
        logdata = self.logdata

        #<module>	<rank>	<record id>	<counter>	<value>	<file name>	<mount pt>	<fs type>
        module = record[0]
        rank = record[1]
        record_id = record[2]
        counter = record[3]
        value = record[4]
        file_name = record[5]
        mount_pt = record[6]
        fs_type = record[7]

        result = re.findall(r'(POSIX|MPIIO)_ACCESS([0-9]+)_(ACCESS|COUNT)', counter)
        #                           0                1            2
        if len(result):
            layer = result[0][0]
            access_type = result[0][1]
            counter_type = result[0][2]

            if counter_type == 'ACCESS':
                logdata['access_size'][access_type] = int(value)
                pass
            elif counter_type == 'COUNT':
                tmpsize = logdata['access_size'][access_type]
                logdata['access_hash'][layer][tmpsize] = int(value)
                pass


    def parse_record_line(self, line):
        current_module = self.current_module

        logdata = self.logdata


        record = line.split("\t")
        logdata["records"]["data"].append(record)

        #<module>	<rank>	<record id>	<counter>	<value>	<file name>	<mount pt>	<fs type>
        module = record[0]
        rank = record[1]
        record_id = record[2]
        counter = record[3]
        value = record[4]
        file_name = record[5]
        mount_pt = record[6]
        fs_type = record[7]

        # ensure summary has counter
        if counter not in logdata['summary']:
            logdata['summary'][counter] = 0

        logdata['summary'][counter] += float(value)


        # ensure fs_data has file
        if file_name not in logdata['fs_data']:
            logdata['fs_data'][file_name] = {'read': 0, 'written': 0}

        # ensure mt_data has mount_point
        if mount_pt not in logdata['mt_data']:
            logdata['mt_data'][mount_pt] = {'read': 0, 'written': 0}

        # ensure mt_data has mount_point
        if fs_type not in logdata['fstype_data']:
            logdata['fstype_data'][fs_type] = {'read': 0, 'written': 0}


        # ensure record is dict
        if record_id not in logdata['file_record_hash']:
            logdata['file_record_hash'][record_id] = {}
            logdata['file_record_hash'][record_id]['FILE_NAME'] = file_name
            logdata['file_record_hash'][record_id]['RANK'] = rank

        logdata['file_record_hash'][record_id][counter] = value


        # accumulate independent and shared data as well as fs data
        if re.match(r'(POSIX|STDIO)_F_READ_TIME', counter):
            if rank == -1:
                logdata['cumul_read_shared'] += float(value)
            else:
                logdata['cumul_read_indep'] += float(value)

        elif re.match(r'(POSIX|STDIO)_F_WRITE_TIME', counter):
            if rank == -1:
                logdata['cumul_write_shared'] += float(value)
            else:
                logdata['cumul_write_indep'] += float(value)

        elif re.match(r'(POSIX|STDIO)_F_META_TIME', counter):
            if rank == -1:
                logdata['cumul_meta_shared'] += float(value)
            else:
                logdata['cumul_meta_indep'] += float(value)

        # TODO: Stub to melt down cumul_ logic into list of counters.. (slightly less efficient maybe)
        #if re.search(r'_TIME$', counter):
        #    result = re.findall(r'(.*?)_F_(.*?)_TIME$', counter)
        #    #                       0       1
        #    if len(result):
        #        layer = result[0][0]
        #        op_type = result[0][1]


        elif re.match(r'(POSIX|STDIO)_BYTES_READ', counter):
            if rank == -1:
                logdata['cumul_read_bytes_shared'] += int(value)
            else:
                logdata['cumul_read_bytes_indep'] += int(value)

            # fs_data counter
            logdata['fs_data'][file_name]['read'] += int(value)
            logdata['mt_data'][mount_pt]['read'] += int(value)
            logdata['fstype_data'][fs_type]['read'] += int(value)

        elif re.match(r'(POSIX|STDIO)_BYTES_WRITTEN', counter):
            if rank == -1:
                logdata['cumul_write_bytes_shared'] += int(value)
            else:
                logdata['cumul_write_bytes_indep'] += int(value)

            # fs_data counter
            logdata['fs_data'][file_name]['written'] += int(value)
            logdata['mt_data'][mount_pt]['written'] += int(value)
            logdata['fstype_data'][fs_type]['written'] += int(value)


        # record start and end of reads and writes
        elif re.search(r'TIMESTAMP$', counter):
            ret = self.parse_record_timestamp(line, record)

        # record common access counter info
        elif re.search(r'ACCESS', counter):
            ret = self.parse_record_access(line, record)


    def process_file_record(self, file_record_id):
        logdata = self.logdata

        # make some fields easily accessible for convienience
        file_record = logdata['file_record_hash'][file_record_id]
        rank = int(file_record['RANK'])

        #pprint.pprint(file_record)

        # ignore data records that don't have POSIX & MPI data
        if all(e not in file_record  for e in ['POSIX_OPENS', 'STDIO_OPEN']):
            return

        # also ignore if file wasn't really opened, just stat probably
        cond1 = 'POSIX_OPENS' not in file_record  or  file_record['POSIX_OPENS'] == 0
        cond2 = 'STDIO_OPENS' not in file_record  or  file_record['STDIO_OPENS'] == 0
        cond3a = 'MPIIO_INDEP_OPENS' not in file_record
        cond3b = cond3a  or  (file_record['MPIIO_INDEP_OPENS'] == 0 and file_record['MPIIO_COLL_OPENS'] == 0)
        if cond1 and cond2  or  cond3b:
            return


        # ensure this file has an entry from now on
        if file_record_id not in logdata['hash_files']:
            logdata['hash_files'][file_record_id] = {
                    'min_open_size': 0,
                    'max_size': 0,

                    'was_read': False,
                    'was_written': False,

                    'procs': None,
                    #'slowest_rank': None,
                    #'slowest_time': None,
                    #'slowest_bytes': None,

                    #'fastest_rank': None,
                    #'fastest_time': None,
                    #'fastest_bytes': None,

                    #'variance_time': None,
                    #'variance_bytes': None,

                    #'variance_time_S': None,
                    #'variance_time_T': None,
                    #'variance_time_n': None,
                    #'variance_bytes_S': None,
                    #'variance_bytes_T': None,
                    #'variance_bytes_n': None,
                }
        hash_file = logdata['hash_files'][file_record_id]


        hash_file['min_open_size'] = 0  # smallest open size by any rank XXX: not doable? counter dropped early?
        hash_file['name'] = file_record['FILE_NAME']

        # multiple posix/stdio related counters
        for layer in ['POSIX', 'STDIO']:
            if (layer + '_OPENS') in file_record:
                # record largest size that the file reached at any rank
                for counter in ['_MAX_BYTE_READ', '_MAX_BYTE_WRITTEN']:
                    counter = layer + counter
                    if hash_file['max_size'] < int(file_record[counter]) + 1:
                        hash_file['max_size'] = int(file_record[counter]) + 1

                # check if file was actually read/written
                if int(file_record[layer + '_READS']) > 0:
                    hash_file['was_read'] = True
                if int(file_record[layer + '_WRITES']) > 0:
                    hash_file['was_written'] = True


        # check if file was read/written
        if 'MPIIO_INDEP_OPENS' in file_record:  # TODO: extend conditions? INDEP+COLL OPENS > 0?
            counters = ['_INDEP_', '_COLL_', '_SPLIT_', '_NB_']
            for counter in counters:
                counter = 'MPIIO' + counter + 'READS'
                #print(counter, file_record[counter])
                if int(file_record[counter])> 0:
                    hash_file['was_read'] = True
                    break

            for counter in counters:
                counter = 'MPIIO' + counter + 'WRITES'
                #print(counter, file_record[counter])
                if int(file_record[counter]) > 0:
                    hash_file['was_written'] = True
                    break


        if rank == -1:
            # TODO: what is the reasoning for this? POSIX takes precedence over STDIO, why not keep both?
            if 'POSIX_OPENS' in file_record and int(file_record['POSIX_OPENS'])> 0:
                hash_file['procs'] = logdata['nprocs']
                hash_file['slowest_rank'] = int(file_record['POSIX_SLOWEST_RANK'])
                hash_file['slowest_time'] = float(file_record['POSIX_F_SLOWEST_RANK_TIME'])
                hash_file['slowest_bytes'] = int(file_record['POSIX_SLOWEST_RANK_BYTES'])
                hash_file['fastest_rank'] = int(file_record['POSIX_FASTEST_RANK'])
                hash_file['fastest_time'] = float(file_record['POSIX_F_FASTEST_RANK_TIME'])
                hash_file['fastest_bytes'] = int(file_record['POSIX_FASTEST_RANK_BYTES'])
                hash_file['variance_time'] = float(file_record['POSIX_F_VARIANCE_RANK_TIME'])
                hash_file['variance_bytes'] = float(file_record['POSIX_F_VARIANCE_RANK_BYTES'])

            elif 'STDIO_OPENS' in file_record and int(file_record['STDIO_OPENS']) > 0:
                hash_file['procs'] = logdata['nprocs']
                hash_file['slowest_rank'] = int(file_record['STDIO_SLOWEST_RANK'])
                hash_file['slowest_time'] = float(file_record['STDIO_F_SLOWEST_RANK_TIME'])
                hash_file['slowest_bytes'] = int(file_record['STDIO_SLOWEST_RANK_BYTES'])
                hash_file['fastest_rank'] = int(file_record['STDIO_FASTEST_RANK'])
                hash_file['fastest_time'] = float(file_record['STDIO_F_FASTEST_RANK_TIME'])
                hash_file['fastest_bytes'] = int(file_record['STDIO_FASTEST_RANK_BYTES'])
                hash_file['variance_time'] = float(file_record['STDIO_F_VARIANCE_RANK_TIME'])
                hash_file['variance_bytes'] = float(file_record['STDIO_F_VARIANCE_RANK_BYTES'])

        else:
            # TODO: could not test, because I have no example where this happens >:/

            # calculate total time and bytes
            total_time = 0
            total_bytes = 0
            for layer in ['POSIX', 'STDIO']:
                if (layer + '_OPENS') in file_record:
                    for counter in ['META', 'READ', 'WRITE']:
                        counter = layer + '_F_' + counter + '_TIME'
                        total_time += float(file_record[counter])

                    for counter in ['READ', 'WRITTEN']:
                        counter = layer + '_BYTES_' + counter
                        total_bytes += int(file_record[counter])

            # determine slowest/fastest time,rank,bytes
            counter = 'slowest_time'
            if counter not in hash_file  or  hash_file[counter] < total_time:
                hash_file['slowest_time'] = total_time
                hash_file['slowest_rank'] = rank
                hash_file['slowest_bytes'] = total_bytes
 
            counter = 'fastest_time'
            if counter not in hash_file  or  hash_file[counter] > total_time:
                hash_file['fastest_time'] = total_time
                hash_file['fastest_rank'] = rank
                hash_file['fastest_bytes'] = total_bytes


            # determine variances (time and bytes)
            if 'variance_time_S' not in hash_file:
                hash_file['variance_time_S'] = 0
                hash_file['variance_time_T'] = total_time
                hash_file['variance_time_n'] = 1
                hash_file['variance_bytes_S'] = 0
                hash_file['variance_bytes_T'] = total_bytes
                hash_file['variance_bytes_n'] = 1
                hash_file['procs'] = 1
                hash_file['variance_time'] = 0
                hash_file['variance_bytes'] = 0
            else:
                n = hash_file['variance_time_n']
                m = 1
                T = hash_file['variance_time_T']

                hash_file['variance_time_S'] += (m / (n * (n + m))) * ((n / m) * total_time - T) * ((n / m) * total_time - T)
                hash_file['variance_time_T'] += total_time
                hash_file['variance_time_n'] += 1

                hash_file['variance_time']    = hash_file['variance_time_S'] / hash_file['variance_time_n']

                n = hash_file['variance_bytes_n']
                m = 1
                T = hash_file['variance_bytes_T']
                hash_file['variance_bytes_S'] += (m / (n * (n + m))) * ((n / m) * total_bytes - T) * ((n / m) * total_bytes - T)
                hash_file['variance_bytes_T'] += total_bytes
                hash_file['variance_bytes_n'] += 1

                hash_file['variance_bytes']    = hash_file['variance_bytes_S'] / hash_file['variance_bytes_n']

                hash_file['procs'] = hash_file['variance_time_n']


        # if this is a non-shared file, then add the time spent here to the
        # total for that particular rank
        # XXX mpiio or posix? should we do both or just pick mpiio over posix?
        if rank != -1:
            # is it mpiio or posix?
            if ('MPIIO_INDEP_OPENS' in file_record and
                    int(file_record['MPIIO_INDEP_OPENS']) > 0 or
                    int(file_record['MPIIO_COLL_OPENS']) > 0):
                # ensure field for dict
                if rank not in logdata['hash_unique_file_time']:
                    logdata['hash_unique_file_time'][rank] = 0.0

                ops = ['META', 'READ', 'WRITE']
                for op in ops:
                    logdata['hash_unique_file_time'][rank] += float(file_record['MPIIO_F_' + ops+'_TIME'])

            else:
                # posix + stdio
                for layer in ['POSIX', 'STDIO']:

                    if (layer+'_OPENS') in file_record:
                        # ensure field for dict
                        if rank not in logdata['hash_unique_file_time']:
                            logdata['hash_unique_file_time'][rank] = 0.0

                        ops = ['META', 'READ', 'WRITE']
                        for op in ops:
                            logdata['hash_unique_file_time'][rank] += float(file_record[layer + '_F_'+ops + '_TIME'])

        else:
            # cumulative time spent on shared files by slowest proc
            # is it mpi-io or posix?
            if ('MPIIO_INDEP_OPENS' in file_record and
                    int(file_record['MPIIO_INDEP_OPENS']) > 0 or
                    int(file_record['MPIIO_COLL_OPENS']) > 0):
                logdata['shared_file_time'] += float(file_record['MPIIO_F_SLOWEST_RANK_TIME'])
            else:
                for layer in ['POSIX', 'STDIO']:
                    if (layer + '_OPENS') in file_record and file_record[layer + '_OPENS'] > 0:
                        logdata['shared_file_time'] += float(file_record[layer + '_F_SLOWEST_RANK_TIME'])
                        break # IMPORTANT! To igve posix precedence

        # any mpi reads?
        mpi_did_read = 0
        if 'MPIIO_INDEP_OPENS' in file_record:
            for mode in ['INDEP', 'COLL', 'NB', 'SPLIT']:
                mpi_did_read += int(file_record['MPIIO_' + mode + '_READS'])

        # add up how many bytes were transferred
        if ('MPIIO_INDEP_OPENS' in file_record and
                (int(file_record['MPIIO_INDEP_OPENS']) > 0 or int(file_record['MPIIO_COLL_OPENS']) > 0) and
                not mpi_did_read):
            # mpi file that was only written; disregard any read accesses that
            # may have been performed for sieving at the posix level
            logdata['total_job_bytes'] += int(file_record['POSIX_BYTES_WRITTEN'])  # TODO: double-check
        else:
            for layer in ['POSIX', 'STDIO']:
                if (layer+'_OPENS') in file_record:
                    logdata['total_job_bytes'] += int(file_record[layer + '_BYTES_WRITTEN'])
                    logdata['total_job_bytes'] += int(file_record[layer + '_BYTES_READ'])

        # end: process_file_record
        pass




    def create_time_summary(self):
        logdata = self.logdata


        # Original:
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


    def create_op_counts(self):
        logdata = self.logdata

        # convienience links
        summary = logdata['summary']
        op_counts = logdata['op_counts']


        for layer in ['POSIX', 'MPIIO_INDEP', 'MPIIO_COLL_', 'STDIO']:
            if (layer+'_OPENS') in summary or (layer + '_INDEP_OPENS') in summary :
            
                entry = []
                for op in ['READS','WRITES','OPENS','STATS','SEEKS','MMAPS']:
                    counter = layer+'_'+op
                    if counter in summary:
                        entry.append(int(summary[counter]))
                    else:
                        entry.append(0)

                val = 0
                if layer in ['POSIX']:
                    val = int(summary[layer+'_FSYNCS'])
                    val += int(summary[layer + '_FDSYNCS'])
                elif layer in ['MPIIO_COLL']:
                    val = int(summary[layer + '_SYNCS'])
                elif layer in ['STDIO']:
                    val = int(summary[layer + '_FLUSHES'])

                entry.append(val)

                op_counts[layer] = entry
            else:
                op_counts[layer] = [0, 0, 0, 0, 0, 0, 0]
                


    def create_io_hist(self):
        logdata = self.logdata

        # convienience links
        summary = logdata['summary']

        fieldname = '-access-hist.dat'


        for layer in ['POSIX']:
            if (layer+'_OPENS') in summary or (layer+'_INDEP_OPENS') in summary:
            
                read = []
                write = []

                for bins in ['0_100','100_1K','1K_10K','10K_100K','100K_1M','1M_4M','4M_10M','10M_100M','100M_1G','1G_PLUS']:
                    read.append(summary[layer + '_SIZE_READ_' + bins])
                    write.append(summary[layer + '_SIZE_WRITE_' + bins])


                logdata[layer.lower() + fieldname] = {"read": read, "write": write}

        for layer in ['MPIIO']:
            if (layer+'_OPENS') in summary or (layer + '_INDEP_OPENS') in summary:
            
                read = []
                write = []

                for bins in ['0_100','100_1K','1K_10K','10K_100K','100K_1M','1M_4M','4M_10M','10M_100M','100M_1G','1G_PLUS']:
                    read.append(summary[layer + '_SIZE_READ_AGG_' + bins])
                    write.append(summary[layer + '_SIZE_WRITE_AGG_' + bins])


                logdata[layer.lower() + fieldname] = {"read": read, "write": write}


    def parse_output(self, out):
        logdata = self.logdata


        # process lines
        lines = out.decode("utf-8").split("\n")
        for line in lines:
            #print(line)

            if re.match(r'^\s*$', line):
                # skip blank lines
                pass

            elif re.match(r'^#', line):
                # info entry
                self.parse_info_line(line)

            else:
                # actual record entry
                self.parse_record_line(line)

        #print("\n"*5)

        # process collected file records
        for file_record in logdata['file_record_hash']:
            self.process_file_record(file_record)

        # TIME_SUMMARY
        self.create_time_summary() 

        # OP COUNTS
        #   POSIX, MPI, STDIO
        self.create_op_counts()

        # IO HIST
        #   POSIX, MPIIO
        self.create_io_hist()

        # PATTERN


        # ACCESS TABLE

        # PATTERN

        # some POSIX, acces size sorting (descinding)


        # file count table

        # FS table

        # var table


    def debug_print(self):

        logdata = self.logdata.copy()

        # for record in logdata["records"]["data"]:
        #    print(record)
        # print("Records:", len(logdata["records"]["data"]))
        # print("\n"*5)

        logdata['records'] = "REMOVED HERE TO LIMIT DEBUG"
        logdata['summary'] = "REMOVED HERE TO LIMIT DEBUG"
        # logdata['mt_data'] = "REMOVED TO LIMIT DEBUG"
        # logdata['fs_data'] = "REMOVED TO LIMIT DEBUG"

        logdata['file_record_hash'] = "REMOVED TO LIMIT DEBUG - refer to hash_files which provides summary"

        pprint.pprint(logdata, indent=1, width="160")


    def dump(self, filetype=None, name="outfile"):
        logdata = self.logdata

        if filetype == "stdout-json":
            print(json.dumps(logdata))
        elif filetype == "js":
            outfile = name + ".js"
            fout = open(outfile, "w")
            fout.write("logdata.push(%s);" % (json.dumps(logdata, indent=1)))
        elif filetype == "sqlite":
            pass
        else:
            outfile = name + ".json"
            fout = open(outfile, "w")
            fout.write(json.dumps(logdata, indent=1))
