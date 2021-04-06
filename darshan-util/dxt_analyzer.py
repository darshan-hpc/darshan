#!/usr/bin/env python3

###
#
# *** Copyright Notice ***
#
# dxt_analyzer.py 
# Copyright (c) 2017, The Regents of the University of California, 
# through Lawrence Berkeley National Laboratory (subject to receipt
# of any required approvals from the U.S. Dept. of Energy).  
# All rights reserved.
#
# If you have questions about your rights to use or distribute this software, 
# please contact Berkeley Lab's Innovation & Partnerships Office 
# at IPO@lbl.gov.
#
# NOTICE.  This software was developed under funding from the 
# U.S. Department of Energy.  As such, the U.S. Government has been granted 
# for itself and others acting on its behalf a paid-up, nonexclusive, 
# irrevocable, worldwide license in the Software to reproduce, prepare 
# derivative works, and perform publicly and display publicly.  Beginning 
# five (5) years after the date permission to assert copyright is obtained 
# from the U.S. Department of Energy, and subject to any subsequent five (5) 
# year renewals, the U.S. Government is granted for itself and others acting 
# on its behalf a paid-up, nonexclusive, irrevocable, worldwide license in 
# the Software to reproduce, prepare derivative works, distribute copies to 
# the public, perform publicly and display publicly, and to permit others to 
# do so.
#
# Email questions to SDMSUPPORT@LBL.GOV
# Scientific Data Management Research Group
# Lawrence Berkeley National Laboratory
#
# last update on Tue Aug  8 08:16:49 PDT 2017
#


'''
dxt_analyzer.py
To plot the read or write activity from Darshan Extended Trace (DXT) logs.

For more information on creating DXT logs, see:
http://www.mcs.anl.gov/research/projects/darshan/docs/darshan3-util.html#_darshan_dxt_parser 

% ./io_activitity_dxt.py --help
usage: dxt_analyzer.py [-h] -i DXT_LOGNAME [-o SAVEFIG] [--show] [--read]
                            [--filemode] [-f FNAME]

io activity plot from dxt log

optional arguments:
  -h, --help            show this help message and exit
  -i DXT_LOGNAME, --input DXT_LOGNAME
                        dxt log path
  -o SAVEFIG, --save SAVEFIG
                        output file name for the plot
  --show                Show the plot rather than saving to a PDF
  --read                READ I/O action to be plotted. 
                        Default is False for WRITE mode.
  --filemode            Single file mode (must be used with --fname)
                        Default is False for all files
  -f FNAME, --fname FNAME
                        name of file to be plotted (must use with --filemode)

e.g.
python dxt_analyzer.py -i darshan_dxt-a.txt
python dxt_analyzer.py -i darshan_dxt-a.txt \
        --filemode -f /global/cscratch1/sd/asim/amrex/a24/plt00000.hdf5
python dxt_analyzer.py -i darshan_dxt-c.txt
python dxt_analyzer.py -i darshan_dxt-d.txt -o dxt-d.pdf
python dxt_analyzer.py -i darshan_dxt-df.txt
python dxt_analyzer.py -i darshan_dxt-v.txt

'''

from builtins import str
import numpy as np
import matplotlib
#matplotlib.use('PDF')
import matplotlib.pyplot as plt
import re
import argparse
import sys

#------------------------------------------------------------------------------
# Regular expression and helper funtion definitions
#------------------------------------------------------------------------------
global_finfo = {} 
#'filename':{'rw':[], 'mount':'', 'fs':'', 'stripe_size':-1, 'stripe_width':-1, 'OSTlist':[]}
#                    Module          rank         write/read      segment               offset                   length                 start                     end                    OST
POSIX_LOG_PATTERN = ' (X_POSIX)\s+([+-]?\d+(?:\.\d+)?)\s+(\S+)\s+([+-]?\d+(?:\.\d+)?)\s+([+-]?\d+(?:\.\d+)?)\s+([+-]?\d+(?:\.\d+)?)\s+([+-]?\d+(?:\.\d+)?)\s+([+-]?\d+(?:\.\d+)?)\s+\[\s*([+-]?\d+(?:\.\d+)?)\]\.*'
POSIX_LOG_NO_OSTS = ' (X_POSIX)\s+([+-]?\d+(?:\.\d+)?)\s+(\S+)\s+([+-]?\d+(?:\.\d+)?)\s+([+-]?\d+(?:\.\d+)?)\s+([+-]?\d+(?:\.\d+)?)\s+([+-]?\d+(?:\.\d+)?)\s+([+-]?\d+(?:\.\d+)?)'

#                    Module          rank         write/read      segment               length                   start                     end 
MPIIO_LOG_PATTERN = ' (X_MPIIO)\s+([+-]?\d+(?:\.\d+)?)\s+(\S+)\s+([+-]?\d+(?:\.\d+)?)\s+([+-]?\d+(?:\.\d+)?)\s+([+-]?\d+(?:\.\d+)?)\s+([+-]?\d+(?:\.\d+)?)'

HEADER_PATTERN = '(#\s+)(\S+):(\s+)(\d+)'
COMMENT_PATTERN_DXT = '# \.*'
USEFUL_COMMENT_PATTERN = '# DXT,\.*'
LISTNUM_PATTERN = '(\d+.*)'

header = re.compile(HEADER_PATTERN)
validposix = re.compile(POSIX_LOG_PATTERN)
posix_no_ost = re.compile(POSIX_LOG_NO_OSTS)
validmpiio = re.compile(MPIIO_LOG_PATTERN)
comment_dxt = re.compile(COMMENT_PATTERN_DXT)
use_comment = re.compile(USEFUL_COMMENT_PATTERN)
listnumpattern = re.compile(LISTNUM_PATTERN)


def store_useful_comment(pieces, finfo_dict, curr_fname):
    '''updates a file's dictionary entry using the pieces of a comment/header line from a DXT log'''
    attribute = pieces[0].replace('# DXT, ', '')
    if attribute=='file_id':
        curr_fname = pieces[2].replace(' ','').rstrip('\n')
        if curr_fname not in list(finfo_dict.keys()):
            finfo_dict[curr_fname] = {'mount':'', 'fs':'', 'stripe_size':-1, 'stripe_width':-1, 'OST_list':[]}
        return curr_fname
    elif attribute=='mnt_pt':
        finfo_dict[curr_fname]['mount'] = pieces[1].replace(', fs_type', '').replace(' ', '').rstrip('\n')
        finfo_dict[curr_fname]['fs'] = pieces[2].replace(' ', '').rstrip('\n')
    elif attribute=='Lustre stripe_size':
        finfo_dict[curr_fname]['stripe_size'] = int(pieces[1].replace(', Lustre stripe_count', '').replace(' ', ''))
        finfo_dict[curr_fname]['stripe_width'] = int(pieces[2].replace(' ',''))
    elif attribute=='Lustre OST obdidx':
        liststr = pieces[1][1:]
        listnums = listnumpattern.match(liststr).groups()[0]
        OSTlist = [int(x) for x in re.findall(r'\d+', listnums)]
        finfo_dict[curr_fname]['OST_list'] = OSTlist
    return curr_fname


def parse_dxt_log_line(line, curr_fname, finfo_dict):
    '''takes a DXT log line and stores pertinent file metadata into a dictionary, and parses read/write trace data into a tuple'''
    if not line: return (curr_fname, (),  -1)
    pieces = line.split(":")
    if comment_dxt.match(pieces[0]):
        if use_comment.match(pieces[0]):
            curr_fname = store_useful_comment(pieces, finfo_dict, curr_fname)
            return (curr_fname, (), -1)
        header_match = header.match(line)
        if header_match:
            return (curr_fname, (header_match.group(2),int(header_match.group(4))), -11)
        else:
            return (curr_fname, (), -1)
    data = validposix.match(line)
    if data:
        return (curr_fname,
            (
            data.group(1).replace('X_', '')+'_'+(data.group(3).upper()), 
            (
                int(data.group(2)), #rank
                int(data.group(6)), #length
                float(data.group(7)), #start
                float(data.group(8)), #end
                int(data.group(9))  #OST
            )
        ), 1)
    data = posix_no_ost.match(line)
    if data:
        return (curr_fname,
            (
            data.group(1).replace('X_', '')+'_'+(data.group(3).upper()), 
            (
                int(data.group(2)), #rank
                int(data.group(6)), #length
                float(data.group(7)), #start
                float(data.group(8)), #end
                -1 # No OST
            )
        ), 1)
    data = validmpiio.match(line)
    if data:
        return (curr_fname,
            (
            data.group(1).replace('X_', '')+'_'+(data.group(3).upper()), 
            (
                int(data.group(2)), #rank
                int(data.group(5)), #length
                float(data.group(6)), #start
                float(data.group(7)), #end
            )
        ), 1)
    return (curr_fname, (), -1)


def get_verts_file(data, module, fname, action):
    '''make a list of rectangle vertices to plot the posix/mpi read/write activity for each rank for a specific file'''
    keyword = module+'_'+action
    filedata = [x[1] for x in [x for x in data if x[0]==fname]]
    filtered = [x[1] for x in [x for x in filedata if x[0]==keyword]]
    activities = [(x[0], (x[2], x[3])) for x in filtered]
    verts = np.zeros((1,4,2))
    for entry in activities:
        lx,rx,by,ty = entry[1][0], entry[1][1], entry[0] - 0.5 , entry[0] + 0.5
        newverts = np.array([((lx,by), (lx,ty), (rx, ty), (rx, by))])
        verts = np.concatenate((verts, newverts))
    return verts[1:]


def get_verts_all(data, module, action):
    '''make a list of rectangle vertices to plot the posix/mpi read/write activity for each rank'''
    keyword = module+'_'+action
    IOdata = [x[1] for x in data]
    filtered = [x[1] for x in [x for x in IOdata if x[0]==keyword]]
    activities = [(x[0], (x[2], x[3])) for x in filtered]
    verts = np.zeros((1,4,2))
    for entry in activities:
        lx,rx,by,ty = entry[1][0], entry[1][1], entry[0] - 0.5 , entry[0] + 0.5
        newverts = np.array([((lx,by), (lx,ty), (rx, ty), (rx, by))])
        verts = np.concatenate((verts, newverts))
    return verts[1:]


#------------------------------------------------------------------------------
# Set variables and create plots
#------------------------------------------------------------------------------

# Variables to be set by user
# name of DXT log file
dxt_logname = './darshan_dxt-5967365.txt' 
# create plot for a specific file (mode='file') or for all files (mode='all')
singlefile_mode = False # for 'all' 
mode = 'all' 
# name of file to be plotted (must use with mode='file')
fname = '/global/cscratch1/sd/junmin/heatTxf.n64.s32/ph5.4096.n128s64.t1.h5'
# FLAG for READ I/O action to be plotted.
read_flag = False  # for READ action 
action = 'WRITE' 
# filename to save the plot (must end with .pdf).
# showflag takes priority than savefig.
showflag = False # If you do not want to save, set this to True. 
savefig = 'dxt_plot.pdf' 

parser = argparse.ArgumentParser(description='io activity plot from dxt log')
parser.add_argument("-i", "--input", action="store", dest="dxt_logname", required=True, help="dxt log path")
parser.add_argument("-o", "--save", action="store", dest="savefig", required=False, help="output file name for the plot")
parser.add_argument("--show", action="store_true", dest="showflag", required=False, help="Show the plot rather than saving to a PDF")
parser.add_argument("--read", action="store_true", dest="read_flag", required=False, help="READ I/O action to be plotted. Default is False for WRITE mode.")
parser.add_argument("--filemode", action="store_true", dest="singlefile_mode", required=False, help="Single file mode (must be used with --fname). Default is False for all files")
parser.add_argument("-f", "--fname", action="store", dest="fname", required=False, help="name of file to be plotted (must use with --filemode)")

#args = parser.parse_args(['-l', '/Users/asim/Desktop/simcodes/vpic/runs/ttest/junmin-darshan_dxt-5967365.txt', 
#'--save', 'dxt_plot.pdf'])
args = parser.parse_args()     # uncomment this line for general use

dxt_logname = args.dxt_logname
if (args.savefig):
    savefig = args.savefig
if (args.showflag):
    showflag = True
if (args.read_flag): 
    read_flag = True
    action = 'READ'
if (args.singlefile_mode): 
    singlefile_mode = True
    mode = 'file'
    fname = args.fname
    

with open(dxt_logname) as infile:
    try:
        line = infile.readline()
        if "# darshan" not in line:
            raise Exception('Invalid file format')
    except:
        print("Error: unable to parse " + dxt_logname + ".", file=sys.stderr)
        print("   Please make sure that it was generated by the darshan-dxt-parser utility.", file=sys.stderr)
        sys.exit(1)
    finfo_dict = {}
    curr_fname = ''
    logdata = []
    jobid="NO_JOBID"
    for line in infile:
        curr_fname, data, flag = parse_dxt_log_line(line, curr_fname, finfo_dict)
        if flag == -11:
            (k,v) = data
            if k == 'jobid': jobid = v
            elif k == 'start_time': start_time = v
            elif k == 'end_time': end_time = v
            elif k == 'nprocs': nprocs = v
        elif flag == 1:
            logdata.append((curr_fname, data))

fig, ax = plt.subplots(dpi=150)

if mode=='file':
    mpiio_verts = get_verts_file(logdata, 'MPIIO', fname, action)
    mpiio_collec = matplotlib.collections.PolyCollection(mpiio_verts, facecolor='blue', edgecolor='blue')
    posix_verts = get_verts_file(logdata, 'POSIX', fname, action)
    posix_collec = matplotlib.collections.PolyCollection(posix_verts, facecolor='red', edgecolor='red')
    title = str(jobid)+'_'+fname.split('/').pop()+'_'+action+'_activity'
else :  # mode=='all'
    mpiio_verts = get_verts_all(logdata, 'MPIIO', action)
    mpiio_collec = matplotlib.collections.PolyCollection(mpiio_verts, facecolor='blue', edgecolor='blue')
    posix_verts = get_verts_all(logdata, 'POSIX', action)
    posix_collec = matplotlib.collections.PolyCollection(posix_verts, facecolor='red', edgecolor='red')
    title = str(jobid)+'_'+action+'_activity'


ax.add_collection(mpiio_collec)
ax.add_collection(posix_collec)
ax.autoscale()
plt.ylabel("MPI rank")
plt.xlabel("Time (s)")
plt.title(title)
if (showflag):
    plt.show()
else:
    plt.savefig(savefig, format='pdf')

