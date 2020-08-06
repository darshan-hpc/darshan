#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import math

from operator import itemgetter

from PIL import Image, ImageDraw


def sanitize_size(x):
    """ Ensure segments are at least represented by one pixel. """
    if x < 1:
        x = 1
    return int(x)



def calc_duration(trace):
    start_time = float('inf')
    end_time = float('-inf')
    for seg in trace:
        if seg['start_time'] < start_time:
            start_time = seg['start_time']
        if seg['end_time'] > end_time:
            end_time = seg['end_time']
    return start_time, end_time, end_time - start_time

def calc_minsize(trace):
    minsize = 0
    for seg in trace:
        size = seg['offset'] + seg['length']
        if size > minsize:
            minsize = size
    return minsize




def segment(rec):
    """
        * write segments, then read segments
        * segments in order they occur
    """

    for item in rec['read_segments']:
        item.update({'type': 'r'})

    for item in rec['write_segments']:
        item.update({'type': '2'})

    trace = rec['read_segments'] + rec['write_segments']
    minsize = calc_minsize(trace)
    start, end, duration = calc_duration(trace)

    print("len(trace):", len(trace), "minsize:", minsize, "duration:", duration)




    count = len(trace)

    factor = 720
    width = sanitize_size( duration * factor )

    factor = width/count
    
    #print(count)
    #print(factor)

    # image properties
    #height = int(math.log(minsize))
    #height = sanitize_size( int((math.log(minsize)*math.log(minsize))/2) )
    height = sanitize_size( math.log(minsize)*math.log(minsize) )


    #print(width, height)


    #img = Image.new('RGB', (width, height), color = (0, 0, 0))
    #img = Image.new('RGBA', (width, height), color = (0, 0, 0, 0))
    img = Image.new('RGBA', (width, height), color = (33, 33, 33, 255))
    

    # sort?
    trace = sorted(trace, key=itemgetter('start_time'))

    draw = ImageDraw.Draw(img)
    for i, event in enumerate(trace):
        typ = event['type']
        off = event['offset']
        lee = event['length']
        sta = event['start_time']
        end = event['end_time']

        #print(typ, off, lee, sta, end)

        xx = i*factor;
        yy = height * (off / minsize)

        wi = 1 * factor;
        he = sanitize_size( height * (lee / minsize) )
    

        fill = None
        #fill = (0,0,0,0)
        if typ == 'r':
            fill = (222, 66, 111, 200)
        elif typ == 'w':
            fill = (66, 222, 222, 200)

        
        #print([xx, yy, xx+wi-1, yy+he-1])

        # draw.rectangle(xy, fill=None, outline=None)
        # where yx either [(x0, y0), (x1, y1)] or [x0, y0, x1, y1]
        draw.rectangle([xx, yy, xx+wi-1, yy+he-1], fill=fill, outline=None)

    del draw

    return img






def wallclock(rec):


    for item in rec['read_segments']:
        item.update({'type': 'r'})

    for item in rec['write_segments']:
        item.update({'type': '2'})

    trace = rec['read_segments'] + rec['write_segments']
    minsize = calc_minsize(trace)
    start, end, duration = calc_duration(trace)

    count = len(trace)

    factor = 720

    if duration == 0:
        duration = 1

    # image properties
    width = sanitize_size( duration * factor )
    #height = int(math.log(minsize))
    height = sanitize_size( math.log(minsize)*math.log(minsize) )

    #print(width, height)

    #img = Image.new('RGB', (width, height), color = (0, 0, 0))
    #img = Image.new('RGBA', (width, height), color = (0, 0, 0, 0))
    img = Image.new('RGBA', (width, height), color = (33, 33, 33, 255))

    # sort?
    trace = sorted(trace, key=itemgetter('start_time'))

    draw = ImageDraw.Draw(img)
    for i, event in enumerate(trace):
        typ = event['type']
        off = event['offset']
        lee = event['length']
        sta = event['start_time'] - start
        end = event['end_time'] - start


        xx = sta/duration * width;
        yy = height * (off / minsize)

        wi = sanitize_size( (end-sta)*factor );
        he = sanitize_size( height * (lee / minsize) )
        

        fill = None
        #fill = (0,0,0,0)
        if typ == 'r':
            fill = (222, 66, 111, 200)
            #fill = (222, 66, 111)
        elif typ == 'w':
            fill = (66, 222, 222, 200)
            #fill = (66, 222, 222)

        #print([xx, yy, xx+wi, yy+he-1])

        # draw.rectangle(xy, fill=None, outline=None)
        # where yx either [(x0, y0), (x1, y1)] or [x0, y0, x1, y1]
        draw.rectangle([xx, yy, xx+wi-1, yy+he-1], fill=fill, outline=None)

    del draw

    return img






def visualize(data, modes=['wallclock', 'segment'], path="./"):
    """
        alternative mode: wallclock

    """


    #print(data)

    fileid = data['cur']['fileid']
    rankid = data['rankid']

    trace = data['cur']['ranks'][ data['rankid'] ]['trace']
    minsize = data['minsize']


    start = data['cur']['ranks'][ data['rankid'] ]['start']
    end = data['cur']['ranks'][ data['rankid'] ]['end']

    duration = (end-start)

    filename = "%s/%s" % (path, fileid)
    #filename = "%s/file%s_rank%s" % (path, fileid, rankid)
    filename = os.path.normpath(filename)

    if 'wallclock' in modes:
        img = wallclock(trace, minsize, duration, start, end)
        print('Writing %s_wallclock.png' % (filename))
        img.save('%s_wallclock.png' % (filename), 'PNG')

    if 'segment' in modes:
        img = segment(trace, minsize, duration)
        print('Writing %s_segment.png' % (filename))
        img.save('%s_segment.png' % (filename), 'PNG')
    


