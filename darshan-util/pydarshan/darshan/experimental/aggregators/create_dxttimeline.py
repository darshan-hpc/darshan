from darshan.report import *

def create_dxttimeline(self, group_by='rank', mode="append"):
    """
    Generate/update a timeline from dxt tracing records of current report.

    Args:
        group_by (str): By which factor to group entries (default: rank)
                        Allowed Parameters: rank, filename
    """

    
    self.mod_read_all_dxt_records("DXT_POSIX")
    self.mod_read_all_dxt_records("DXT_MPIIO")



    ctx = {'groups': [], 'items': []}


    groups = ctx['groups']
    items = ctx['items']
    

    start_time = datetime.datetime.fromtimestamp( self.data['metadata']['job']['start_time_sec'] )



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
            #"filename": rec['filename'],
            "filename": "FIXME: NEED FILENAME",

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
            #"content": "[%s] " % (mod) + rec['filename'][-84:],
            "content": "[%s] " % (mod) + "NEED FILENAME",
            "order": seg['start_time']
        }
        groups.append(group)



    supported = ['DXT_POSIX', 'DXT_MPIIO']
    for mod in supported:
        if mod in self.data['records']:
            for rec in self.data['records'][mod]:
                groupify(rec, mod)






    # overwrite existing summary entry
    if mode == "append":
        self.summary['timeline'] = ctx
    

    return ctx


