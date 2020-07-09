from darshan.report import *

def records_as_dict(self, mode='append', recs=None):
    """
    Convert all counters to dictionaries with human-readable names.

    Args:
        mode (str): Whether to 'append' (default) or to 'return' aggregation. 

    Return:
        None or dict: Depending on mode
    """

    # convienience
    if recs is None:
        recs = self.records


    ctx = {}

    # aggragate
    for mod in recs:

        if mod in ['DXT_POSIX']:
            continue


        # check records for module are present
        if mod not in ctx:
            ctx[mod] = []

        for rec in recs[mod]:
            c = None
            fc = None

            if 'counters' in rec:
                c = dict(zip(self.counters[mod]['counters'], rec['counters'].tolist()))
            
            if 'fcounters' in rec:
                fc = dict(zip(self.counters[mod]['fcounters'], rec['fcounters'].tolist()))

            if rec['id'] in self.name_records:
                nrec = self.name_records[rec['id']]
            else:
                nrec = None

            ctx[mod].append({'id': rec['id'], 'rank': rec['rank'], 'counters': c, 'fcounters': fc, 'name_record': nrec})



    return ctx
