from darshan.report import *

def name_records_summary(self):
    """
    Count records for every name record.

    Args:
        mod_name (str): 

    Return:
        None
    """

    counts = {}

    for mod, records in self.records.items():
        for rec in records:
            if rec['id'] not in counts:
                counts[rec['id']] = {}
                
            ctx = counts[rec['id']]
            if mod not in ctx:
                ctx[mod] = 1
            else:
                ctx[mod] += 1

    return counts
