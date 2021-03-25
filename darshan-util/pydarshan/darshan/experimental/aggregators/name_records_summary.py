from darshan.report import *

def name_records_summary(self):
    """
    Count records for every name record.

    Args:
        mod_name (str): 

    Return:
        dict with counts nested <nrec-id>/<mod>
    """

    counts = {}

    for mod, records in self.records.items():
        for rec in records:
            if rec['id'] not in counts:
                counts[rec['id']] = {'name': self.name_records[rec['id']], 'counts': {}}

            if mod not in counts[rec['id']]['counts']:
                counts[rec['id']]['counts'][mod] = 1
            else:
                counts[rec['id']]['counts'][mod] += 1

    return counts
