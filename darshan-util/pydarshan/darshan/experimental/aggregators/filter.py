from darshan.report import *

import sys


def filter(self, mods=None, name_records=None, data_format='numpy', mode='append'):
    """
    Return filtered list of records.

    Args:
        mods:           Name(s) of modules to preserve 
        name_records:   Id(s)/Name(s) of name_records to preserve

    Return:
        None
    """

    # convienience
    recs = self.records
    ctx = {}


    mods_wildcard = False
    name_records_wildcard = False


    if mods in ['all', '*', None]:
        mods_wildcard = True
        mods = None

    if name_records in ['all', '*', None]:
        name_records_wildcard = True
        name_records = None

    
    # change inputs to whitelists
    if mods == None:
        mods = self.records.keys()


    if name_records == None:
        name_records = list(self.name_records.keys())

    else:
        resolve_table = {}
        for key, value in self.name_records.items():
            resolve_table[key] = key
            resolve_table[value] = key

        ids = []
        for nrec in name_records:
            if nrec in resolve_table:
                ids.append(resolve_table[nrec])

        # TODO: decide if overwriting kargs is really a good idea.. currently considering it a sanitation step
        name_records = ids



    print(mods)
    print(name_records)


    if name_records != None:

        # aggragate
        for mod, recs in self.records.items():

            if mod not in mods:
                continue

            for rec in recs:
                nrec = rec['id'] 

                if nrec in name_records:
                    if mod not in ctx:
                        ctx[mod] = []

                    ctx[mod].append(rec)




    if mode == 'append':
        name = 'filter'
        if name not in self.summary:
            self.summary[name] = {}
        self.data[name] = ctx
    
    return ctx

