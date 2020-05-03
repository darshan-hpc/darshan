from darshan.report import *

import sys


def reduce(self, operation="sum", mods=None, name_records=None, mode='append', data_format="numpy"):
    """
    Reduce records.

    Args:
        mods:           Name(s) of modules to preserve (reduced)
        name_records:   Id(s)/Name(s) of name_records to preserve (reduced)


    Return:
        None
    """


    # convienience
    recs = self.records
    ctx = {}


    mods_wildcard = False
    name_records_wildcard = False


    if mods in ['distinct', 'unique']:
        mods_wildcard = False
        mods = None
    elif mods in ['all', '*', None]:
        mods_wildcard = True
        mods = None

    if name_records in ['distinct', 'unique']:
        name_records_wildcard = False
        name_records = None
    elif name_records in ['all', '*', None]:
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



    #print(mods)
    #print(name_records)


    if name_records != None:

        # aggragate
        for mod, recs in self.records.items():

            if mod not in mods:
                continue

            for rec in recs:
                nrec = rec['id'] 

                if nrec in name_records:
                    if mod not in ctx:
                        ctx[mod] = {}

                    # TODO: consider regex?, but filter those out at resolve_table
                    if name_records_wildcard:
                        nrec_pattern = '*'
                    else:
                        nrec_pattern = nrec


                    for counters in ['counters', 'fcounters']:
                        if nrec_pattern not in ctx[mod]:
                            ctx[mod][nrec_pattern] = {}


                        if counters not in rec:
                            continue


                        if counters not in ctx[mod][nrec_pattern]:
                            ctx[mod][nrec_pattern][counters] = rec[counters]
                        else:
                            ctx[mod][nrec_pattern][counters] = np.add(ctx[mod][nrec_pattern][counters], rec[counters])



    # convert records back to list
    result = {}
    for mod, name_records in ctx.items():
        if mod not in result:
            result[mod] = []
        
        for name_record, val in name_records.items():
            rec = {"id": name_record, "rank": -1}
            rec.update({"id": name_record, "rank": -1})            
            rec.update(val)  

            result[mod].append(rec)            




    if mode == 'append':
        name = 'reduction'
        if name not in self.summary:
            self.summary[name] = {}
        self.data[name] = ctx

    return result
