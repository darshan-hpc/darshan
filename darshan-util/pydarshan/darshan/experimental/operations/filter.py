from darshan.report import *

import sys
import copy
import re


def filter(self, mods=None, name_records=None, pattern=None, regex=None):
    """
    Return filtered list of records.

    Args:
        mods:           Name(s) of modules to preserve 
        name_records:   Id(s)/Name(s) of name_records to preserve

    Return:
        None
    """

    
    r = copy.deepcopy(self)


    # convienience
    recs = r.records
    ctx = {}


    mods_wildcard = False
    name_records_wildcard = False


    if mods in ['all', '*', None]:
        mods_wildcard = True
        mods = None

    if name_records in ['all', '*', None]:
        name_records_wildcard = True
        name_records = None

    
    # whitelist all mods
    if mods == None:
        mods = r.records.keys()


    if pattern != None:
        pattern = pattern.replace("*", "(.*?)")
    elif regex:
        pattern = regex


    # whitelist name_records
    if name_records == None and pattern == None and regex == None:
        # allow all name records if no critirium provided
        name_records = list(r.name_records.keys())
    else:
        resolve_table = {}
        ids = []

        for key, value in r.name_records.items():
            resolve_table[key] = key
            resolve_table[value] = key

            # whitelist names that match pattern
            if pattern != None or regex != None:
                if re.match(pattern, value):
                    ids.append(key)


        # convert filenames/name_records mix into list of ids only
        if name_records != None:
            for nrec in name_records:
                if nrec in resolve_table:
                    ids.append(resolve_table[nrec])

        # TODO: decide if overwriting kargs is really a good idea.. currently considering it a sanitation step
        name_records = ids


    if name_records != None:
        # aggragate
        for mod, recs in r.records.items():

            if mod not in mods:
                continue

            for rec in recs:
                nrec = rec['id'] 

                if nrec in name_records:
                    if mod not in ctx:
                        ctx[mod] = DarshanRecordCollection(mod=mod, report=r)

                    ctx[mod].append(rec._records[0])


    r.records = ctx


    return r
