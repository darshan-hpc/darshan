from darshan.report import *

import datetime
import copy


def merge(self, other, reduce_first=False):
    """
    Merge two darshan reports and return a new combined report.

    Args:
        mods:           Name(s) of modules to preserve (reduced)
        name_records:   Id(s)/Name(s) of name_records to preserve (reduced)

    Return:
        None
    """

    # new report
    nr = DarshanReport()

    # keep provenance?
    if self.provenance_enabled or other.provenance_enabled:
        # Currently, assume logs remain in memomry to create prov. tree on demand
        # Alternative: maintain a tree with simpler refs? (modified reports would not work then)

        #nr.provenance_reports[self.filename] = copy.copy(self)
        #nr.provenance_reports[other.filename] = copy.copy(other)

        nr.provenance_reports[self.filename] = None
        nr.provenance_reports[other.filename] = None

        nr.provenance_graph.append(("add", self, other, datetime.datetime.now()))


    # update metadata helper
    def update_metadata(report, force=False):
        if force:
            nr.start_time = report.start_time
            nr.end_time = report.end_time
            return

        if report.start_time < nr.start_time:
            nr.start_time = report.start_time

        if report.end_time > nr.end_time:
            nr.end_time = report.end_time


    update_metadata(self, force=True)
    update_metadata(other)


    # copy over records (references, under assumption single records are not altered)
    for report in [self, other]:
        for key, records in report.data['records'].items():
            #print(report, key)
            if key not in nr.records:
                nr.records[key] = copy.copy(records)
            else:
                nr.records[key]._records = nr.records[key]._records + copy.copy(records._records)

        for key, mod in report.modules.items():
            if key not in nr.modules:
                nr.modules[key] = copy.copy(mod)
                # TODO: invalidate len/counters

        for key, counter in report.counters.items():
            if key not in nr.counters:
                nr.counters[key] = copy.copy(counter)

        for key, nrec in report.name_records.items():
            if key not in nr.counters:
                nr.name_records[key] = copy.copy(nrec)
                # TODO: verify colliding name_records?

    return nr
