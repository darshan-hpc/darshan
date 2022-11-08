from darshan.report import *

def mod_agg_iohist(self, mod, mode='append'):
    """
    Generate aggregated histogram for mod_name.

    Args:
        mod_name (str): 

    Return:
        None
    """

    # sanitation and guards
    supported = ["POSIX", "MPI-IO", "H5D", "PNETCDF_VAR"]
    if mod not in supported:
        raise Exception("Unsupported mod_name for aggregated iohist.")


    # convienience
    recs = self.records
    ctx = {}



    # check records for module are present
    if mod not in recs:
        return


    # aggragate
    for rec in recs[mod]:
        if mod not in ctx:
            ctx[mod] = rec['counters']
        else:
            ctx[mod] = np.add(ctx[mod], rec['counters'])



    # cleanup and prepare for json serialization
    def fix_name(name):
        name = name.split("_")
        typ = "UNKNOWN"
        if "READ" in name:
            typ = "READ"
        elif "WRITE" in name:
            typ = "WRITE"
        name = "%s_%s_%s" % (typ, name[-2], name[-1])
        return name

    tmp = json.dumps(ctx[mod], cls=DarshanReportJSONEncoder)
    tmp = json.loads(tmp)
    cn = backend.counter_names(mod)
    c = dict(zip(cn, tmp))
    c = {k:c[k] for k in c if re.match('.*?_SIZE_.*?', k)}
    c = {fix_name(k):v for k, v  in c.items()}
    ctx = c


    if mode == 'append':
        if 'agg_iohist' not in self.data:
            self.summary['agg_iohist'] = {}
        self.summary['agg_iohist'][mod] = ctx
    
    return ctx


