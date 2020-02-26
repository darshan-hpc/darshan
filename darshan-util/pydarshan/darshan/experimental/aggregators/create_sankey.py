from darshan.report import *

def create_sankey(self):
    """
    Generate a summary that shows the dataflow between ranks, files and
    their mountpoints.
    """

    # convienience
    recs = self.report['records']
    nrecs = self.report['name_records']
    ctx = {}


    # check records for module are present
    if 'POSIX' not in recs:
        self.report['sankey'] = None
        return

    ranks = {}
    files = {}
    mounts = {}

    nodes = {}
    edges = {}


    # build mnt list
    mnts = []
    for mnt in self.report['mounts']:
        mnts.append(mnt[0])


    # collect records
    for rec in recs['POSIX']:
        
        rnk = "r_%d" % (rec['rank'])

        fnr = rec['id']
        fnr = nrecs[fnr]

        # determine mount point
        mnt = None
        for curr in mnts:
            if re.search(curr, fnr):
                mnt = curr
                break
        mnt = "m_%s" % mnt

       
        nodes[rnk] = {'name': rnk}
        nodes[fnr] = {'name': fnr}
        nodes[mnt] = {'name': mnt}

        #rnk2fnr += 1
        #fnr2mnt += 1


        rnk2fnr = "%s->%s" % (rnk, fnr)
        fnr2mnt = "%s->%s" % (fnr, mnt)

        if rnk2fnr not in edges:
            edges[rnk2fnr] = {"value": 0, "source": rnk, "target": fnr}
        edges[rnk2fnr]["value"] += 1


        if fnr2mnt not in edges:
            edges[fnr2mnt] = {"value": 0, "source": fnr, "target": mnt}
        edges[fnr2mnt]["value"] += 1


    ctx = {
        "nodes": list(nodes.values()),
        "links": list(edges.values())
    }


    tmp = json.dumps(ctx, cls=NumpyEncoder)
    tmp = json.loads(tmp)

    self.report['sankey'] = tmp





