from darshan.report import *


def configure_groups():
    """
    Prepare a dictionary to lookup high level group ordering.
    """
    from collections import OrderedDict 

    order = 0.0
    group_config = OrderedDict()

    group_config['H5F'] = {}
    group_config['H5D'] = {}
    group_config['MPIIO'] = {}
    group_config['DXT_MPIIO'] = {}
    group_config['STDIO'] = {}
    group_config['POSIX'] = {}
    group_config['DXT_POSIX'] = {}
    group_config['LUSTRE'] = {}

    # apply order
    for k,v in group_config.items():
        v['order'] = order
        order += 1.0

    return group_config

def purge_empty_nestedGroups(groups):
    for k,v in groups.items():
        if 'nestedGroups' in v:
            if len(v['nestedGroups']) == 0:
                v.pop('nestedGroups', None)
                v.pop('showNested', None)

def normalize_mod(mod, inverse=False):
    mapping = {
        'MPI-IO': 'MPIIO'
    }

    inverse_mapping = {
        'MPIIO': 'MPI-IO'
    }

    if inverse and mod in inverse_mapping:
        return inverse_mapping[mod]

    if mod in mapping:
        return mapping[mod]

    return mod


def update_parent_item(self, groups, items, parent_id, start=None, end=None):
    item = None
    if parent_id in items:
        item = items[parent_id]
    else:
        items[parent_id] = {
            'id': parent_id,
            'group': parent_id,
            'content': '',
            'start': start,
            'end': end
        }
        item = items[parent_id]

    if start < item['start']:
        item['start'] = start
    if end > item['end']:
        item['end'] = end


def summarized_items(self, groups, items, mod, nmod, rec, rec_id, group_id, parent_group):
    if mod in ['LUSTRE']:
        # skip, nothing to summarize
        return
    
    item_id = f'{group_id}'
    start = float('inf')
    end = float('-inf')

    
    drec = {}
    drec['fcounters'] = dict(zip(self.counters[mod]['fcounters'], rec['fcounters']))


    # find min/max for starttime and endtime
    ops = ['OPEN', 'CLOSE']
    if mod not in ['H5F']:
        # add the following ops for all but H5F, ...
        ops += ['READ', 'WRITE']
    for op in ops:
        item_id = f'{group_id}:{op}'
        cur_start = drec['fcounters'][f'{nmod}_F_{op}_START_TIMESTAMP']
        cur_end = drec['fcounters'][f'{nmod}_F_{op}_END_TIMESTAMP']
              
        if cur_start < start:
            start = cur_start      
        if cur_end > end:
            end = cur_end
    
    # add item
    if item_id not in items:
        items[item_id] = {
            'id': item_id,
            'group': group_id,
            'content': '',
            'start': start,
            'end': end
        }

    # order by first access
    if start < groups[group_id]['order']:
        groups[group_id]['order'] = start 
        
    update_parent_item(self, groups, items, parent_group, start=start, end=end)


def compress_pathname(pathname):
    max_len = 42
    
    if len(pathname) < max_len:
        return pathname
    
    elems = pathname.split('/')
    
    #if len(elems[-1]) < max_len:
    #    return elems[-1]
    
    snip = '...'
    
    return pathname[0:int((max_len-len(snip))/2)]  + snip + elems[-1][-int((max_len-len(snip))/2):]




def create_timeline(self, 
        group_by='mod,file,rank', 
        action='attach,overwrite', summary_name='timeline'
        ):
    """
    Generate/update a timeline from records of the current report state.

    Args:
        group_by (str): By which factor to group entries (default: rank)
                        Allowed Parameters: rank, filename
    """

    report = self

    groups = {}
    items = {}
    

    group_config = configure_groups()
    #start_time = datetime.datetime.fromtimestamp( self.data['metadata']['job']['start_time'] )



    for mod in report.modules:
        nmod = normalize_mod(mod)
        group_id = nmod
        
        groups[group_id] = {
            'id': group_id, 
            'content': f'{group_id}',
            'order': group_config[nmod]['order'],
            'nestedGroups': [], # to be filled later
            'showNested': False
        }

    
    for mod in report.modules:
        if mod in ['DXT_POSIX', 'DXT_MPIIO']:
            continue
        
        nmod = normalize_mod(mod)
        parent_group = nmod
        
        for rec in report.records[mod]:
            rec_id = rec['id']
            group_id = f'{nmod}:{rec_id}' 
            
            # add group
            if group_id not in groups:           
                groups[group_id] = {
                    'id': group_id, 
                    'content':
                        '<b>' +
                        compress_pathname(report.name_records[rec['id']]) +
                        '</b><br>' + 
                        f'{group_id}' + 
                        '',
                    'order': float('inf'),
                    'title': report.name_records[rec['id']],
                    'nestedGroups': [], # to be filled later
                    'showNested': False
                }
                groups[parent_group]['nestedGroups'].append(group_id)
            
            # add items
            #detailed_items(groups, items, mod, nmod, rec, rec_id, group_id, parent_group)
            summarized_items(self, groups, items, mod, nmod, rec, rec_id, group_id, parent_group)


    purge_empty_nestedGroups(groups)

    # flatten dictionaries to list
    timeline = {
        'groups': [v for k,v in groups.items()],
        'items': [v for k,v in items.items()]
    }

    # overwrite existing summary entry
    if action == "attach,overwrite":
        self.summary[summary_name] = timeline

    return timeline


