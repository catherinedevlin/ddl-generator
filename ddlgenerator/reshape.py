import logging
from collections import OrderedDict, namedtuple, defaultdict
import doctest
from hashlib import md5
import hashlib
from pprint import pprint

def namedtuples_to_ordereddicts(data):
    """
    For each item in a list of ``data``, if it is a ``namedtuple``,
    convert it to an ``OrderedDict``.
    
    Does not drill down into child objects.
    """
    return [ OrderedDict((k,v) for (k,v) in zip(row._fields, row))
             if hasattr(row, '_fields') else row
             for row in data]

def _id_fieldname(fieldnames, table_name = ''):
    """
    Finds the field name from a dict likeliest to be its unique ID
    
    >>> _id_fieldname({'bar': True, 'id': 1}, 'foo')
    'id'
    >>> _id_fieldname({'bar': True, 'foo_id': 1, 'goo_id': 2}, 'foo')
    'foo_id'
    >>> _id_fieldname({'bar': True, 'baz': 1, 'baz_id': 3}, 'foo')
    """
    templates = ['%s_%%s' % table_name, '%s', '_%s']
    for stub in ['id', 'num', 'no', 'number']:
        for t in templates:
            if t % stub in fieldnames:
                return t % stub

class ID_Giver(object):
    """
    Works like a database's SEQUENCE to assign ascending numeric IDs.
    
    Initialize once per program, pass table name.
    
    >>> test_id_giver = ID_Giver()
    >>> test_id_giver.assign('tbl1')
    1
    >>> test_id_giver.assign('tbl1')
    2
    >>> test_id_giver.assign('tbl2')
    1 
    """
    def __init__(self):
        self.sequences = defaultdict(int)
       
    def assign(self, table_name):
        self.sequences[table_name] += 1
        return self.sequences[table_name]
       
id_giver = ID_Giver()   # TODO: maybe should be a singleton?

class ID_provider(object):
    """
    Provides unique IDs.
    
    >>> idp1 = ID_provider(int, max=4)
    >>> idp1.next()
    5
    >>> idp1.next()
    6
    >>> idp2 = ID_provider(str)
    >>> id2 = idp2.next()
    >>> (len(id2), type(id2))
    (32, <class 'str'>)
    """
    def __init__(self, type, max=0):
        if type not in (str, int):
            raise NotImplementedError("can only work with ``str`` or ``int`` IDs")
        self.type = type
        self.max = max
    def next(self):
        if self.type == int:
            self.max += 1
            return self.max
        else:
            return md5().hexdigest()
        
def ensure_id(dct, table_name):
    """
    Finds an *id field in ``dct``; creates it if absent;
    returns (its name, its value)
    """
    id = self._id_fieldname(dct, key)
    if not id:
        logging.warning('%s lacks ID field, adding' % self.table_name)
        dct['id'] = id_giver(table_name)
        id = 'id'
    return (id, dct[id])
   
def unnest_child_dict(parent, key, parent_name=''):
    """
    If ``parent`` dictionary has a ``key`` whose ``val`` is a dict,
    unnest ``val``'s fields into ``parent`` and remove ``key``.
    
    >>> parent = {'province': 'Québec', 'capital': {'name': 'Québec City', 'pop': 491140}}
    >>> unnest_child_dict(parent, 'capital', 'provinces')
    >>> pprint(parent)
    {'capital_name': 'Québec City', 'capital_pop': 491140, 'province': 'Québec'}

    >>> parent = {'province': 'Québec', 'capital': {'id': 1, 'name': 'Québec City', 'pop': 491140}}
    >>> unnest_child_dict(parent, 'capital', 'provinces')
    >>> pprint(parent)
    {'capital_id': 1,
     'capital_name': 'Québec City',
     'capital_pop': 491140,
     'province': 'Québec'}

    >>> parent = {'province': 'Québec', 'capital': {'id': 1, 'name': 'Québec City'}}
    >>> unnest_child_dict(parent, 'capital', 'provinces')
    >>> pprint(parent)
    {'capital': 'Québec City', 'province': 'Québec'}
    
    """
    val = parent[key]
    name = "%s['%s']" % (parent_name, key)
    logging.debug("Unnesting dict %s" % name)
    id = _id_fieldname(val, parent_name)
    if id:
        logging.debug("%s is %s's ID" % (id, key))
        if len(val) <= 2:
            logging.debug('Removing ID column %s.%s' % (key, id))
            val.pop(id)
    if len(val) == 0:
        logging.debug('%s is empty, removing from %s' % (name, parent_name))
        parent.pop(key)
        return
    elif len(val) == 1:
        logging.debug('Nested one-item dict in %s, making scalar.' % name)
        parent[key] = list(val.values())[0]
        return
    else:
        logging.debug('Pushing all fields from %s up to %s' % (name, parent_name))
        new_field_names = ['%s_%s' % (key, child_key) for child_key in val]
        overlap = (set(new_field_names) & set(parent)) - set(id or [])
        if overlap:
            logging.error("Could not unnest child %s; %s present in %s"
                          % (name, key, ','.join(overlap), parent_name))
            return
        for (child_key, child_val) in val.items():
            new_field_name = '%s_%s' % (key, child_key)
            parent[new_field_name] = child_val
        parent.pop(key)

class ParentTable(list):
    """
    List of ``dict``s that knows (or creates) its own primary key field. 
    """
    def __init__(self, data, singular_name):
        """
        >>> data =     [{'province': 'Québec', 'capital': {'name': 'Québec City', 'pop': 491140},
        ...              'cities': [{'name': 'Montreal', 'pop': 1649519}, {'name': 'Laval', 'pop': 401553}]},
        ...              {'province': 'Québec', 'capital': {'name': 'Québec City', 'pop': 491140},
        ...              'cities': [{'name': 'Montreal', 'pop': 1649519}, {'name': 'Laval', 'pop': 401553}]},
        ...             ]
        >>> provinces = ParentTable(data, 'province')
        >>> provinces.pk_name
        'id'
       
        """
        self.name = singular_name
        super(ParentTable, self).__init__(data)
        self.assign_pk()
       
    def all_values_for(self, key_name): 
        return [row.get(key_name) for row in self if row.get(key_name)]

    def suitability_as_key(self, key_name):
        """
        Returns: (result, key_type)
        ``result`` is True, False, or 'absent' or 'partial' (both still usable)
        ``key_type`` is ``int`` for integer keys or ``str`` for hash keys
        
        >>> data =     [{'province': 'Québec', 'capital': {'name': 'Québec City', 'pop': 491140},
        ...              'cities': [{'name': 'Montreal', 'pop': 1649519}, {'name': 'Laval', 'pop': 401553}]},
        ...              {'province': 'Québec', 'capital': {'name': 'Québec City', 'pop': 491140},
        ...              'cities': [{'name': 'Montreal', 'pop': 1649519}, {'name': 'Laval', 'pop': 401553}]},
        ...             ]
        >>> provinces = ParentTable(data, 'province')        
        >>>
        
        """
        pk_values = self.all_values_for(key_name)
        if not pk_values:
            return ('absent', int)  # could still use it
        types = set(type(v) for v in pk_values)
        if len(types) > 1:
            return (False, None)     # no heterogeneously typed keys!
        key_type = list(types)[0]
        num_unique_values = len(set(pk_values))
        if num_unique_values < len(pk_values):
            return (False, None)     # non-unique
        if num_unique_values == len(pk_values):
            return (True, key_type)  # perfect!
        return ('partial', key_type) # unique, but some rows need populating
       
    def use_this_pk(self, pk_name, key_type):
        self.pk_name = pk_name
        if key_type == int:
            self.pk_giver = ID_provider(key_type, max([0, ] + self.all_values_for(self.pk_name)))
        else:
            self.pk_giver = ID_provider(key_type)

    def assign_pk(self):
        preferences = ['id', '%s_id' % self.name, '_%s_id' % self.name, ]
        suitabilities = []
        for pk_name in preferences:
            (suitability, key_type) = self.suitability_as_key(pk_name)
            suitabilities.append((suitability, key_type))
            if not suitability:
                continue
            elif suitability == True:
                self.use_this_pk(pk_name, key_type)
                return
        # Fine, then, we'll manufacture a primary key
        for settle_for in ('absent', 'partial'):
            for (pk_name, (suitability, key_type)) in zip(preferences, suitabilities):
                if suitability == settle_for:
                    self.use_this_pk(pk_name, key_type)
                    for row in self:
                        if pk_name not in row:
                            row[pk_name] = self.pk_giver.next()
                    return
        raise Exception("""Failed to assign primary key to %s
                           Potential key names %s all in use but nonunique"""
                        % (','.join(preferences), self.name))
    
   
class ForeignKey(object):
    def __init__(self, data, list_name):
        """
        >>> provinces = [{'province': 'Québec', 'capital': {'name': 'Québec City', 'pop': 491140},
        ...              'cities': [{'name': 'Montreal', 'pop': 1649519}, {'name': 'Laval', 'pop': 401553}]},
        ...              {'province': 'Québec', 'capital': {'name': 'Québec City', 'pop': 491140},
        ...              'cities': [{'name': 'Montreal', 'pop': 1649519}, {'name': 'Laval', 'pop': 401553}]},
        ...             ]
        >>> provinces = ParentTable(provinces, 'province')
        >>> fk = ForeignKey(provinces, 'cities')
        
        # >>> fk.parent.name
        'province'
        # >>> fk.in_child
        'province.id'
        
        """
        
      
def unnest_children(data, parent_name=''):
    """
    For each ``key`` in each row of ``data`` (which must be a list of dicts),
    unnest any dict values into ``parent``, and remove list values into separate lists.
    
    Return a defaultdict(list) of data extracted from child lists.
    
    >>> provinces = [{'province': 'Québec', 'capital': {'name': 'Québec City', 'pop': 491140},
    ...              'cities': [{'name': 'Montreal', 'pop': 1649519}, {'name': 'Laval', 'pop': 401553}]},
    ...              {'province': 'Québec', 'capital': {'name': 'Québec City', 'pop': 491140},
    ...              'cities': [{'name': 'Montreal', 'pop': 1649519}, {'name': 'Laval', 'pop': 401553}]},
    ...             ]
    
    # >>> unnest_children(provinces)
    
    # >>> pprint(parent)
    
    """
    child_data = defaultdict(list)
    data = ParentTable(data, parent_name)
    for row in data:
        for (key, val) in row.items():
            if hasattr(val, 'items'): 
                unnest_child_dict(row, val, key)
            elif isinstance(val, list) or isinstance(val, tuple):
                for child in val:
                    if hasattr(child, 'items'):
                        if parent_id_key_name in child and child[parent_id_key_name] != parent_id:
                            errmsg = ('Cannot create %s in %s.%s, already present'
                                      % (parent_id_key_name, self.table_name, key))
                            raise KeyAlreadyExists(errmsg)
                        child[parent_id_key_name] = parent_id
                        self.children.append(child)
                        row.pop(key)


if __name__ == '__main__':
    doctest.testmod(optionflags=doctest.NORMALIZE_WHITESPACE)    