#!/usr/bin/python
# -*- coding: utf8
import logging
from collections import OrderedDict, namedtuple, defaultdict
import doctest
from hashlib import md5
import hashlib
import copy
from pprint import pprint
from ddlgenerator.reserved import sql_reserved_words
import re
try:
    import ddlgenerator.typehelpers as th
except ImportError:
    import typehelpers as th # TODO: can py2/3 split this

_illegal_in_column_name = re.compile(r'[^a-zA-Z0-9_$#]')
def clean_key_name(key):
    """
    Makes ``key`` a valid and appropriate SQL column name:

    1. Replaces illegal characters in column names with ``_``

    2. Prevents name from beginning with a digit (prepends ``_``)

    3. Lowercases name.  If you want case-sensitive table
    or column names, you are a bad person and you should feel bad.
    """
    result = _illegal_in_column_name.sub("_", key.strip())
    if result[0].isdigit():
        result = '_%s' % result
    if result.upper() in sql_reserved_words:
        result = '_%s' % key
    return result.lower()

def walk_and_clean(data):
    """
    Recursively walks list of dicts (which may themselves embed lists and dicts),
    transforming namedtuples to OrderedDicts and
    using ``clean_key_name(k)`` to make keys into SQL-safe column names

    >>> data = [{'a': 1}, [{'B': 2}, {'B': 3}], {'F': {'G': 4}}]
    >>> pprint(walk_and_clean(data))
        [OrderedDict([('a', 1)]),
         [OrderedDict([('b', 2)]), OrderedDict([('b', 3)])],
          OrderedDict([('f', OrderedDict([('g', 4)]))])]
    """
    # transform namedtuples to OrderedDicts
    if hasattr(data, '_fields'):
        data = OrderedDict((k,v) for (k,v) in zip(data._fields, data))
    # Recursively clean up child dicts and lists
    if hasattr(data, 'items') and hasattr(data, '__setitem__'):
        for (key, val) in data.items():
            data[key] = walk_and_clean(val)
    elif isinstance(data, list) or isinstance(data, tuple) \
         or hasattr(data, '__next__') or hasattr(data, 'next'):
        data = [walk_and_clean(d) for d in data]

    # Clean up any keys in this dict itself
    if hasattr(data, 'items'):
        original_keys = data.keys()
        tup = ((clean_key_name(k), v) for (k, v) in data.items())
        data = OrderedDict(tup)
        if len(data) < len(original_keys):
            raise KeyError('Cleaning up %s created duplicates' %
                           original_keys)
    return data

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

class UniqueKey(object):
    """
    Provides unique IDs.

    >>> idp1 = UniqueKey('id', int, max=4)
    >>> idp1.next()
    5
    >>> idp1.next()
    6
    >>> idp2 = UniqueKey('id', str)
    >>> id2 = idp2.next()
    >>> (len(id2), type(id2))
    (32, <class 'str'>)
    """
    def __init__(self, key_name, key_type, max=0):
        self.name = key_name
        if key_type != int and not hasattr(key_type, 'lower'):
            raise NotImplementedError("Primary key field %s is %s, must be string or integer"
                                      % (key_name, key_type))
        self.type = key_type
        self.max = max
    def next(self):
        if self.type == int:
            self.max += 1
            return self.max
        else:
            return md5().hexdigest()

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
        new_field_names = ['%s_%s' % (key, child_key.strip('_')) for child_key in val]
        overlap = (set(new_field_names) & set(parent)) - set(id or [])
        if overlap:
            logging.error("Could not unnest child %s; %s present in %s"
                          % (name, key, ','.join(overlap), parent_name))
            return
        for (child_key, child_val) in val.items():
            new_field_name = '%s_%s' % (key, child_key.strip('_'))
            parent[new_field_name] = child_val
        parent.pop(key)

_sample_data = [{'province': 'Québec', 'capital': {'name': 'Québec City', 'pop': 491140},
                 'id': 1, 'province_id': 1,
                 'cities': [{'name': 'Montreal', 'pop': 1649519}, {'name': 'Laval', 'pop': 401553}]},
                {'province': 'Ontario', 'capital': {'name': 'Toronto', 'pop': 2615060}, 'province_id': 2,
                 'cities': [{'name': 'Ottawa', 'pop': 883391}, {'name': 'Missisauga', 'pop': 713443}]},
                {'province': 'New Brunswick', 'capital': {'name': 'Fredricton', 'pop': 56224},
                 'id': 3, 'province_id': 3,
                 'cities': [{'name': 'Saint John', 'pop': 70063}, {'name': 'Moncton', 'pop': 69074}]},
               ]

def all_values_for(data, field_name):
    return [row.get(field_name) for row in data if field_name in row]

def unused_field_name(data, preferences):
    for pref in preferences:
        if not all_values_for(data, pref):
            return pref
    raise KeyError("All desired names already taken in %s" % self.name)

class ParentTable(list):
    """
    List of ``dict``s that knows (or creates) its own primary key field.

    >>> provinces = ParentTable(_sample_data, 'province', pk_name='province_id')
    >>> provinces.pk.name
    'province_id'
    >>> [p[provinces.pk.name] for p in provinces]
    [1, 2, 3]
    >>> provinces.pk.max
    3

    Now if province_id is unusable because it's nonunique:
    >>> data2 = copy.deepcopy(_sample_data)
    >>> for row in data2: row['province_id'] = 4
    >>> provinces2 = ParentTable(data2, 'province', pk_name='id', force_pk=True)
    >>> provinces2.pk.name
    'id'
    >>> [p[provinces2.pk.name] for p in provinces2]
    [1, 4, 3]

    """
    def is_in_all_rows(self, value):
        return len([1 for r in self if r.get(value)]) == len(self)

    def __init__(self, data, singular_name, pk_name=None, force_pk=False):
        self.name = singular_name
        super(ParentTable, self).__init__(data)
        self.pk_name = pk_name
        if force_pk or (self.pk_name and self.is_in_all_rows(self.pk_name)):
            self.assign_pk()
        else:
            self.pk = None

    def suitability_as_key(self, key_name):
        """
        Returns: (result, key_type)
        ``result`` is True, False, or 'absent' or 'partial' (both still usable)
        ``key_type`` is ``int`` for integer keys or ``str`` for hash keys

        """
        pk_values = all_values_for(self, key_name)
        if not pk_values:
            return ('absent', int)  # could still use it
        key_type = type(th.best_coercable(pk_values))
        num_unique_values = len(set(pk_values))
        if num_unique_values < len(pk_values):
            return (False, None)     # non-unique
        if num_unique_values == len(self):
            return (True, key_type)  # perfect!
        return ('partial', key_type) # unique, but some rows need populating

    def use_this_pk(self, pk_name, key_type):
        if key_type == int:
            self.pk = UniqueKey(pk_name, key_type, max([0, ] + all_values_for(self, pk_name)))
        else:
            self.pk = UniqueKey(pk_name, key_type)

    def assign_pk(self):
        """

        """
        if not self.pk_name:
            self.pk_name = '%s_id' % self.name
            logging.warning('Primary key %s.%s not requested, but nesting demands it'
                            % (self.name, self.pk_name))
        (suitability, key_type) = self.suitability_as_key(self.pk_name)
        if not suitability:
            raise Exception('Duplicate values in %s.%s, unsuitable primary key'
                            % (self.name, self.pk_name))
        self.use_this_pk(self.pk_name, key_type)
        if suitability in ('absent', 'partial'):
            for row in self:
                if self.pk_name not in row:
                    row[self.pk_name] = self.pk.next()


def unnest_children(data, parent_name='', pk_name=None, force_pk=False):
    """
    For each ``key`` in each row of ``data`` (which must be a list of dicts),
    unnest any dict values into ``parent``, and remove list values into separate lists.

    Return (``data``, ``pk_name``, ``children``, ``child_fk_names``) where

    ``data``
      the transformed input list
    ``pk_name``
      field name of ``data``'s (possibly new) primary key
    ``children``
      a defaultdict(list) of data extracted from child lists
    ``child_fk_names``
      dict of the foreign key field name in each child

    """
    possible_fk_names = ['%s_id' % parent_name, '_%s_id' % parent_name, 'parent_id', ]
    if pk_name:
        possible_fk_names.insert(0, '%s_%s' % (parent_name, pk_name.strip('_')))
    children = defaultdict(list)
    field_names_used_by_children = defaultdict(set)
    child_fk_names = {}
    parent = ParentTable(data, parent_name, pk_name=pk_name, force_pk=force_pk)
    for row in parent:
        try:
            for (key, val) in list(row.items()):
                if hasattr(val, 'items'):
                    unnest_child_dict(parent=row, key=key, parent_name=parent_name)
                elif isinstance(val, list) or isinstance(val, tuple):
                    # force listed items to be dicts, not scalars
                    row[key] = [v if hasattr(v, 'items') else {key: v} for v in val]
        except AttributeError:
            raise TypeError('Each row should be a dictionary, got %s: %s' % (type(row), row))
        for (key, val) in row.items():
            if isinstance(val, list) or isinstance(val, tuple):
                for child in val:
                    field_names_used_by_children[key].update(set(child.keys()))
    for (child_name, names_in_use) in field_names_used_by_children.items():
        if not parent.pk:
            parent.assign_pk()
        for fk_name in possible_fk_names:
            if fk_name not in names_in_use:
                break
        else:
            raise Exception("Cannot find unused field name in %s.%s to use as foreign key"
                            % (parent_name, child_name))
        child_fk_names[child_name] = fk_name
        for row in parent:
            if child_name in row:
                for child in row[child_name]:
                    child[fk_name] = row[parent.pk.name]
                    children[child_name].append(child)
                row.pop(child_name)
    # TODO: What if rows have a mix of scalar / list / dict types?
    return (parent, parent.pk.name if parent.pk else None, children, child_fk_names)

if __name__ == '__main__':
    doctest.testmod(optionflags=doctest.NORMALIZE_WHITESPACE)
