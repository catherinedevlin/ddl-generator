#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Given data, automatically guess-generates DDL to create SQL tables.

Invoke with one table's worth of data at a time, from command line::

    $ ddlgenerator postgresql sourcedata.yaml
    
The ``-i`` flag generates INSERT statements as well::

    $ ddlgenerator -i postgresql sourcedata.yaml
    
or from Python::

    >>> menu = Table('../tests/menu.json')
    >>> ddl = menu.ddl('postgresql')
    >>> inserts = menu.inserts('postgresql')
    >>> all_sql = menu.sql('postgresql', inserts=True)

Use ``-k <keyname>`` or ``--key=<keyname>`` to set ``keyname`` as the table's
primary key.  If the field does not exist, it will be added.  If ``-k`` is not given,
no primary key will be created, *unless* it is required to set up child tables
(split out from sub-tables nested inside the original data).

You will need to hand-edit the resulting SQL to add indexes.

You can use wildcards to generate from multiple files at once::

    $ ddlgenerator postgresql "*.csv"

Remember to enclose the file path in quotes to prevent the shell
from expanding the argument (if it does, ddlgenerator will run
against each file *separately*, setting up one table for each).
"""
from collections import OrderedDict, defaultdict
from io import StringIO 
import copy
import csv
import datetime
from decimal import Decimal
import doctest
import functools
import json
import logging
import os.path
import re
import pickle
import shelve
import pprint
import textwrap
import sqlalchemy as sa
from sqlalchemy.schema import CreateTable
import dateutil.parser
import yaml
try:
    import pymongo
except ImportError:
    pymongo = None
from data_dispenser.sources import Source
try:
    import ddlgenerator.typehelpers as th
    from ddlgenerator import reshape
except ImportError:
    import typehelpers as th # TODO: can py2/3 split this
    import reshape
    
logging.basicConfig(filename='ddlgenerator.log', filemode='w')
metadata = sa.MetaData()

class KeyAlreadyExists(KeyError):
    pass

dialect_names = 'drizzle firebird mssql mysql oracle postgresql sqlite sybase'.split()

def ordered_yaml_load(stream, Loader=yaml.Loader, object_pairs_hook=OrderedDict):
    """
    Preserves order with OrderedDict as yaml is loaded
    Thanks to coldfix
    http://stackoverflow.com/questions/5121931/in-python-how-can-you-load-yaml-mappings-as-ordereddicts
    """
    class OrderedLoader(Loader):
        pass
    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        lambda loader, node: object_pairs_hook(loader.construct_pairs(node)))
    return yaml.load(stream, OrderedLoader)

json_loader = functools.partial(json.loads, object_pairs_hook=OrderedDict)
json_loader.__name__ = 'json_loader'
    
def _eval_csv(target):
    """
    Yields OrderedDicts from a CSV string
    """
    reader = csv.DictReader(StringIO(target))
    return [(OrderedDict((k, row[k]) for k in reader.fieldnames)) for row in reader]
            
def _dump(sql, *multiparams, **params):
    pass
   
mock_engines = {}
for engine_name in ('postgresql', 'sqlite', 'mysql', 'oracle', 'mssql'):
    mock_engines[engine_name] = sa.create_engine('%s://' % engine_name, 
                                                 strategy='mock', executor=_dump)

class Table(object):
    """
    >>> data = '''
    ... - 
    ...   name: Lancelot
    ...   kg: 69.4 
    ...   dob: 9 jan 461
    ... -
    ...   name: Gawain
    ...   kg: 69.4  '''
    >>> print(Table(data, "knights").ddl('postgresql').strip())
    DROP TABLE IF EXISTS knights;
    CREATE TABLE knights (
    	name VARCHAR(8) NOT NULL, 
    	kg DECIMAL(3, 1) NOT NULL, 
    	dob TIMESTAMP WITHOUT TIME ZONE, 
    	UNIQUE (name), 
    	UNIQUE (dob)
    );
    """
    
        
    eval_funcs_by_ext = {'.py': [eval, ],
                         '.json': [json_loader, ],
                         '.yaml': [ordered_yaml_load, ],
                         '.csv': [_eval_csv, ], 
                         '.pickle': [pickle.loads, ],
                         }
    eval_funcs_by_ext['.json'][0].__name__ = 'json'
    eval_funcs_by_ext['*'] = [eval, ] + eval_funcs_by_ext['.json'] + eval_funcs_by_ext['.yaml'] \
                             + eval_funcs_by_ext['.csv'] 
                        
    _looks_like_filename = re.compile(r'^[^\s\,]+$') 
    def _load_data(self, data):
        """
        Populates ``self.data`` from ``data``, whether ``data`` is a 
        string of JSON or YAML, a filename containing the same, 
        or simply Python data. 
        """
        if pymongo and isinstance(data, pymongo.collection.Collection):
            self.table_name = data.name
            data = data.find()
        if hasattr(data, 'read'): # duck-type open file object test
            data = data.read()    # and then go on and handle the data as a string
        file_extension = '*'
        if hasattr(data, 'lower'):  # duck-type string test
            if os.path.isfile(data):
                (file_path, file_extension) = os.path.splitext(data)
                self.table_name = os.path.split(file_path)[1]
                logging.info('Reading data from %s' % data)
                if data.endswith('.pickle'):
                    file_mode = 'rb'
                else:
                    file_mode = 'rU'
                with open(data, file_mode) as infile:
                    data = infile.read()
                    if file_mode == 'rU' and hasattr(data, 'decode'):
                        data = data.decode('utf8')  # TODO: fix messy Py 2/3 unicode problem
                logging.info('Reading data from %s' % data)
            else:
                logging.debug('Not called with a valid file path')
            funcs = self.eval_funcs_by_ext.get(file_extension, self.eval_funcs_by_ext['*'])
            for func in funcs:
                logging.debug('Applying %s to data' % func.__name__)
                try:
                    self.data = func(data)
                    if hasattr(self.data, 'lower'):
                        logging.warning("Data was interpreted as a single string - no table structure:\n%s" 
                                        % self.data[:100])                    
                    elif self.data:
                        logging.info('Data successfully interpreted with %s' % func.__name__)
                        return 
                except Exception as e:  # our deserializers may throw a variety of errors
                    logging.warning('Could not interpret data; %s threw\n%s' % (func.__name__, str(e)))
                    pass
            logging.critical('All interpreters failed')
            if self._looks_like_filename.search(data):
                raise IOError("Filename not found")
            else:
                raise SyntaxError("Unable to interpret data")
        else:
            self.data = data
        if not self.data:
            raise SyntaxError("No data found")
        
    table_index = 0
    
    def __init__(self, data, table_name=None, default_dialect=None, 
                 save_metadata_to=None, metadata_source=None,
                 varying_length_text = False, uniques=False, 
                 pk_name=None, force_pk=False,
                 _parent_table=None, _fk_field_name=None, reorder=False,
                 loglevel=logging.WARN):
        """
        Initialize a Table and load its data.
        
        If ``varying_length_text`` is ``True``, text columns will be TEXT rather than VARCHAR.
        This *improves* performance in PostgreSQL.
        
        If a ``metadata<timestamp>`` YAML file generated from a previous ddlgenerator run is
        provided, *only* ``INSERT`` statements will be produced, and the table structure 
        determined during the previous run will be assumed.
        """
        logging.getLogger().setLevel(loglevel) 
        self.table_name = None
        self.varying_length_text = varying_length_text
        self._load_data(data)
        if hasattr(self.data, 'lower'):
            raise SyntaxError("Data was interpreted as a single string - no table structure:\n%s" 
                              % self.data[:100])
        self.table_name = table_name or self.table_name or 'generated_table%s' % Table.table_index
        Table.table_index += 1
        self.table_name = reshape.clean_key_name(self.table_name)
        
        if not hasattr(self.data, 'append') and not hasattr(self.data, '__next__') \
            and not hasattr(self.data, 'next'):
            self.data = [self.data,]
        self.data = reshape.walk_and_clean(self.data)
        
        (self.data, self.pk_name, children, child_fk_names) = reshape.unnest_children(
            data=self.data, parent_name=self.table_name, pk_name=pk_name, force_pk=force_pk)
      
        self.default_dialect = default_dialect
        self.comments = {}
        child_metadata_sources = {}
        if metadata_source:
            if isinstance(metadata_source, OrderedDict):
                logging.info('Column metadata passed in as OrderedDict')
                self.columns = metadata_source
            else:
                logging.info('Pulling column metadata from file %s' % metadata_source)
                with open(metadata_source) as infile:
                    self.columns = yaml.load(infile.read())
            for (col_name, col) in self.columns.items():
                if isinstance(col, OrderedDict):
                    child_metadata_sources[col_name] = col
                    self.columns.pop(col_name)
                else:
                    self._fill_metadata_from_sample(col)
        else:
            self._determine_types(varying_length_text, uniques=uniques)

        if reorder:
            ordered_columns = OrderedDict()
            if pk_name and pk_name in self.columns:
                ordered_columns[pk_name] = self.columns.pop(pk_name)
            for (c, v) in sorted(self.columns.items()):
                ordered_columns[c] = v
            self.columns = ordered_columns    
            
        if _parent_table:
            fk = sa.ForeignKey('%s.%s' % (_parent_table.table_name, _parent_table.pk_name))
        else:
            fk = None
            
        column_args = []
        self.table = sa.Table(self.table_name, metadata, 
                              *[sa.Column(cname, col['satype'], 
                                          fk if fk and (_fk_field_name == cname) else None,
                                          primary_key=(cname == self.pk_name),
                                          unique=col['is_unique'],
                                          nullable=col['is_nullable'],
                                          doc=self.comments.get(cname)) 
                                for (cname, col) in self.columns.items()
                                if True
                                ])
      
        self.children = {child_name: Table(child_data, table_name=child_name, 
                                           default_dialect=self.default_dialect, 
                                           varying_length_text = varying_length_text, 
                                           uniques=uniques, pk_name=pk_name, force_pk=force_pk,
                                           _parent_table=self, reorder=reorder,
                                           _fk_field_name = child_fk_names[child_name],
                                           metadata_source = child_metadata_sources.get(child_name),
                                           loglevel=loglevel)
                         for (child_name, child_data) in children.items()}
       
        if save_metadata_to:
            if not save_metadata_to.endswith(('.yml','yaml')):
                save_metadata_to += '.yaml'
            with open(save_metadata_to, 'w') as outfile:
                outfile.write(yaml.dump(self._saveable_metadata()))
            logging.info('Pass ``--save-metadata-to %s`` next time to re-use structure' %
                         save_metadata_to)

    def _saveable_metadata(self):
        result = copy.copy(self.columns)
        for v in result.values():
            v.pop('satype')  # yaml chokes on sqla classes
        for (child_name, child) in self.children.items():
            result[child_name] = child._saveable_metadata()
        return result
    
    def _dialect(self, dialect):
        if not dialect and not self.default_dialect:
            raise KeyError("No SQL dialect specified")
        dialect = dialect or self.default_dialect
        if dialect not in mock_engines:
            raise NotImplementedError("SQL dialect '%s' unknown" % dialect)        
        return dialect
    
    _supports_if_exists = {k: False for k in dialect_names}
    _supports_if_exists['postgresql'] = _supports_if_exists['sqlite'] = True
    _supports_if_exists['mysql'] = _supports_if_exists['sybase'] = True
    def _dropper(self, dialect):
        template = "DROP TABLE %s %s" 
        if_exists = "IF EXISTS" if self._supports_if_exists[dialect] else ""
        return template % (if_exists, self.table_name)
            
    _comment_wrapper = textwrap.TextWrapper(initial_indent='-- ', subsequent_indent='-- ')
    def ddl(self, dialect=None, creates=True, drops=True):
        """
        Returns SQL to define the table.
        """
        dialect = self._dialect(dialect)
        creator = CreateTable(self.table).compile(mock_engines[dialect]) 
        creator = "\n".join(l for l in str(creator).splitlines() if l.strip()) # remove empty lines 
        comments = "\n\n".join(self._comment_wrapper.fill("in %s: %s" % 
                                                        (col, self.comments[col])) 
                                                        for col in self.comments)
        result = []
        if drops:
            result.append(self._dropper(dialect) + ';')
        if creates:
            result.append("%s;\n%s" % (creator, comments))
        for child in self.children.values():
            result.append(child.ddl(dialect=dialect, creates=creates, drops=drops))
        return '\n\n'.join(result)
        
      
    _datetime_format = {}  # TODO: test the various RDBMS for power to read the standard
    def _prep_datum(self, datum, dialect, col):
        pytype = self.columns[col]['pytype']
        if pytype == datetime.datetime:
            datum = dateutil.parser.parse(datum)
        elif pytype == bool:
            datum = th.coerce_to_specific(datum)
        else:
            datum = pytype(str(datum))
        if isinstance(datum, datetime.datetime):
            if dialect in self._datetime_format:
                return datum.strftime(self._datetime_format[dialect])
            else:
                return "'%s'" % datum
        elif hasattr(datum, 'lower'):
            return "'%s'" % datum.replace("'", "''")
        else:
            return datum
        
    _insert_template = "INSERT INTO {table_name} ({cols}) VALUES ({vals});" 
    def inserts(self, dialect=None):
        dialect = self._dialect(dialect)
        for row in self.data:
            cols = ", ".join(c for c in row)
            vals = ", ".join(str(self._prep_datum(val, dialect, key)) for (key, val) in row.items())
            yield self._insert_template.format(table_name=self.table_name, cols=cols, vals=vals)
        for child in self.children.values():
            for row in child.inserts(dialect):
                yield row
            
    def sql(self, dialect=None, inserts=False, creates=True, drops=True, metadata_source=None):
        """
        Combined results of ``.ddl(dialect)`` and, if ``inserts==True``, ``.inserts(dialect)``.
        """
        result = [self.ddl(dialect, creates=creates, drops=drops)]
        if inserts:
            for row in self.inserts(dialect):
                result.append(row)
        return '\n'.join(result)
        
    def __str__(self):
        if self.default_dialect:
            return self.ddl()
        else:
            return self.__repr__()
        
    def _fill_metadata_from_sample(self, col):
        col['pytype'] = type(col['sample_datum'])
        if isinstance(col['sample_datum'], Decimal):
            col['satype'] = sa.DECIMAL(*th.precision_and_scale(col['sample_datum']))
        elif isinstance(col['sample_datum'], str):
            if self.varying_length_text:
                col['satype'] = sa.Text()
            else:
                col['satype'] = sa.Unicode(len(col['sample_datum']))
        else:
            col['satype'] = self.types2sa[type(col['sample_datum'])]        
        return col
        
    types2sa = {datetime.datetime: sa.DateTime, int: sa.Integer, 
                float: sa.Numeric, bool: sa.Boolean}
   
    def _determine_types(self, varying_length_text=False, uniques=False):
        column_data = OrderedDict()
        self.columns = OrderedDict() 
        self.comments = {}
        rowcount = 0
        for row in self.data:
            rowcount += 1
            keys = row.keys()
            for col_name in self.columns:
                if col_name not in keys:
                    self.columns[col_name]['is_nullable'] = True
            if not isinstance(row, OrderedDict):
                keys = sorted(keys)
            for k in keys:
                v = row[k]
                if not th.is_scalar(v):
                    v = str(v)
                    self.comments[k] = 'nested values! example:\n%s' % pprint.pformat(v)
                    logging.warning('in %s: %s' % (k, self.comments[k]))
                v = th.coerce_to_specific(v)
                if k not in self.columns:
                    self.columns[k] = {'sample_datum': v, 'is_unique': True, 
                                       'is_nullable': not (rowcount == 1 and v is not None), 
                                       'is_unique': set([v,])}
                else:
                    col = self.columns[k]
                    col['sample_datum'] = th.best_representative(col['sample_datum'], v)
                    if (v is None):
                        col['is_nullable'] = True
                    if (col['is_unique'] != False):
                        if v in col['is_unique']:
                            col['is_unique'] = False
                        else:
                            col['is_unique'].add(v)
        for col_name in self.columns:
            col = self.columns[col_name]
            self._fill_metadata_from_sample(col)
            col['is_unique'] = bool(col['is_unique'])
       
    
if __name__ == '__main__':
    doctest.testmod(optionflags=doctest.NORMALIZE_WHITESPACE)    
