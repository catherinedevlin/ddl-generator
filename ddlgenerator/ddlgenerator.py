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

You will need to hand-edit the resulting SQL to add:

 - Primary keys
 - Foreign keys
 - Indexes
 - Delete unwanted UNIQUE indexes 
   (ddlgenerator adds them wherever a column's data is unique)
 
"""
from collections import OrderedDict
from io import StringIO 
import csv
import datetime
from decimal import Decimal
import doctest
import functools
import json
import logging
import os.path
import re
import pprint
import textwrap
import sqlalchemy as sa
from sqlalchemy.schema import CreateTable
import dateutil.parser
import yaml
try:
    import ddlgenerator.typehelpers as th
except ImportError:
    import typehelpers as th # TODO: can py2/3 split this
    
logging.basicConfig(filename='ddlgenerator.log', filemode='w')
metadata = sa.MetaData()

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
    ...   kg: 83
    ...   dob: 9 jan 461
    ... -
    ...   name: Gawain
    ...   kg: 69.4  '''
    >>> print(Table(data, "knights").ddl('postgresql').strip())
    DROP TABLE IF EXISTS knights;
    CREATE TABLE knights (
    	name VARCHAR(8) NOT NULL, 
    	kg DECIMAL(3, 1) NOT NULL, 
    	dob TIMESTAMP WITHOUT TIME ZONE 
    );
    """
    
        
    eval_funcs_by_ext = {'.py': [eval, ],
                         '.json': [json_loader, ],
                         '.yaml': [ordered_yaml_load, ],
                         '.csv': [_eval_csv, ], 
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
        TODO: accept open files
        """
        file_extension = None
        if hasattr(data, 'lower'):  # duck-type string test
            if os.path.isfile(data):
                (file_path, file_extension) = os.path.splitext(data)
                self.table_name = os.path.split(file_path)[1]
                logging.info('Reading data from %s' % data)
                with open(data) as infile:
                    data = infile.read()
                    if hasattr(data, 'decode'):
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
                        logging.warn("Data was interpreted as a single string - no table structure:\n%s" 
                                     % self.data[:100])                    
                    elif self.data:
                        logging.info('Data successfully interpreted with %s' % func.__name__)
                        return 
                except Exception as e:  # our deserializers may throw a variety of errors
                    logging.warn('Could not interpret data with %s' % func.__name__)
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
                  
    def __init__(self, data, table_name=None, default_dialect=None, varying_length_text = False, uniques=False,
                 loglevel=logging.WARN):
        """
        Initialize a Table and load its data.
        
        If ``varying_length_text`` is ``True``, text columns will be TEXT rather than VARCHAR.
        This *improves* performance in PostgreSQL.
        """
        logging.getLogger().setLevel(loglevel) 
        self.table_name = 'generated_table'
        self._load_data(data)
        if hasattr(self.data, 'lower'):
            raise SyntaxError("Data was interpreted as a single string - no table structure:\n%s" 
                              % self.data[:100])
        if table_name:
            # keep dots if explicitly given by user
            self.table_name = '.'.join(self._clean_column_name(piece) 
                                       for piece in table_name.split('.'))
        else:
            self.table_name = self._clean_column_name(self.table_name)
        if not hasattr(self.data, 'append'): # not a list
            self.data = [self.data,]
        # namedtuples to OrderedDicts
        self.data = [OrderedDict((k,v) for (k,v) in zip(row._fields, row))
                     if hasattr(row, '_fields') else row
                     for row in self.data]
        self.default_dialect = default_dialect
        self._determine_types(varying_length_text, uniques=uniques)
        self.table = sa.Table(self.table_name, metadata, 
                              *[sa.Column(c, t, 
                                          unique=self.is_unique[c],
                                          nullable=self.is_nullable[c],
                                          doc=self.comments.get(c)) 
                                for (c, t) in self.satypes.items()])

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
    def ddl(self, dialect=None):
        """
        Returns SQL to define the table.
        """
        dialect = self._dialect(dialect)
        creator = CreateTable(self.table).compile(mock_engines[dialect]) 
        creator = "\n".join(l for l in unicode(creator).splitlines() if l.strip()) # remove empty lines 
        comments = "\n\n".join(self._comment_wrapper.fill("in %s: %s" % 
                                                        (col, self.comments[col])) 
                                                        for col in self.comments)
        return "%s;\n%s;\n%s" % (self._dropper(dialect), creator, comments)
      
    _datetime_format = {}
    def _prep_datum(self, datum, dialect, col):
        pytype = self.pytypes[col]
        if pytype == datetime.datetime:
            datum = dateutil.parser.parse(datum)
        elif pytype == bool:
            datum = th.coerce_to_specific(datum)
        else:
            datum = pytype(unicode(datum))
        if isinstance(datum, datetime.datetime):
            if dialect in self._datetime_format:
                return datum.strftime(self._datetime_format[dialect])
            else:
                return "'%s'" % datum
        elif hasattr(datum, 'lower'):
            return "'%s'" % datum.replace("'", "''")
        else:
            return datum
        
    _insert_template = u"INSERT INTO {table_name} ({cols}) VALUES ({vals});" 
    def inserts(self, dialect=None):
        dialect = self._dialect(dialect)
        for row in self.data:
            cols = ", ".join(c for c in row)
            vals = ", ".join(unicode(self._prep_datum(val, dialect, key)) for (key, val) in row.items())
            yield self._insert_template.format(table_name=self.table_name, cols=cols, vals=vals)
        #TODO: distinguish between inserting blank strings and inserting NULLs
            
    def sql(self, dialect=None, inserts=False):
        """
        Combined results of ``.ddl(dialect)`` and, if ``inserts==True``, ``.inserts(dialect)``.
        """
        result = [self.ddl(dialect), ]
        if inserts:
            for row in self.inserts(dialect):
                result.append(row)
        return '\n'.join(result)
        
    def __str__(self):
        if self.default_dialect:
            return self.ddl()
        else:
            return self.__repr__()
        
    types2sa = {datetime.datetime: sa.DateTime, int: sa.Integer, 
                float: sa.Numeric, bool: sa.Boolean}
   
    _illegal_in_column_name = re.compile(r'[^a-zA-Z0-9_$#]') 
    def _clean_column_name(self, col_name):
        """
        Replaces illegal characters in column names with ``_``
        
        Also lowercases all identifiers.  If you want mixed-case table
        or column names, you are a bad person and you should feel bad.
        """
        result = self._illegal_in_column_name.sub("_", col_name)
        if result[0].isdigit():
            result = '_%s' % result
        return result.lower()
    
    def _determine_types(self, varying_length_text=False, uniques=False):
        self.columns = OrderedDict()
        self.satypes = OrderedDict() 
        self.pytypes = {}
        self.is_unique = {}
        self.is_nullable = {}
        self.comments = {}
        rowcount = 0
        for row in self.data:
            rowcount += 1
            keys = row.keys()
            if not isinstance(row, OrderedDict):
                keys = sorted(keys)
            for k in keys:
                v = row[k]
                if not th.is_scalar(v):
                    v = unicode(v)
                    self.comments[k] = 'nested values! example:\n%s' % pprint.pformat(v)
                    logging.warn('in %s: %s' % (k, self.comments[k]))
                k = self._clean_column_name(k)
                if k not in self.columns:
                    self.columns[k] = []
                self.columns[k].append(v)
        for col in self.columns:
            sample_datum = th.best_coercable(self.columns[col])
            self.pytypes[col] = type(sample_datum)
            if isinstance(sample_datum, Decimal):
                self.satypes[col] = sa.DECIMAL(*th.precision_and_scale(sample_datum))
            elif isinstance(sample_datum, basestring):
                if varying_length_text:
                    self.satypes[col] = sa.Text()
                else:
                    self.satypes[col] = sa.String(len(sample_datum))
            else:
                self.satypes[col] = self.types2sa[type(sample_datum)]
            self.is_unique[col] = uniques and (len(set(self.columns[col])) == len(self.columns[col]))
            self.is_nullable[col] = (len(self.columns[col]) < rowcount 
                                      or None in self.columns[col])
        
    
if __name__ == '__main__':
    doctest.testmod(optionflags=doctest.NORMALIZE_WHITESPACE)    