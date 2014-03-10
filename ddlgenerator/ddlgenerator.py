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
try:
    from io import StringIO 
except ImportError:
    from cStringIO import StringIO
import csv
import datetime
from decimal import Decimal, InvalidOperation
import doctest
import functools
import json
import logging
import math
import os.path
import re
import textwrap
import sqlalchemy as sa
from sqlalchemy.schema import CreateTable
import dateutil.parser
import yaml

logging.basicConfig(filename='ddlgenerator.log', filemode='w')
metadata = sa.MetaData()

dialect_names = 'drizzle firebird mssql mysql oracle postgresql sqlite sybase'.split()

def is_scalar(x):
    return hasattr(x, 'lower') or not hasattr(x, '__iter__')

def precision_and_scale(x):
    """
    From a float, decide what precision and scale are needed to represent it.
   
    >>> precision_and_scale(54.2)
    (3, 1)
    >>> precision_and_scale(9)
    (1, 0)
    
    Thanks to Mark Ransom, 
    http://stackoverflow.com/questions/3018758/determine-precision-and-scale-of-particular-number-in-python
    """
    if isinstance(x, Decimal):
        precision = len(x.as_tuple().digits)
        scale = -1 * x.as_tuple().exponent
        return (precision, scale)
    max_digits = 14
    int_part = int(abs(x))
    magnitude = 1 if int_part == 0 else int(math.log10(int_part)) + 1
    if magnitude >= max_digits:
        return (magnitude, 0)
    frac_part = abs(x) - int_part
    multiplier = 10 ** (max_digits - magnitude)
    frac_digits = multiplier + int(multiplier * frac_part + 0.5)
    while frac_digits % 10 == 0:
        frac_digits /= 10
    scale = int(math.log10(frac_digits))
    return (magnitude + scale, scale)

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

complex_enough_to_be_date = re.compile(r"[\-\. /]")
def coerce_to_specific(datum):
    """
    Coerces datum to the most specific data type possible
    Order of preference: datetime, integer, decimal, float, string
    
    >>> coerce_to_specific(7.2)
    Decimal('7.2')
    >>> coerce_to_specific("Jan 17 2012")
    datetime.datetime(2012, 1, 17, 0, 0)
    >>> coerce_to_specific("something else")
    'something else'
    """
    try:
        if len(complex_enough_to_be_date.findall(datum)) > 1:
            return dateutil.parser.parse(datum)
    except (ValueError, TypeError) as e:
        pass
    try:
        return int(str(datum))
    except ValueError:
        pass
    try: 
        return Decimal(str(datum))
    except InvalidOperation:
        pass
    try:
        return float(str(datum))
    except ValueError:
        pass
    return str(datum)

def _places_b4_and_after_decimal(d):
    """
    >>> _places_b4_and_after_decimal(Decimal('54.212'))
    (2, 3)
    """
    tup = d.as_tuple()
    return (len(tup.digits) + tup.exponent, max(-1*tup.exponent, 0))
    
def worst_decimal(d1, d2):
    """
    Given two Decimals, return a 9-filled decimal representing both enough > 0 digits
    and enough < 0 digits (scale) to accomodate numbers like either.
    
    >>> worst_decimal(Decimal('762.1'), Decimal('-1.983'))
    Decimal('999.999')
    """
    (d1b4, d1after) = _places_b4_and_after_decimal(d1)
    (d2b4, d2after) = _places_b4_and_after_decimal(d2)
    return Decimal('9' * max(d1b4, d2b4) + '.' + '9' * max(d1after, d2after))
    
def best_coercable(data):
    """
    Given an iterable of scalar data, returns the datum representing the most specific
    data type the list overall can be coerced into, preferring datetimes,
    then integers, then decimals, then floats, then strings.  
    
    >>> best_coercable((6, '2', 9))
    6
    >>> best_coercable((Decimal('6.1'), 2, 9))
    Decimal('6.1')
    >>> best_coercable(('2014 jun 7', '2011 may 2'))
    datetime.datetime(2014, 6, 7, 0, 0)
    >>> best_coercable((7, 21.4, 'ruining everything'))
    'ruining everything'
    """
    preference = (datetime.datetime, int, Decimal, float, str)
    worst_pref = 0 
    worst = ''
    for datum in data:
        coerced = coerce_to_specific(datum)
        pref = preference.index(type(coerced))
        if pref > worst_pref:
            worst_pref = pref
            worst = coerced
        elif pref == worst_pref:
            if isinstance(coerced, Decimal):
                worst = worst_decimal(coerced, worst)
                # TODO: how do signs affect precision in various RDBMSs?
            elif isinstance(coerced, float):
                worst = max(coerced, worst)
            else:  # int, str
                if len(str(coerced)) > len(str(worst)):
                    worst = coerced
    return worst
            
    
complex_enough_to_be_date = re.compile(r"[\\\-\. /]")
def sqla_datatype_for(datum):
    """
    Given a scalar Python value, picks an appropriate SQLAlchemy data type.
    
    >>> sqla_datatype_for(7.2)
    DECIMAL(precision=2, scale=1)
    >>> sqla_datatype_for("Jan 17 2012")
    <class 'sqlalchemy.sql.sqltypes.DATETIME'>
    >>> sqla_datatype_for("something else")
    String(length=14)
    """
    try:
        if complex_enough_to_be_date.search(datum):
            result = dateutil.parser.parse(datum)
            return sa.DATETIME
    except (TypeError, ValueError):
        pass
    try:
        (prec, scale) = precision_and_scale(datum)
        return sa.DECIMAL(prec, scale)
    except TypeError:
        return sa.String(len(datum))

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
    )
    ;
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
        TODO: accept XML, pickles; open files
        """
        file_extension = None
        remembered_exception = None
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
        self.table_name = self._clean_column_name(table_name or self.table_name)
        if not hasattr(self.data, 'append'): # not a list
            self.data = [self.data,]
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
        creator = "\n".join(l for l in str(creator).splitlines() if l.strip()) # remove empty lines 
        comments = "\n\n".join(self._comment_wrapper.fill("in %s: %s" % 
                                                        (col, self.comments[col])) 
                                                        for col in self.comments)
        return "%s;\n%s\n%s;" % (self._dropper(dialect), creator, comments)
        # TODO: Accept NamedTuple data source
      
    _datetime_format = {}
    def _prep_datum(self, datum, dialect, col):
        #import ipdb; ipdb.set_trace()
        pytype = self.pytypes[col]
        if pytype == datetime.datetime:
            datum = dateutil.parser.parse(datum)
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
                float: sa.Numeric, }
   
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
                if not is_scalar(v):
                    v = str(v)
                    self.comments[k] = 'nested values! example: %s' % v
                    logging.warn('in %s: %s' % (k, self.comments[k]))
                k = self._clean_column_name(k)
                if k not in self.columns:
                    self.columns[k] = []
                self.columns[k].append(v)
        for col in self.columns:
            sample_datum = best_coercable(self.columns[col])
            self.pytypes[col] = type(sample_datum)
            if isinstance(sample_datum, Decimal):
                self.satypes[col] = sa.DECIMAL(*precision_and_scale(sample_datum))
            elif isinstance(sample_datum, str):
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