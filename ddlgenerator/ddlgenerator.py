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
from collections import OrderedDict
import copy
import datetime
from decimal import Decimal
import doctest
import logging
import os.path
import pprint
import re
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
    import typehelpers as th  # TODO: can py2/3 split this
    import reshape

logging.basicConfig(filename='ddlgenerator.log', filemode='w')
metadata = sa.MetaData()


class KeyAlreadyExists(KeyError):
    pass


dialect_names = '''drizzle firebird mssql mysql oracle postgresql
                   sqlite sybase sqlalchemy django'''.split()


def _dump(sql, *multiparams, **params):
    pass

mock_engines = {}
for engine_name in ('postgresql', 'sqlite', 'mysql', 'oracle', 'mssql'):
    mock_engines[engine_name] = sa.create_engine('%s://' % engine_name,
                                                 strategy='mock',
                                                 executor=_dump)                                                              

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
    <BLANKLINE>
    CREATE TABLE knights (
    	name VARCHAR(8) NOT NULL,
    	kg DECIMAL(3, 1) NOT NULL,
    	dob TIMESTAMP WITHOUT TIME ZONE,
    	UNIQUE (name),
    	UNIQUE (dob)
    );
    """

    table_index = 0

    def _find_table_name(self, data):
        if not self.table_name:
            if pymongo and isinstance(data, pymongo.collection.Collection):
                self.table_name = data.name
            elif hasattr(data, 'lower'):  # duck-type string test
                if os.path.isfile(data):
                    (file_path, file_extension) = os.path.splitext(data)
                    self.table_name = os.path.split(file_path)[1].lower()
        self.table_name = self.table_name or \
                          'generated_table%s' % Table.table_index
        self.table_name = reshape.clean_key_name(self.table_name)
        Table.table_index += 1

    def __init__(self, data, table_name=None, default_dialect=None,
                 save_metadata_to=None, metadata_source=None,
                 varying_length_text=False, uniques=False,
                 pk_name=None, force_pk=False,
                 _parent_table=None, _fk_field_name=None, reorder=False,
                 loglevel=logging.WARN, limit=None):
        """
        Initialize a Table and load its data.

        If ``varying_length_text`` is ``True``,
        text columns will be TEXT rather than VARCHAR.
        This *improves* performance in PostgreSQL.

        If a ``metadata<timestamp>`` YAML file generated
        from a previous ddlgenerator run is
        provided, *only* ``INSERT`` statements will be produced,
        and the table structure
        determined during the previous run will be assumed.
        """
        logging.getLogger().setLevel(loglevel)
        self.varying_length_text = varying_length_text
        self.table_name = table_name
        self._find_table_name(data)
        # Send anything but Python data objects to
        # data_dispenser.sources.Source
        if hasattr(data, 'lower') or hasattr(data, 'read'):
            self.data = Source(data, limit=limit)
        else:
            try:
                self.data = iter(data)
            except TypeError:
                self.data = Source(data)
            
        if (    self.table_name.startswith('generated_table')
            and hasattr(self.data, 'table_name')):
            self.table_name = self.data.table_name
        self.table_name = self.table_name.lower()

        self.data = reshape.walk_and_clean(self.data)

        (self.data, self.pk_name, children, child_fk_names
            ) = reshape.unnest_children(data=self.data,
                                        parent_name=self.table_name,
                                        pk_name=pk_name,
                                        force_pk=force_pk)

        self.default_dialect = default_dialect
        self.comments = {}
        child_metadata_sources = {}
        if metadata_source:
            if isinstance(metadata_source, OrderedDict):
                logging.info('Column metadata passed in as OrderedDict')
                self.columns = metadata_source
            else:
                logging.info('Pulling column metadata from file %s'
                             % metadata_source)
                with open(metadata_source) as infile:
                    self.columns = yaml.load(infile.read())
            for (col_name, col) in self.columns.items():
                if isinstance(col, OrderedDict):
                    child_metadata_sources[col_name] = col
                    self.columns.pop(col_name)
                else:
                    self._fill_metadata_from_sample(col)
        else:
            self._determine_types()

        if reorder:
            ordered_columns = OrderedDict()
            if pk_name and pk_name in self.columns:
                ordered_columns[pk_name] = self.columns.pop(pk_name)
            for (c, v) in sorted(self.columns.items()):
                ordered_columns[c] = v
            self.columns = ordered_columns

        if _parent_table:
            fk = sa.ForeignKey('%s.%s' % (_parent_table.table_name,
                                          _parent_table.pk_name))
        else:
            fk = None

        self.table = sa.Table(self.table_name, metadata,
                              *[sa.Column(cname, col['satype'],
                                          fk if fk and (_fk_field_name == cname)
                                             else None,
                                          primary_key=(cname == self.pk_name),
                                          unique=(uniques and col['is_unique']),
                                          nullable=col['is_nullable'],
                                          doc=self.comments.get(cname))
                                for (cname, col) in self.columns.items()
                                if True
                                ])
        
        self.children = {child_name: Table(child_data, table_name=child_name,
                                           default_dialect=self.default_dialect,
                                           varying_length_text=varying_length_text,
                                           uniques=uniques, pk_name=pk_name,
                                           force_pk=force_pk,
                                           _parent_table=self, reorder=reorder,
                                           _fk_field_name=child_fk_names[child_name],
                                           metadata_source=child_metadata_sources.get(child_name),
                                           loglevel=loglevel)
                         for (child_name, child_data) in children.items()}

        if save_metadata_to:
            if not save_metadata_to.endswith(('.yml', 'yaml')):
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
            result.append(child.ddl(dialect=dialect, creates=creates,
                          drops=drops))
        return '\n\n'.join(result)

    table_backref_remover = re.compile(r',\s+table\s*\=\<.*?\>')
    capitalized_words = re.compile(r"\b[A-Z]\w+")
    sqlalchemy_setup_template = textwrap.dedent("""
        from sqlalchemy import create_engine, %s
        engine = create_engine(r'sqlite:///:memory:')
        metadata = MetaData(bind=engine)
                        
        %s = %s
        
        metadata.create_all()""" )
    def sqlalchemy(self, is_top=True):
        """Dumps Python code to set up the table's SQLAlchemy model"""
        table_def = self.table_backref_remover.sub('', self.table.__repr__())
        table_def = table_def.replace("MetaData(bind=None)", "metadata")
        table_def = table_def.replace("Column(", "\n  Column(")
        table_def = table_def.replace("schema=", "\n  schema=")
        result = [table_def, ]
        result.extend(c.sqlalchemy(is_top=False) for c in self.children)
        result = "\n".join(result)
        if is_top:
            sqla_imports = set(self.capitalized_words.findall(table_def))
            sqla_imports &= set(dir(sa))
            sqla_imports = sorted(sqla_imports)
            result = self.sqlalchemy_setup_template % (
                ", ".join(sqla_imports), self.table_name, result)
            result = textwrap.dedent(result)
        return result

    def django_models(self, metadata_source=None):
        sql = self.sql(dialect='postgresql', inserts=False, creates=True,
            drops=True, metadata_source=metadata_source)
        u = sql.split(';\n')

        try:
            import django
        except ImportError:
            print('Cannot find Django on the current path. Is it installed?')
            django = None

        if django:
            from django.conf import settings
            from django.core import management
            from django import setup

            import sqlite3
            import os

            db_filename = 'generated_db.db'

            conn = sqlite3.connect(db_filename)
            c = conn.cursor()
            for i in u:
                c.execute(i)

            if not settings.configured:
                settings.configure(
                    DEBUG='on',
                    SECRET_KEY='1234',
                    ALLOWED_HOSTS='localhost',
                    DATABASES = {'default' : {'NAME':db_filename,'ENGINE':'django.db.backends.sqlite3'}},
                    )
                django.setup()
            management.call_command('inspectdb', interactive=False)
            os.remove(db_filename)
        
    _datetime_format = {}  # TODO: test the various RDBMS for power to read the standard
    def _prep_datum(self, datum, dialect, col):
        if datum is None or not str(datum).strip():
            return 'NULL'
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
            # simple SQL injection protection, sort of... ?
            return "'%s'" % datum.replace("'", "''")
        else:
            return datum

    _insert_template = "INSERT INTO {table_name} ({cols}) VALUES ({vals});"

    def inserts(self, dialect=None):        
        if dialect and dialect.startswith("sqla"):
            yield "conn = engine.connect()"
            yield "inserter = %s.insert()" % self.table_name
            for row in self.data:
                yield "conn.execute(inserter, **{row})".format(row=str(dict(row)))
            yield "conn.connection.commit()"
        else:        
            dialect = self._dialect(dialect)
            for row in self.data:
                cols = ", ".join(c for c in row)
                vals = ", ".join(str(self._prep_datum(val, dialect, key))
                                 for (key, val) in row.items())
                yield self._insert_template.format(table_name=self.table_name,
                                                   cols=cols, vals=vals)
            for child in self.children.values():
                for row in child.inserts(dialect):
                    yield row

    def sql(self, dialect=None, inserts=False, creates=True,
            drops=True, metadata_source=None):
        """
        Combined results of ``.ddl(dialect)`` and, if ``inserts==True``,
        ``.inserts(dialect)``.
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
                str_len = max(len(col['sample_datum']), col['str_length'])
                col['satype'] = sa.Unicode(len(col['sample_datum']))
        else:
            col['satype'] = self.types2sa[type(col['sample_datum'])]
            if col['satype'] == sa.Integer and (
                col['sample_datum'] > 2147483647 or col['sample_datum'] < -2147483647):
                col['satype'] = sa.BigInteger
        return col

    types2sa = {datetime.datetime: sa.DateTime, int: sa.Integer,
                float: sa.Numeric, bool: sa.Boolean,
                type(None): sa.Text}

    def _determine_types(self):
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
                v_raw = row[k]
                if not th.is_scalar(v_raw):
                    v = str(v_raw)
                    self.comments[k] = 'nested values! example:\n%s' % \
                                       pprint.pformat(v)
                    logging.warning('in %s: %s' % (k, self.comments[k]))
                v = th.coerce_to_specific(v_raw)
                if k not in self.columns:
                    self.columns[k] = {'sample_datum': v,
                                       'str_length': len(str(v_raw)),
                                       'is_nullable': not (rowcount == 1 and
                                                           v is not None and
                                                           str(v).strip()
                                                           ),
                                       'is_unique': set([v, ])}
                else:
                    col = self.columns[k]
                    col['str_length'] = max(col['str_length'], len(str(v_raw)))
                    old_sample_datum = col.get('sample_datum')
                    col['sample_datum'] = th.best_representative(
                        col['sample_datum'], v)
                    if (v is None) or (not str(v).strip()):
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
