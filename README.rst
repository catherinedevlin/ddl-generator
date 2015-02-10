=============
DDL Generator
=============

Infers SQL DDL (Data Definition Language) from table data.

Use at command line::

    $ ddlgenerator -i postgresql '[{"Name": "Alfred", "species": "wart hog", "kg": 22}]'

    DROP TABLE generated_table;
    CREATE TABLE generated_table (
	    name VARCHAR(6) NOT NULL, 
	    kg INTEGER NOT NULL, 
	    species VARCHAR(8) NOT NULL 
    )
    ;
    INSERT INTO generated_table (kg, Name, species) VALUES (22, 'Alfred', 'wart hog');
    
Reads data from files::

    $ ddlgenerator postgresql mydata.yaml > mytable.sql

Enables one-line creation of tables with their data

    $ ddlgenerator --inserts postgresql mydata.json | psql 

To use in Python::

    >>> from ddlgenerator.ddlgenerator import Table
    >>> table = Table([{"Name": "Alfred", "species": "wart hog", "kg": 22}])
    >>> sql = table.sql('postgresql', inserts=True)

Supported data formats
----------------------

- Pure Python
- YAML
- JSON
- CSV
- Pickle
- HTML

Features
--------

- Supports all SQL dialects supported by SQLAlchemy
- Coerces data into most specific data type valid on all column's values
- Takes table name from file name
- Guesses format of input data if unspecified by file extension
- with ``-i``/``--inserts`` flag, adds INSERT statements
- with ``-u``/``--uniques`` flag, surmises UNIQUE constraints from data
- Handles nested data, creating child tables as needed
- Reads HTML tables, including those embedded in noisy websites

Options
-------

::

      -h, --help            show this help message and exit
      -k KEY, --key KEY     Field to use as primary key
      -r, --reorder         Reorder fields alphabetically, ``key`` first
      -u, --uniques         Include UNIQUE constraints where data is unique
      -t, --text            Use variable-length TEXT columns instead of VARCHAR
      -d, --drops           Include DROP TABLE statements
      -i, --inserts         Include INSERT statements
      --no-creates          Do not include CREATE TABLE statements
      --save-metadata-to FILENAME
			    Save table definition in FILENAME for later --use-
			    saved-metadata run
      --use-metadata-from FILENAME
			    Use metadata saved in FROM for table definition, do
			    not re-analyze table structure
      -l LOG, --log LOG     log level (CRITICAL, FATAL, ERROR, DEBUG, INFO, WARN)

Generate SQLAlchemy models
--------------------------

Use ``sqlalchemy`` as the model to generate Python for defining SQLAlchemy
models::

    $ ddlgenerator sqlalchemy '[{"Name": "Alfred", "species": "wart hog", "kg": 22}]'

    Table0 = Table('Table0', metadata, 
      Column('species', Unicode(length=8), nullable=False), 
      Column('kg', Integer(), nullable=False), 
      Column('name', Unicode(length=6), nullable=False), 
      schema=None)

Generate Django models
----------------------

If Django is installed on the path then using ``django`` as the model will run the
generated ddl through Django's ``inspectdb`` management command to produce a model
file::

    $ ddlgenerator django '[{"Name": "Alfred", "species": "wart hog", "kg": 22}]'

    # This is an auto-generated Django model module.
    # You'll have to do the following manually to clean this up:
    #   * Rearrange models' order
    #   * Make sure each model has one field with primary_key=True
    #   * Remove `managed = False` lines if you wish to allow Django to create and delete the table
    # Feel free to rename the models, but don't rename db_table values or field names.
    #
    # Also note: You'll have to insert the output of 'django-admin.py sqlcustom [appname]'
    # into your database.
    from __future__ import unicode_literals

    from django.db import models

    class Table0(models.Model):
        species = models.CharField(max_length=8)
        kg = models.IntegerField()
        name = models.CharField(max_length=6)
        class Meta:
            managed = False
            db_table = 'Table0'

Large tables
------------

As of now, ``ddlgenerator`` is not well-designed for table sizes approaching
your system's available memory.

One approach to save time and memory for large tables is to break your input data into multiple
files, then run ``ddlgenerator`` with ``--save-metadata`` against a small 
but representative sample.  Then run with ``--no-creates`` and ``-use-saved-metadata``
to generate INSERTs from the remaining files without needing to re-determine the
column types each time.

Installing
----------

Requires Python3.

From PyPI::

    pip3 install ddlgenerator

From source::

    git clone https://github.com/catherinedevlin/ddl-generator.git
    cd ddl-generator
    pip3 install .

Alternatives
------------

* `csvkit.csvsql <https://csvkit.readthedocs.org/en/0.7.3/scripts/csvsql.html>`_
* `pandas.read_*` methods
* `prequel <https://github.com/timClicks/prequel.git>`_ for SQLite

Credits
-------

- Mike Bayer for sqlalchemy
- coldfix and Mark Ransom for their StackOverflow answers
- Audrey Roy for cookiecutter
- Brandon Lorenz for Django model generation



