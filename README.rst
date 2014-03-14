=============
DDL Generator
=============

Infers SQL DDL (Data Definition Language) from table data.

Use at command line::

    $ ddlgenerator -i postgresql '{"Name": "Alfred", "species": "wart hog", "kg": 22}'

    DROP TABLE generated_table;
    CREATE TABLE generated_table (
	    name VARCHAR(6) NOT NULL, 
	    kg INTEGER NOT NULL, 
	    species VARCHAR(8) NOT NULL 
    )
    ;
    INSERT INTO generated_table (kg, Name, species) VALUES (22, 'Alfred', 'wart hog');
    
Accepts file paths::

    $ ddlgenerator postgresql mydata.yaml > mytable.sql

Enables one-line creation of tables with their data

    $ ddlgenerator --inserts postgresql mydata.json | psql 

With ``-i``/``--inserts`` flag, insert statements are included.

or use in Python::

    >>> from ddlgenerator.ddlgenerator import Table
    >>> table = Table({"Name": "Alfred", "species": "wart hog", "kg": 22})
    >>> sql = table.sql('postgresql', inserts=True)

* Free software: MIT license

Supported data formats
----------------------

- Pure Python
- YAML
- JSON
- CSV
- Pickle

Features
--------

- Supports all SQL dialects supported by SQLAlchemy
- Coerces data into numeric or date form if possible
- Takes table name from file name
- Guesses format of input data if unspecified
- with ``-u``/``--uniques`` flag, surmises UNIQUE constraints from data
- Chokes demurely on nested data

Installing
----------

::

    git clone git clone https://github.com/catherinedevlin/ddl-generator.git
    cd ddl-generator
    python setup.py install

Credits
-------

- Mike Bayer for sqlalchemy
- coldfix and Mark Ransom for their StackOverflow answers
- Audrey Roy for cookiecutter


