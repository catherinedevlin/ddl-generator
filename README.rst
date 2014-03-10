=============
DDL Generator
=============

Infers SQL DDL (Data Definition Language) from table data

Use at command line::

    $ ddlgenerator postgresql mydata.yaml > mytable.sql

or use in Python::

    >>> from ddlgenerator.ddlgenerator import Table
    >>> table = Table({"Name": "Alfred", "species": "wart hog", "kg": 22})
    >>> print(table.ddl('postgresql'))

    DROP TABLE generated_table;
    CREATE TABLE generated_table (
	    name VARCHAR(6) NOT NULL, 
	    kg INTEGER NOT NULL, 
	    species VARCHAR(8) NOT NULL, 
	    UNIQUE (name), 
	    UNIQUE (kg), 
	    UNIQUE (species)
    )
    

Of *course* it makes a ton of guesses and assumptions.  It's your
job to edit the resulting DDL.  We just give you something to get
a quick start with.

* Free software: MIT license

Supported data formats
----------------------

- Pure Python
- YAML
- JSON

Features
--------

- Supports all SQL dialects supported by SQLAlchemy
- Coerces data into numeric or date form if possible

Credits
-------

- Mike Bayer for sqlalchemy
- coldfix and Mark Ransom for their StackOverflow answers


