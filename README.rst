===============================
DDL Generator
===============================

.. image:: https://badge.fury.io/py/ddlgenerator.png
    :target: http://badge.fury.io/py/ddlgenerator
    
.. image:: https://travis-ci.org/catherinedevlin/ddlgenerator.png?branch=master
        :target: https://travis-ci.org/catherinedevlin/ddlgenerator

.. image:: https://pypip.in/d/ddlgenerator/badge.png
        :target: https://crate.io/packages/ddlgenerator?version=latest


Generates SQL DDL that will accept Python data

Begin with a single Python dictionary (representing a single row)
or a list of dictionaries (representing a full table of data).

::

    >>> from ddlgenerator import ddl
    >>> onerow = {"Name": "Alfred", "species": "wart hog", "kg": 22}
    >>> definition = ddl(onerow)
    >>> print(definition.postgresql)

    DROP TABLE IF EXISTS table1;
    CREATE TABLE table1 (
      name    VARCHAR PRIMARY KEY, 
      species VARCHAR NOT NULL, 
      kg      INTEGER );


Of *course* it makes a ton of guesses and assumptions.  It's your
job to edit the resulting DDL.  We just give you something to get
a quick start with.

If your data is in XML, JSON, CSV, etc., you will find it easy to
convert to Python, then run it through ``ddlgenerator`` from there.

* Free software: MIT license
* Documentation: http://ddlgenerator.rtfd.org.

Features
--------

* TODO
