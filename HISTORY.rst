.. :changelog:

History
-------

0.1.0 (2014-03-22)
++++++++++++++++++

* First release on PyPI.

0.1.2 (2014-07-15)
++++++++++++++++++

* ``data_dispenser`` moved to separate module

0.1.3 (2014-07-16)
++++++++++++++++++

* Bugfix for long integers found after short strings

0.1.4 (2014-07-25)
++++++++++++++++++

* Fixed bug: external ``data_dispenser`` unused by 0.1.3!

0.1.5 (2014-07-25)
++++++++++++++++++

* ``sqlalchemy`` pseudo-dialect added

0.1.6 (2014-07-25)
++++++++++++++++++

* Generate sqlalchemy inserts

0.1.7 (2014-09-14)
++++++++++++++++++

* Read via HTTP
* Support HTML format
* Generate Django models

0.1.7.1 (2014-09-14)
++++++++++++++++++++

* Require data-dispenser 0.2.3

0.1.7.3 (2014-10-19)
++++++++++++++++++++

* Require all formerly recommended dependencies, for simplicity
* Several bugfixes for complex number-like fields

0.1.8 (2015-02-01)
++++++++++++++++++

* UNIQUE contstraints handled properly in sqlalchemy output

0.1.8.2 (2015-02-05)
++++++++++++++++++++

* Cleaner SQLAlchemy generation for fixtures

0.1.9 (2015-02-10)
++++++++++++++++++

* README fixes from Anatoly Technonik, Mikhail Podgurskiy
* Parse args passed to ``generate(args, namespace)`` for non-command-line use