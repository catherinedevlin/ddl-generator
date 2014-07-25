#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_ddlgenerator
----------------------------------

Tests for `ddlgenerator` module.
"""

import glob
import unittest
import pymongo
import os.path
from collections import namedtuple, OrderedDict
try:
    from ddlgenerator.ddlgenerator import Table
except ImportError:
    from ddlgenerator import Table

def here(filename):
    return os.path.join(os.path.dirname(__file__), filename)

class TestMongo(unittest.TestCase):
    
    def setUp(self):
        data = [{'year': 2013,
                 'physics': ['François Englert', 'Peter W. Higgs'],
                 'chemistry': ['Martin Karplus', 'Michael Levitt', 'Arieh Warshel'],
                 'peace': ['Organisation for the Prohibition of Chemical Weapons (OPCW)',],
                 },
                {'year': 2011,
                 'physics': ['Saul Perlmutter', 'Brian P. Schmidt', 'Adam G. Riess'],
                 'chemistry': ['Dan Shechtman',],
                 'peace': ['Ellen Johnson Sirleaf', 'Leymah Gbowee', 'Tawakkol Karman'],
                 },
                ]
        self.data = data
        self.conn = pymongo.Connection()
        self.db = self.conn.ddlgenerator_test_db
        self.tbl = self.db.prize_winners
        self.tbl.insert(self.data)
        
    def tearDown(self):
        self.conn.drop_database(self.db)
        
    def testData(self):
        winners = Table(self.tbl, pk_name='year')
        generated = winners.sql('postgresql', inserts=True)
        self.assertIn('REFERENCES prize_winners (year)', generated)
        
        
class TestFiles(unittest.TestCase):
    
    def test_use_open_file(self):
        with open(here('knights.yaml')) as infile:
            knights = Table(infile)
            generated = knights.sql('postgresql', inserts=True)
            self.assertIn('Lancelot', generated)
            
    def test_nested(self):
        merovingians = [
                        OrderedDict([('name', {'name_id': 1, 'name_txt': 'Clovis I'}), 
                                     ('reign', {'from': 486, 'to': 511}),
                                     ]),
                        OrderedDict([('name', {'name_id': 1, 'name_txt': 'Childebert I'}), 
                                     ('reign', {'from': 511, 'to': 558}),
                                     ]),
                        ]
        tbl = Table(merovingians)
        generated = tbl.sql('postgresql', inserts=True).strip()
        
    def test_pydata_named_tuples(self):
        prov_type = namedtuple('province', ['name', 'capital', 'pop'])
        canada = [prov_type('Quebec', 'Quebec City', '7903001'),
                  prov_type('Ontario', 'Toronto', '12851821'), ]
        tbl = Table(canada)
        generated = tbl.sql('postgresql', inserts=True).strip()
        self.assertIn('capital VARCHAR(11) NOT NULL,', generated)
        self.assertIn('(name, capital, pop) VALUES (\'Quebec\', \'Quebec City\', 7903001)', generated)
        
    def test_files(self):
        for sql_fname in glob.glob(here('*.sql')):
            with open(sql_fname) as infile:
                expected = infile.read().strip()
            (fname, ext) = os.path.splitext(sql_fname)
            for source_fname in glob.glob(here('%s.*' % fname)):
                (fname, ext) = os.path.splitext(source_fname)
                if ext != '.sql':
                    tbl = Table(source_fname, uniques=True)
                    generated = tbl.sql('postgresql', inserts=True, drops=True).strip()
                    self.assertEqual(generated, expected)
            
        
if __name__ == '__main__':
    unittest.main()
