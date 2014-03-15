"""
test_ddlgenerator
----------------------------------

Tests for `ddlgenerator` module.
"""

import glob
import unittest
from collections import namedtuple, OrderedDict
try:
    from ddlgenerator.ddlgenerator import Table
except ImportError:
    from ddlgenerator import Table

class TestFiles(unittest.TestCase):
    
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
        for sql_fname in glob.glob('*.sql'):
            (fname, ext) = sql_fname.split('.')
            with open(sql_fname) as infile:
                expected = infile.read().strip()
            for source_fname in glob.glob('%s.*' % fname):
                (fname, ext) = source_fname.split('.')
                if ext != 'sql':
                    tbl = Table(source_fname, uniques=True)
                    generated = tbl.sql('postgresql', inserts=True, drops=True).strip()
                    self.assertEqual(generated, expected)
            
        
if __name__ == '__main__':
    unittest.main()