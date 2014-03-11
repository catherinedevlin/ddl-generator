"""
test_ddlgenerator
----------------------------------

Tests for `ddlgenerator` module.
"""

import glob
import unittest
from collections import namedtuple
try:
    from ddlgenerator.ddlgenerator import Table
except ImportError:
    from ddlgenerator import Table

class TestFiles(unittest.TestCase):
    
    def test_pydata_named_tuples(self):
        prov_type = namedtuple('province', ['name', 'capital', 'pop'])
        canada = [prov_type('Quebec', 'Quebec City', '7903001'),
                  prov_type('Ontario', 'Toronto', '12851821'), ]
        tbl = Table(canada)
        generated = tbl.sql('postgresql', inserts=True).strip()
        expected = "DROP TABLE IF EXISTS generated_table;\nCREATE TABLE generated_table (\n\tname VARCHAR(7) NOT NULL, \n\tcapital VARCHAR(11) NOT NULL, \n\tpop INTEGER NOT NULL\n);\n\nINSERT INTO generated_table (name, capital, pop) VALUES ('Quebec', 'Quebec City', 7903001);\nINSERT INTO generated_table (name, capital, pop) VALUES ('Ontario', 'Toronto', 12851821);"
        self.assertEqual(generated, expected)
        
    def test_files(self):
        for sql_fname in glob.glob('*.sql'):
            (fname, ext) = sql_fname.split('.')
            with open(sql_fname) as infile:
                expected = infile.read().strip()
            for source_fname in glob.glob('%s.*' % fname):
                (fname, ext) = source_fname.split('.')
                if ext != 'sql':
                    tbl = Table(source_fname, uniques=True)
                    generated = tbl.sql('postgresql', inserts=True).strip()
                    self.assertEqual(generated, expected)
            
        
if __name__ == '__main__':
    unittest.main()