from csv2psql import column
import unittest
from should_dsl import should, should_not
from textwrap import dedent

class ColumnSpec(unittest.TestCase):
    def test_str(self):
        column.Column("crap","SERIAL","junk")\
            .__str__() | should | equal_to(
            '{"type": "SERIAL", "name": "crap", "additional": "junk"}'
        )