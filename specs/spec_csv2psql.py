
from csv2psql import reservedwords, mangle
import unittest
from should_dsl import should, should_not

class Csv2psqlSpec(unittest.TestCase):
    def test_psql_reserved_words_exists(self):
        reservedwords.psql_reserved_words | should_not | be_empty

    def test_magle_exists(self):
        mangle.mangle | should_not | equal_to(None)
        mangle.mangle_table | should_not | equal_to(None)

