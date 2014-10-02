
from csv2psql import reservedwords, mangle, sqlgen
import unittest
from should_dsl import should, should_not

class Csv2psqlSpec(unittest.TestCase):
    def test_psql_reserved_words_exists(self):
        reservedwords.psql_reserved_words | should_not | be_empty

    def test_magle_exists(self):
        mangle.mangle | should_not | equal_to(None)
        mangle.mangle_table | should_not | equal_to(None)


class SqlGenSpec(unittest.TestCase):
    def test_date(self):
        sqlgen.date("db","col1","YYYY") | should | equal_to("""
        alter table db alter column col1 TYPE DATE
        using
        CASE
          WHEN col1 is not NULL and col1::int <> 0
          then
            to_date(col1::text,'YYYY')
        ELSE
          NULL
        END;""")

    def test_make_set(self):
        sqlgen._make_set("table",{"one":1,"two":2},"primary") | should | equal_to(
            "one = temp_table.one,two = temp_table.two")