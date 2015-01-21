from csv2psql import psql_copy
import unittest
from should_dsl import should, should_not
from textwrap import dedent


class PsqlCopy(unittest.TestCase):
    def test_string_as_int_throws(self):
        Exception |should| be_thrown_by(lambda: psql_copy.psqlencode("SECONDARY ACREAGE",int))

    def test_string_as_int_throws_ValueError(self):
        ValueError |should| be_thrown_by(lambda: psql_copy.psqlencode("SECONDARY ACREAGE",int))