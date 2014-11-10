from csv2psql import logic, to_postgres
import unittest
from should_dsl import should, should_not


class MockConn:
    def cursor(self):
        print "cursor"
        return self

    def execute(self, sql):
        print "execute w sql: " + sql
        return sql

    def close(self):
        print "close"
        return


class MockToPostgres(to_postgres.ToPostgres):
    def gen_conn(self, async=True):
        print "in gen_con"
        return MockConn()


def mock_to_postgres(url, sql):
    print "in mock_to_postgres, url: " + url + "sql: " + sql
    return MockToPostgres(url, sql)


class ToPostgresSpec(unittest.TestCase):
    # import pydevd
    # pydevd.settrace('localhost', port=9797, stdoutToServer=True, stderrToServer=True, suspend=False)


    def test_chain_has_sql_prop(self):
        logic.chain("SQL", mock_to_postgres).sql | should | equal_to("SQL")

    def test_chain_has_sql_to_postgres(self):
        (logic.chain("SQL", mock_to_postgres).to_postgres is not None) | should | be(True)

    def test_chain_has_sql_to_postgres(self):
        logic.chain("SQL", mock_to_postgres).to_postgres("dumb url") | should | be("SQL")