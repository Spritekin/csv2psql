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

# Capture Stdio within a block
#
# http://stackoverflow.com/questions/5136611/capture-stdout-from-a-script-in-python
import contextlib
@contextlib.contextmanager
def capture():
    import sys
    from cStringIO import StringIO
    oldout,olderr = sys.stdout, sys.stderr
    try:
        out=[StringIO(), StringIO()]
        sys.stdout,sys.stderr = out
        yield out
    finally:
        sys.stdout,sys.stderr = oldout, olderr
        out[0] = out[0].getvalue()
        out[1] = out[1].getvalue()


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

    def test_chain_has_sql_to_postgres_not_none(self):
        (logic.chain("SQL", mock_to_postgres).to_postgres is not None) | should | be(True)

    def test_chain_has_sql_pipe_not_none(self):
        (logic.chain("SQL", mock_to_postgres).pipe is not None) | should | be(True)

    def test_chain_has_sql_to_postgres(self):
        logic.chain("SQL", mock_to_postgres).to_postgres("dumb url") | should | be("SQL")

    def test_chain_has_sql_pipe(self):
        with capture() as out:
            logic.chain("SQL", mock_to_postgres).pipe()
            out[0].getvalue() | should | equal_to("SQL\n")