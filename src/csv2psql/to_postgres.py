import psycopg2
import logger
import json
from urlparse import urlparse


def to_postgres_copy(url, sql, data_stream):
    return ToPostgres(url, sql).process_copy_sql(data_stream)


def to_postgres(url, sql):
    return ToPostgres(url, sql).process_sql()


class ToPostgres:
    def __init__(self, url, sql):
        self.url = url
        self.url_obj = urlparse(self.url)
        self.sql = sql
        logger.info(True, json.dumps(self.url_obj))

    def gen_conn(self, async=True):
        dbname = self.url_obj.path[1:]  # remove first /
        # dsn=None, database=None, user=None, password=None, host=None,
        # port=None, connection_factory=None, cursor_factory=None, async=False
        return psycopg2.connect(
            database=dbname,
            user=self.url_obj.username,
            password=self.url_obj.password,
            host=self.url_obj.hostname,
            port=self.url_obj.port,
            async=async)

    def with_conn(self, fn, async=False):
        conn = self.gen_conn(async)
        ret = fn(conn)
        conn.commit()
        conn.close()
        return ret

    def process_sql(self, sql=None, async=False):
        if not sql:
            sql = self.sql

        def run_sql(conn):
            cur = conn.cursor()
            return cur.execute(sql)

        return self.with_conn(run_sql, async)

    def process_copy_sql(self, data_stream, sql=None, async=False):
        if not sql:
            sql = self.sql

        copy_statement = sql

        def run_sql(conn):
            cur = conn.cursor()
            return cur.copy_expert(copy_statement, data_stream)

        return self.with_conn(run_sql, async)