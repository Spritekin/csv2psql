import psycopg2
from urlparse import urlparse


def to_postgres(url, sql):
    return ToPostgres(url, sql)


class ToPostgres:
    def __init__(self, url, sql):
        self.url = url
        self.url_obj = urlparse(self.url)
        self.result = self.process_sql(sql)

    def gen_conn(self, async=True):
        dbname = self.url_obj.path
        # dsn=None, database=None, user=None, password=None, host=None,
        # port=None, connection_factory=None, cursor_factory=None, async=False
        return psycopg2.connect(
            database=dbname,
            user=self.url_obj.username,
            password=self.url_obj.password,
            host=self.url_obj.hostname,
            port=self.url_obj.port,
            async=async)

    def with_conn(self, fn, async):
        conn = self.gen_conn(async)
        ret = fn(conn)
        conn.close()
        return ret

    def process_sql(self, sql, async=True):
        def run_sql(conn):
            cur = conn.cursor()
            return cur.execute(sql)

        return self.with_conn(run_sql, async)