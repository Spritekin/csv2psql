from csv2psql import reservedwords, mangle, sql_alters, column
from csv2psql.sql_alter_strings import *
import unittest
from should_dsl import should, should_not
from textwrap import dedent


class Csv2psqlSpec(unittest.TestCase):
    def test_psql_reserved_words_exists(self):
        reservedwords.psql_reserved_words | should_not | be_empty

    def test_magle_exists(self):
        mangle.mangle | should_not | equal_to(None)
        mangle.mangle_table | should_not | equal_to(None)


class SqlAlterSpec(unittest.TestCase):
    def test_date(self):
        sql_alters._date("db", "col1", "YYYY") | should | equal_to(dedent("""
        ALTER TABLE db ALTER COLUMN col1 TYPE DATE
        USING
        CASE
          WHEN col1 IS NOT NULL AND col1::INT <> 0 AND char_length(col1::text) = 4
          THEN
            to_date(col1::TEXT,'YYYY')
        ELSE
          NULL
        END;"""))

    def test_make_set(self):
        sql_alters._make_set(["one", "two"], "primary", "temp", True) | should | equal_to(
            "primary = temp.primary,one = temp.one,two = temp.two")

    def test_join_keys(self):
        sql_alters._join_keys(['one', 'two', 'three']) \
        | should | equal_to("one || '_' || two || '_' || three")

    def test_make_primary_key_w_join(self):
        sql_alters.make_primary_key_w_join("db", "new_key", ['one', 'two', 'three']) | should | \
        equal_to(dedent("""
        ALTER TABLE db ADD COLUMN new_key VARCHAR(200);
        UPDATE db SET new_key = (one || '_' || two || '_' || three);

        DELETE FROM db
        WHERE one IS NULL OR two IS NULL OR three IS NULL;

        -- primary
        ALTER TABLE db ALTER COLUMN new_key SET NOT NULL;
        ALTER TABLE db ADD PRIMARY KEY (new_key);
        """))

    def test_merge(self):
        sql_alters.merge(["one", "two"],
                     "table1",
                     "new_key",
                     True,
                     "tempTable") | should | equal_to(dedent(
            """
            BEGIN TRANSACTION;
            LOCK TABLE table1 IN EXCLUSIVE MODE;

            UPDATE table1
            SET new_key = tempTable.new_key,one = tempTable.one,two = tempTable.two
            FROM tempTable
            WHERE table1.new_key = tempTable.new_key;

            INSERT INTO table1 (new_key,one,two)
            SELECT DISTINCT tempTable.new_key,tempTable.one,tempTable.two
            FROM tempTable
            LEFT OUTER JOIN table1 ON (table1.new_key= tempTable.new_key)
            WHERE table1.new_key IS NULL;

            END TRANSACTION;
            """
        ))

    def test_pg_dump_str(self):
        sql_alters.pg_dump_str("db", "schema", "table1", "-s") \
        | should | equal_to("pg_dump db --schema schema --table table1 -s")

    def test_verify_dates(self):
        sql_alters.verify_dates("sometable", "YYYY", ['purchased', 'sold']) | should | equal_to(
            dedent("""
                    SELECT COUNT(*) AS YYYY FROM sometable
                    WHERE purchased IS NOT NULL AND sold IS NOT NULL ;"""))


    def test_delete_dupes(self):
        sql_alters.delete_dupes(["one", "two"], "key", "table1", "serial") | should | equal_to(dedent(
            """
            DELETE FROM table1
            WHERE (one, two, serial) IN (
            SELECT t1.one, t1.two, t1.serial

            FROM table1 AS t1, table1 AS t2
            WHERE t1.serial > t2.serial
            AND t1.one = t2.one
            AND t1.two = t2.two
            );
            """
        ))

    def test_count_dupes(self):
        sql_alters.count_dupes(["one", "two"], "key", "table1", "serial") | should | equal_to(dedent(
            """
            SELECT COUNT(*) AS DUPES
            FROM table1 AS t1, table1 AS t2
            WHERE t1.serial > t2.serial
            AND t1.one = t2.one
            AND t1.two = t2.two;"""
        ))

    def test_add_col(self):
        col = column.Column("crap", "SERIAL", "loto crap")
        sql_alters.add_col(col.name, col.type, "table1", col.additional) \
        | should | equal_to(dedent("""
            ALTER TABLE table1 ADD COLUMN crap SERIAL loto crap;
            """
        ))


    def test_fast_delete_dupes(self):
        sql_alters.fast_delete_dupes(["one", "two"], "key", "public.table1") | should | equal_to(dedent(
            """
            CREATE TABLE TMP_TABLE_table1 AS
            SELECT DISTINCT ON (one, two) *
            FROM table1;

            SELECT (SELECT COUNT(*) as val1 FROM table1) - (SELECT COUNT(*) AS val2 FROM TMP_TABLE_table1) AS DUPES;

            DROP TABLE table1;
            CREATE TABLE table1 AS
            SELECT DISTINCT * FROM TMP_TABLE_table1;
            DROP TABLE TMP_TABLE_table1;
            """
        ))