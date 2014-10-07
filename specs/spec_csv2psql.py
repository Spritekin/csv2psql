from csv2psql import reservedwords, mangle, sqlgen
from csv2psql.sqlstrings import *
import unittest
from should_dsl import should, should_not
from textwrap import dedent


class Csv2psqlSpec(unittest.TestCase):
    def test_psql_reserved_words_exists(self):
        reservedwords.psql_reserved_words | should_not | be_empty

    def test_magle_exists(self):
        mangle.mangle | should_not | equal_to(None)
        mangle.mangle_table | should_not | equal_to(None)


class SqlGenSpec(unittest.TestCase):
    def test_date(self):
        sqlgen._date("db", "col1", "YYYY") | should | equal_to(dedent("""
        ALTER TABLE db ALTER COLUMN col1 TYPE DATE
        USING
        CASE
          WHEN col1 IS NOT NULL AND col1::INT <> 0
          THEN
            to_date(col1::TEXT,'YYYY')
        ELSE
          NULL
        END;"""))

    def test_make_set(self):
        sqlgen._make_set(["one", "two"], "primary", "temp", True) | should | equal_to(
            "primary = temp.primary,one = temp.one,two = temp.two")

    def test_join_keys(self):
        sqlgen._join_keys(['one', 'two', 'three']) \
        | should | equal_to("one || '-' || two || '-' || three")

    def test_make_primary_key_w_join(self):
        sqlgen.make_primary_key_w_join("db","new_key", ['one', 'two', 'three']) | should |\
        equal_to(dedent("""
        ALTER TABLE db ADD COLUMN new_key VARCHAR(200);
        UPDATE db SET new_key = (one || '-' || two || '-' || three);

        DELETE FROM db
        WHERE one IS NULL OR two IS NULL OR three IS NULL;

        -- primary
        ALTER TABLE db ALTER COLUMN new_key SET NOT NULL;
        ALTER TABLE db ADD PRIMARY KEY (new_key);
        """))

    def test_merge(self):
        sqlgen.merge(["one", "two"],
                     "table1",
                     "new_key",
                     True,
                     "tempTable") | should | equal_to( dedent(
            """
            BEGIN TRANSACTION;
            LOCK TABLE table1 IN EXCLUSIVE MODE;

            UPDATE table1
            SET new_key = tempTable.new_key,one = tempTable.one,two = tempTable.two
            FROM tempTable
            WHERE table1.new_key = tempTable.new_key;

            INSERT INTO table1
            SELECT DISTINCT tempTable.new_key,tempTable.one,tempTable.two
            FROM tempTable
            LEFT OUTER JOIN table1 ON (table1.new_key= tempTable.new_key)
            WHERE table1.new_key IS NULL;

            END TRANSACTION;
            """
        ))

    def test_pg_dump_str(self):
        sqlgen.pg_dump_str("db","schema","table1","-s") \
        | should | equal_to("pg_dump db --schema schema --table table1 -s")

    def test_verify_dates(self):
        sqlgen.verify_dates("sometable", "YYYY", ['purchased', 'sold']) | should | equal_to(
            dedent("""
                    SELECT COUNT(*) AS YYYY FROM sometable
                    WHERE purchased IS NOT NULL AND sold IS NOT NULL ;"""))


    def test_delete_dupes(self):
        sqlgen.delete_dupes(["one", "two"], "key", "table1", "serial") | should | equal_to(dedent(
            """
            DELETE FROM table1
            WHERE (one, two, serial) IN (
            SELECT one, two, serial

            FROM table1 AS t1, table1 AS t2
            WHERE t1.serial > t2.serial
            AND t1.one = t2.one
            AND t1.two = t2.two
            );
            """
        ))

    def test_count_dupes(self):
        sqlgen.count_dupes(["one","two"],"key","table1","serial") | should | equal_to(dedent(
            """
            SELECT COUNT(*)
            FROM table1 AS t1, table1 AS t2
            WHERE t1.serial > t2.serial
            AND t1.one = t2.one
            AND t1.two = t2.two;"""
        ))