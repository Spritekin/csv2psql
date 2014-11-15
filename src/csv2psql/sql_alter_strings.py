from textwrap import dedent
# %s in the correct order:
# - column_types
# - table_name
# - sets ~ key = $1 ... i-n = $i-$n value
# - key_name =  key/index name
# - indexes (column names)
# - values

# tempTableName
#
bulk_upsert_str = """
BEGIN TRANSACTION;
LOCK TABLE {perm_table} IN EXCLUSIVE MODE;

UPDATE {perm_table}
SET {sets}
FROM {temp_table}
WHERE {perm_table}.{key} = {temp_table}.{key};

INSERT INTO {perm_table} ({cols})
SELECT DISTINCT {selects}
FROM {temp_table}
LEFT OUTER JOIN {perm_table} ON ({perm_table}.{key}= {temp_table}.{key})
WHERE {perm_table}.{key} IS NULL;

END TRANSACTION;
"""
date_str = """
ALTER TABLE {tablename} ALTER COLUMN {col} TYPE DATE
USING
CASE
  WHEN {col} IS NOT NULL AND {col}::INT <> 0 AND char_length({col}::text) = {format_len}
  THEN
    to_date({col}::TEXT,'{dateformat}')
ELSE
  NULL
END;"""


add_col_str="""
ALTER TABLE {tablename} ADD COLUMN {col} {type} {additional};
"""

join_keys_primary_str = """
ALTER TABLE {tablename} ADD COLUMN {primary_key} VARCHAR(200);
UPDATE {tablename} SET {primary_key} = ({keys_to_join});
{maybe_force_deletion_on_bad_keys}
-- primary
ALTER TABLE {tablename} ALTER COLUMN {primary_key} SET NOT NULL;
ALTER TABLE {tablename} ADD PRIMARY KEY ({primary_key});
"""

bad_key_deletion_str = """
DELETE FROM {tablename}
WHERE {ors_missing_keys}
"""


#THIS METHOD IS SLOW SQL
select_dupes_str ="""
FROM {tablename} AS t1, {tablename} AS t2
WHERE {difference}
{clause}"""

#THIS METHOD IS SLOW SQL
delete_dups_str = """
DELETE FROM {tablename}
WHERE ({cols}) IN (
SELECT {specific_cols}
{select_statement}
);
"""

delete_dups_fast_str = """
CREATE TABLE TMP_TABLE_{tablename} AS
SELECT DISTINCT ON ({cols}) *
FROM {tablename};

SELECT (SELECT COUNT(*) as val1 FROM {tablename}) - (SELECT COUNT(*) AS val2 FROM TMP_TABLE_{tablename}) AS DUPES;

DROP TABLE {tablename};
CREATE TABLE {tablename} AS
SELECT DISTINCT * FROM TMP_TABLE_{tablename};
DROP TABLE TMP_TABLE_{tablename};
"""

count_dups_str = "\nSELECT COUNT(*) AS DUPES" + select_dupes_str + ";"

verify_dates_str = dedent("""
SELECT COUNT(*) AS {date_format} FROM {tablename}
WHERE{not_nulls};""")


def pg_dump_str(db_name, schema_name, table_name, option):
    """
    :param db_name: all name props are self explanatory
    :param schema_name:
    :param table_name:
    :param option: pg_dump options example is -s for schema only
    :return:
    """
    return "pg_dump {db_name} --schema {schema_name} --table {table_name} {option}".format(
        db_name=db_name,
        schema_name=schema_name,
        table_name=table_name,
        option=option
    )