__author__ = 'Nicholas McCready'

# %s in the correct order:
# - column_types
# - table_name
# - sets ~ key = $1 ... i-n = $i-$n value
# - key_name =  key/index name
#   - indexes (column names)
#   - values
_mergeString = """
    CREATE FUNCTION merge({column_types}) RETURNS VOID AS
    $$
    BEGIN
        LOOP
            -- first try to update the key where $1 is key
            UPDATE {table_name} SET {sets} WHERE {key_name} = $1;
            IF found THEN
                RETURN;
            END IF;
            -- not there, so try to insert the key
            -- if someone else inserts the same key concurrently,
            -- we could get a unique-key failure
            BEGIN
                INSERT INTO {table_name}({indexes}) VALUES ({values});
                RETURN;
            EXCEPTION WHEN unique_violation THEN
                -- do nothing, and loop to try the UPDATE again
            END;
        END LOOP;
    END;
    $$
    LANGUAGE plpgsql;
    """


# tempTableName
#
_bulk_upsert = """
LOCK TABLE {perm_table} IN EXCLUSIVE MODE;

UPDATE {perm_table}
SET {sets}
FROM {temp_table}
WHERE {perm_table}.{key}; = {temp_table}.{key}

INSERT INTO {perm_table}
SELECT {perm_table}.{key}; = {temp_table}.{key}
FROM {temp_table}
LEFT OUTER JOIN {perm_table} ON ({perm_table}.{key}= {temp_table}.{key})
WHERE {perm_table}.{key} IS NULL;

COMMIT;
"""


def date(tablename, colname, dateformat):
    return """
        alter table {table} alter column {col} TYPE DATE
        using
        CASE
          WHEN {col} is not NULL and {col}::int <> 0
          then
            to_date({col}::text,'{dateformat}')
        ELSE
          NULL
        END;""".format(table=tablename, col=colname, dateformat=dateformat)


def bulk_update(tablename, tbl, primary_key):
    sets = _make_set(tbl, primary_key)
    return _bulk_upsert.format(perm_table=tablename,
                               temp_table="temp_" + tablename, sets=sets)


def _make_set(tablename, tbl, primary_key):
    tablename = "temp_" + tablename
    str = ""
    for key in tbl:
        if key != primary_key:
            str = "{col} = {temp_table}.{col}".format(col=key, temp_table=tablename) + "," + str

    return str[:-1]


def merge(table_name, table_dict):
    #TODO get the values and keys out of the table_dict to fill out the _mergeString
    return
