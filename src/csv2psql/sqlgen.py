from textwrap import dedent

__author__ = 'Nicholas McCready'

# %s in the correct order:
# - column_types
# - table_name
# - sets ~ key = $1 ... i-n = $i-$n value
# - key_name =  key/index name
# - indexes (column names)
# - values
_merge_function_str = """
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
_bulk_upsert_str = """
LOCK TABLE {perm_table} IN EXCLUSIVE MODE;

UPDATE {perm_table}
SET {sets}
FROM {temp_table}
WHERE {perm_table}.{key}; = {temp_table}.{key};

INSERT INTO {perm_table}
SELECT {perm_table}.{key}; = {temp_table}.{key}
FROM {temp_table}
LEFT OUTER JOIN {perm_table} ON ({perm_table}.{key}= {temp_table}.{key})
WHERE {perm_table}.{key} IS NULL;

COMMIT;
"""
_date_str = """
ALTER TABLE {tablename} ALTER COLUMN {col} TYPE DATE
USING
CASE
  WHEN {col} IS NOT NULL AND {col}::INT <> 0
  THEN
    to_date({col}::TEXT,'{dateformat}')
ELSE
  NULL
END;"""

_join_keys_primary_str = """
ALTER TABLE {tablename} ADD COLUMN {primary_key} VARCHAR(200);
UPDATE {tablename} SET {primary_key} = ({keys_to_join});

-- primary
ALTER TABLE {tablename} ALTER COLUMN {primary_key} SET NOT NULL;
ALTER TABLE {tablename} ADD PRIMARY KEY ({primary_key})
"""


def _join_keys(keys_to_join):
    joined_str = ""
    i = 0
    length = len(keys_to_join)
    for key in keys_to_join:
        i += 1
        if not i == length:
            joined_str += key + " || '-' || "
        else:
            joined_str += key
    return joined_str


def make_primary_key_w_join(tablename, primary_key_name, keys_to_join):
    return _join_keys_primary_str.format(
        tablename=tablename,
        primary_key=primary_key_name,
        keys_to_join=_join_keys(keys_to_join))


def dates(tablename, cols, dateformat):
    str = ""
    for col in cols:
        str += _date(tablename, col, dateformat) + "\n"
    return str


def _date(tablename, colname, dateformat):
    return _date_str.format(tablename=tablename, col=colname, dateformat=dateformat)


def _make_set(tablename, tbl, primary_key,temp_tablename):
    str = ""
    for key in tbl:
        if key != primary_key:
            str = "{col} = {temp_table}.{col}".format(col=key, temp_table=temp_tablename) + "," + str

    return str[:-1]


def bulk_upsert(tablename, tbl, primary_key, temp_tablename=''):
    if not temp_tablename:
        temp_tablename = "temp_" + tablename
    sets = _make_set(tablename, tbl, primary_key, temp_tablename)
    # print "sets: " + sets
    ret = _bulk_upsert_str.format(perm_table=tablename,
                                  temp_table=temp_tablename,
                                  sets=sets,
                                  key=primary_key)
    # print ret
    return ret


def merge(tablename, tbl, primary_key, temp_tablename):
    return bulk_upsert(tablename, tbl, primary_key, temp_tablename)

