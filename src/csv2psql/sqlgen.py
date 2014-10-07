from textwrap import dedent
from os import popen
from sqlstrings import *

__author__ = 'Nicholas McCready'


def verify_dates(table_name, date_format, cols):
    not_nulls_str = " "
    for date in cols:
        not_nulls_str += date + " IS NOT NULL AND "
    # remove last AND
    not_nulls_str = not_nulls_str[:-4]

    return verify_dates_str.format(date_format=date_format,
                                   tablename=table_name,
                                   not_nulls=not_nulls_str)


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
    deletion_str = bad_key_deletion_str.format(
        ors_missing_keys=_make_key_deletion_set(keys_to_join, primary_key_name),
        tablename=tablename)
    return join_keys_primary_str.format(
        tablename=tablename,
        primary_key=primary_key_name,
        keys_to_join=_join_keys(keys_to_join),
        maybe_force_deletion_on_bad_keys=deletion_str)


def dates(tablename, cols, dateformat):
    str = ""
    for col in cols:
        str += _date(tablename, col, dateformat) + "\n"
    return str


def _date(tablename, colname, dateformat):
    return date_str.format(tablename=tablename, col=colname, dateformat=dateformat)


def _make_set(fieldnames, primary_key, temp_tablename, make_primary_first):
    str = ""
    array = _get_fieldnames_w_key(fieldnames, primary_key, make_primary_first)
    for key in array:
        str += "{col} = {temp_table}.{col}".format(col=key, temp_table=temp_tablename) + ","

    return str[:-1]


def _make_key_deletion_set(fieldnames, primary_key):
    str = ""
    array = _get_fieldnames_w_key(fieldnames, primary_key, False)
    for key in array:
        if key != primary_key:
            str += "{col} IS NULL OR ".format(col=key)

    return str[:-4] + ";"


def delete_dupes(fieldnames, primary_key, temp_tablename, serial):
    obj = dupes_clause(fieldnames, primary_key, temp_tablename, serial)
    cols = ""
    obj['filtered_keys'].append(serial)
    # print obj['filtered_keys']
    for key in obj['filtered_keys']:
        cols += "%s, " % key
    cols = cols[:-2]

    this_select_str=select_dupes_str.format(
        tablename=temp_tablename,
        difference=obj['diff'],
        clause=obj['clause']
    )

    return delete_dups_str.format(
        tablename=temp_tablename,
        cols=cols,
        select_statement=this_select_str
    )


def dupes_clause(fieldnames, primary_key, temp_tablename, serial):
    clause = ""
    array = _get_fieldnames_w_key(fieldnames, primary_key, False)
    keys = []
    for key in array:
        if key != primary_key:
            keys.append(key)
            clause += "AND t1.{col} = t2.{col}\n".format(col=key, tablename=temp_tablename)
    clause = clause[:-1]
    diff = "t1.{s} > t2.{s}".format(s=serial)

    return {'clause': clause, 'diff': diff, 'filtered_keys': keys}


def count_dupes(fieldnames, primary_key, temp_tablename, serial):
    obj = dupes_clause(fieldnames, primary_key, temp_tablename, serial)
    return count_dups_str.format(
        tablename=temp_tablename,
        clause=obj['clause'],
        difference=obj['diff']
    )


def _get_fieldnames_w_key(fieldnames, primary_key, make_primary_first):
    # keep fieldnames immutable
    array = fieldnames[:]
    if make_primary_first:
        array.insert(0, primary_key)
    else:
        array.append(primary_key)
    return array


def _make_selects(fieldnames, primary_key, temp_tablename, make_primary_first):
    str = ""
    array = _get_fieldnames_w_key(fieldnames, primary_key, make_primary_first)
    for col in array:
        str += "{temp_table}.{col}".format(col=col, temp_table=temp_tablename) + ","

    return str[:-1]


def bulk_upsert(fieldnames, tablename, primary_key, make_primary_first, temp_tablename=''):
    if not temp_tablename:
        temp_tablename = "temp_" + tablename
    sets = _make_set(fieldnames, primary_key, temp_tablename, make_primary_first)
    selects = _make_selects(fieldnames, primary_key, temp_tablename, make_primary_first)
    # print "sets: " + sets
    ret = bulk_upsert_str.format(perm_table=tablename,
                                 temp_table=temp_tablename,
                                 sets=sets,
                                 key=primary_key,
                                 selects=selects)
    # print ret
    return ret


def merge(fieldnames, tablename, primary_key, make_primary_first, temp_tablename, do_log=False):
    if do_log:
        print "-- tablename: %s" % tablename
        print "-- fieldnames: %s" % fieldnames
        print "-- primary_key: %s" % primary_key
        print "-- temp_tablename: %s" % temp_tablename

    return bulk_upsert(fieldnames, tablename, primary_key, make_primary_first, temp_tablename)


def pg_dump(db_name, schema_name, table_name, option="-s"):
    """
    see pg_dump_str

    This just executes pg_dump with popen and returns the output
    """
    cmd = pg_dump_str(db_name, schema_name, table_name, option)
    return popen(cmd).read()