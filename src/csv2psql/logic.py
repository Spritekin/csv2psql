import sys
import os
import os.path
import csv
from mangle import *
from reservedwords import *
import sql_alters
import sql_procedures
import sql_triggers
from column import *
import logger

# TODO: write spec
def _psql_identifier(s):
    '''wraps any reserved word with double quote escapes'''
    k = mangle(s)
    if k.lower() in psql_reserved_words:
        return '"%s"' % (k)
    return k


# TODO: write spec
def _isbool(v):
    return str(v).strip().lower() == 'true' or str(v).strip().lower() == 'false'


# TODO: write spec
def _grow_varchar(s):
    '''varchar grows by 80,150,255,1024

    >>> _grow_varchar(None)
    80
    >>> _grow_varchar("hello")
    80
    >>> _grow_varchar("hello hello hello hello hello" * 5)
    255
    >>> _grow_varchar("hello hello hello hello hello" * 10)
    1024
    >>> _grow_varchar("hello hello hello hello hello" * 100)
    2900

    '''
    if s is None:
        return 150  # default size
    l = len(s)
    if l <= 80:
        return 150
    if l <= 255:
        return 255
    if l <= 1024:
        return 1024
    return l


def _psqlencode(v, dt):
    '''encodes using the text mode of PostgreSQL 8.4 "COPY FROM" command

    >>> _psqlencode('hello "there"', str)
    'hello "there"'
    >>> _psqlencode("hello 'there'", str)
    "hello 'there'"
    >>> _psqlencode('True', int)
    '1'
    >>> _psqlencode(100.1, float)
    '100.1'
    >>> _psqlencode(100.1, int)
    '100'
    >>> _psqlencode('', str)
    ''
    >>> _psqlencode(None, int)
    '\\N'
    >>> _psqlencode("	", str)
    '\\x09'

    '''
    if v is None or v == '':
        # if dt == str else '\\N'
        return ''

    if dt == int:
        if str(v).strip().lower() == 'true':
            return '1'
        if str(v).strip().lower() == 'false':
            return '0'
        return str(int(v))
    if dt == float:
        return str(float(v))
    s = ''
    for c in str(v):
        if ord(c) < ord(' '):
            s += '\\x%02x' % (ord(c))
        else:
            s += c
    return s


def _sniffer(f, maxsniff=-1, datatype={}):
    '''sniffs out data types'''
    _tbl = dict()
    logger.info(True, "-- fieldnames: %s" % f.fieldnames)
    logger.info(True, "-- datatype: %s" % datatype)
    # initialize data types
    for k in f.fieldnames:
        _k = mangle(k)
        assert len(_k) > 0
        _tbl[_k] = {'type': str, 'width': _grow_varchar(None)}  # default data type
        if _k in datatype:
            dt = datatype[_k]
            if dt in ['int', 'int4', 'integer']:
                _tbl[_k] = {'type': int, 'width': 4}
            elif dt in ['smallint', 'short']:
                _tbl[_k] = {'type': int, 'width': 2}
            elif dt in ['float', 'double', 'float8']:
                _tbl[_k] = {'type': float, 'width': 8}
            elif dt in ['text', 'str']:
                _tbl[_k] = {'type': str, 'width': -1}
            elif dt in ['int8', 'bigint']:
                _tbl[_k] = {'type': int, 'width': 8}

    _need_sniff = False
    for k in f.fieldnames:
        if mangle(k) not in datatype:
            _need_sniff = True
            break

    # sniff out data types
    if maxsniff <> 0 and _need_sniff:
        i = 0
        for row in f:
            i += 1
            if maxsniff > 0 and i > maxsniff:
                break

            # if _verbose: print >>sys.stderr, 'sniffing row', i, '...', row, _tbl

            # sniff each data field
            for k in f.fieldnames:
                _k = mangle(k)
                assert len(_k) > 0

                v = row[k]
                assert type(v) == str
                if len(v) == 0:
                    continue  # skip empty strings

                if _k in datatype:
                    continue  # skip already typed column

                (dt, dw) = (_tbl[_k]['type'], _tbl[_k]['width'])
                try:
                    if (_isbool(v) or int(v) is not None) and not (dt == float):
                        _tbl[_k] = {'type': int, 'width': 4}
                except ValueError, e:
                    try:
                        if dt == int:  # revert to string
                            _tbl[_k] = {'type': str, 'width': _grow_varchar(v)}
                        if float(v) is not None:
                            _tbl[_k] = {'type': float, 'width': 8}
                    except ValueError, e:
                        if dt == float:
                            _tbl[_k] = {'type': str, 'width': _grow_varchar(v)}
                        if dt == str and dw < len(v):
                            _tbl[_k] = {'type': dt, 'width': _grow_varchar(v)}
    return _tbl


def csv2psql(ifn, tablename,
             fout=sys.stdout,
             analyze_table=True,
             cascade=False,
             create_table=True,
             datatype={},
             default_to_null=True,
             default_user=None,
             delimiter=',',
             force_utf8=False,
             load_data=True,
             maxsniff=-1,
             pkey=None,
             quiet=True,
             schema=None,
             strip_prefix=True,
             truncate_table=False,
             uniquekey=None,
             database_name='',
             is_merge=False,
             joinkeys=None,
             dates=None,
             is_dump=False,
             make_primary_key_first=False,
             serial=None,
             timestamp=None,
             do_add_cols=False):
    # maybe copy?
    orig_tablename = tablename + ""
    skip = is_merge or is_dump

    logger.info(True, "-- skip: %s" % skip)

    if skip:
        tablename = "temp_" + tablename

    if schema is None and not skip:
        schema = os.getenv('CSV2PSQL_SCHEMA', 'public').strip()
        if schema == '':
            schema = None

    if default_user is None and not skip:
        default_user = os.getenv('CSV2PSQL_USER', '').strip()
        if default_user == '':
            default_user = None

    if ifn == '-':
        maxsniff = 0
        create_table = False

    # pass 1
    _tbl = dict()
    # always create temporary table
    logger.info(True, "-- original csv: %s" % ifn)
    f = csv.DictReader(sys.stdin if ifn == '-' else open(ifn, 'rU'), restval='', delimiter=delimiter)
    mangled_field_names = []
    for key in f.fieldnames:
        mangled_field_names.append(mangle(key))
    _tbl = _sniffer(f, maxsniff, datatype)

    logger.info(True, "-- _tbl: %s" % _tbl)

    if default_user is not None and not skip:
        print >> fout, "SET ROLE", default_user, ";\n"

    # add schema as sole one in search path, and snip table name if starts with schema
    if schema is not None and not skip:
        print >> fout, "SET search_path TO %s;" % (schema)
        if strip_prefix and tablename.startswith(schema):
            tablename = tablename[len(schema) + 1:]
            while not tablename[0].isalpha():
                tablename = tablename[1:]

    # add explicit client encoding
    if force_utf8:
        print >> fout, "\\encoding UTF8\n"

    if quiet and not skip:
        print >> fout, "SET client_min_messages TO ERROR;\n"

    if create_table and not skip:
        logger.info(True, "-- CREATING TABLE")
        _create_table(fout, tablename, cascade, _tbl, f, default_to_null, default_user, pkey, uniquekey, serial,
                      timestamp)
        print >> fout, sql_procedures.modified_time_procedure.procedure_str
        # print >> fout, sql_triggers.modified_time_trigger(tablename)

    if truncate_table and not load_data and not skip:
        print >> fout, "TRUNCATE TABLE", tablename, ";"

    # pass 2
    if load_data and not skip:
        _out_as_copy(f, fout, tablename, delimiter, _tbl, ifn)

    if load_data and analyze_table and not skip:
        print >> fout, "ANALYZE", tablename, ";"

    # fix bad dates ints or stings to correct int format
    if dates is not None:
        for date_format, cols in dates.iteritems():
            print >> fout, sql_alters.dates(tablename, cols, date_format)
    if do_add_cols:
        additional_cols(fout, tablename, serial, timestamp, mangled_field_names, is_merge)

    # take cols and merge them into one primary_key
    join_keys_key_name = None
    if joinkeys is not None:
        (keys, key_name) = joinkeys
        join_keys_key_name = key_name
        if serial is not None:
            print >> fout, sql_alters.count_dupes(keys, key_name, tablename, serial, True)
            print >> fout, sql_alters.delete_dupes(keys, key_name, tablename, serial, True)
        print >> fout, sql_alters.make_primary_key_w_join(tablename, key_name, keys)

    primary_key = pkey if pkey is not None else join_keys_key_name
    if is_array(primary_key):
        primary_key = primary_key[0]

    # take temporary table and merge it into a real table
    if primary_key is not None and is_dump:
        if create_table and database_name:
            print >> fout, sql_alters.pg_dump(database_name, schema, tablename)
            # TODO re-order the primary_key to first column

    if is_merge and primary_key is not None:
        logger.info(True, "-- mangled_field_names: %s" % mangled_field_names)
        logger.info(True, "-- make_primary_key_first %s" % make_primary_key_first)

        print >> fout, sql_triggers.modified_time_trigger(orig_tablename)

        print >> fout, sql_alters.merge(mangled_field_names, orig_tablename,
                                        primary_key, make_primary_key_first, tablename)
    return _tbl


def additional_cols(fout, tablename, serial, timestamp, mangled_field_names, is_merge):
    cols_to_add_later = []

    # managed by procedure / function
    mod_time = Column("modified_time", "timestamp".upper(), "default current_timestamp")
    cols_to_add_later.append(mod_time)
    mangled_field_names.append(mod_time.type)

    if serial is not None:
        type_str = "SERIAL".upper()
        cols_to_add_later.append(Column(serial, type_str))
        mangled_field_names.append(type_str)

    if timestamp is not None:
        type_str = "timestamp".upper()
        cols_to_add_later.append(Column(timestamp, type_str, "default current_timestamp"))
        mangled_field_names.append(type_str)

    logger.info(True, "-- cols_to_add_later: %s" % cols_to_add_later)
    if len(cols_to_add_later) > 0 and not is_merge:
        print >> fout, sql_alters.add_cols(cols_to_add_later, tablename)


def is_array(var):
    return isinstance(var, (list, tuple))


def _create_table(fout, tablename, cascade, _tbl, f, default_to_null, default_user, pkey, uniquekey, serial=None,
                  timestamp=None):
    temporary_str = ""

    print >> fout, "DROP TABLE IF EXISTS", tablename, "CASCADE;" if cascade else ";"

    print >> fout, "CREATE %s TABLE" % temporary_str, tablename, "(\n\t",
    cols = list()
    for k in f.fieldnames:
        _k = mangle(k)
        if _k is None or len(_k) < 1:
            continue

        (dt, dw) = (_tbl[_k]['type'], _tbl[_k]['width'])

        if dt == str:
            if dw > 0 and dw <= 1024:
                sqldt = "VARCHAR(%d)" % (dw)
            else:
                sqldt = "TEXT"
        elif dt == int:
            if dw > 4:
                sqldt = "BIGINT"
            else:
                if dw > 2:
                    sqldt = "INTEGER"
                else:
                    sqldt = "SMALLINT"
        elif dt == float:
            if dw > 4:
                sqldt = "DOUBLE PRECISION"
            else:
                sqldt = "REAL"
        else:
            sqldt = "TEXT"  # unlimited length

        if not default_to_null:
            sqldt += " NOT NULL"
        cols.append('%s %s' % (_psql_identifier(_k), sqldt))

    print >> fout, ",\n\t".join(cols)
    print >> fout, ");"
    if default_user is not None:
        print >> fout, "ALTER TABLE", tablename, "OWNER TO", default_user, ";"
    # TODO remove as this is basically duplicated in joinKeys, also pKey looks to never have
    # been flushed out, this is the only part that does anything, the copy part does nothing on pkey
    if pkey is not None:
        print >> fout, "ALTER TABLE", tablename, "ADD PRIMARY KEY (", ','.join(pkey), ");"
    if uniquekey is not None:
        print >> fout, "ALTER TABLE", tablename, "ADD UNIQUE (", ','.join(uniquekey), ");"


def _out_as_copy(fields, fout, tablename, delimiter, _tbl, ifn, exit_on_error=False):
    """
    :param fields:
    :param fout: fileout
    :param tablename:
    :param delimiter: not used but could be if we were just using the csv
    :param _tbl: hashmap holding datatypes and values to be checked for integrity
    :param ifn: original csv name
    :param exit_on_error:  If a row fails to pass a data type if this is true the import is aborted. Else we skip the row.
    :return: None

    Purpose is to ensure data integrity by checking original csv data against the intended type for a col/row.

    Approach:
    Output only valid rows that have the correct types. This is an easier approach from a coding standpoint, however it is
    slower as it require much more file IO when we already have the data in the CSV file itself. (Which PSQL can handle).
    There fore this is basically duplicating an existing CSV within a *.sql file.

    An alternate approach would be to remove the invalid rows from an existing (or copied) csv. The rational here is there
    would likely be fewer errors than successes. Thus lis I/O . Thus the \COPY statement here would point to a *.csv file
    and have the delimiter and Null checks attached.
    """
    print >> fout, "\COPY %s FROM stdin NULL AS ''" % (tablename)
    f = fields
    for row in f:
        # we have to ensure that we're cleanly reading the input data
        outrow = []
        for k in f.fieldnames:
            assert k in row
            try:
                _k = mangle(k)
                if _k in _tbl and 'type' in _tbl[_k]:
                    dt = _tbl[_k]['type']
                else:
                    dt = str
                outrow.append(_psqlencode(row[k], dt))
            except ValueError, e:
                logger.error(True, 'ERROR: %s' % ifn)
                details = {"k": k, "_k": _k, "error_type": type(e), "error": e}

                logger.error(True, '', '', details)
                logger.error(True, "row: %s" % row)
                if exit_on_error:
                    logger.critical(True,"exit_on_error for row is true, exiting!")
                    sys.exit(1)
        print >> fout, "\t".join(outrow)
    print >> fout, "\\."


def _out_as_dump_sql():
    return