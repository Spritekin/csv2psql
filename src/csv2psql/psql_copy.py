import logger
import sys
from shutil import copyfile
from mangle import *
from bad_lines import remove_bad_line_number
import re

reg_matcher = re.compile('^.*"((.*"){2})*.*$')


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

    if v is None or v == '' or v == '\\N':
        return ''

    if dt == int:
        if str(v).strip().lower() == 'true':
            return '1'
        if str(v).strip().lower() == 'false':
            return '0'
        return str(int(v))
    if dt == float:
        return str(float(v))

    # find odd quoted string
    if reg_matcher.match(v):
        #if odd quote is first char
        if v[0] == '"':
            raise Exception("Bad terminated string!")
    s = ''
    for c in str(v):
        if ord(c) < ord(' '):
            s += '\\x%02x' % (ord(c))
        else:
            s += c
    return s


def out_as_copy_stdin(fields, sql, tablename, delimiter, _tbl, exit_on_error=False):
    """
    :param fields:
    :param sql: sql String
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
    """
    nullStr = "NULL AS ''"
    sql += "\COPY {tablename} FROM stdin {nullhandle}".format(tablename=tablename, nullhandle=nullStr)
    f = fields
    index = 0
    for row in f:
        index += 1
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
            except ValueError as e:
                _handle_error(e, k,  _k, row, index, dt, exit_on_error)
            except Exception as e:
                _handle_error(e, k,  _k, row, index, dt, exit_on_error)
        sql += "\t".join(outrow)
    sql += "\\."

    return sql

def out_as_copy_csv(fields, sql, tablename, delimiter, _tbl, csvfilename, exit_on_error=False):
    """
    :param fields:
    :param sql: sql String
    :param tablename:
    :param delimiter: not used but could be if we were just using the csv
    :param _tbl: hashmap holding datatypes and values to be checked for integrity
    :param ifn: original csv name
    :param exit_on_error:  If a row fails to pass a data type if this is true the import is aborted. Else we skip the row.
    :return: None

    Purpose is to ensure data integrity by checking original csv data against the intended type for a col/row.

    Append a COPY psql statement directed at a .csv file to load.
    If any invalid lines (rows) are found, they are removed.

    Remove the invalid rows from an existing (or copied) csv. The rational here is there
    would likely be fewer errors than successes. Thus lis I/O . Thus the \COPY statement here would point to a *.csv file
    and have the delimiter and Null checks attached.
    """
    # backup original file (to look for errors)
    copyfile(csvfilename, "orig_" + csvfilename)
    nullStr = "NULL AS \'\\N\'"
    # HEADER CSV , for csv skip header
    sql += "\COPY {tablename} FROM '{csvfilename}' {nullhandle} CSV HEADER DELIMITER '{delimiter}';".format(
        csvfilename=csvfilename, tablename=tablename, nullhandle=nullStr, delimiter=delimiter)
    f = fields
    index = 0
    for row in f:
        index += 1
        # we have to ensure that we're cleanly reading the input data
        for k in f.fieldnames:
            assert k in row
            try:
                _k = mangle(k)
                if _k in _tbl and 'type' in _tbl[_k]:
                    dt = _tbl[_k]['type']
                else:
                    dt = str
                _psqlencode(row[k], dt)
            except ValueError as e:
                _handle_error(e, k, csvfilename, _k, row, index, dt, exit_on_error)
            except Exception as e:
                _handle_error(e, k, csvfilename, _k, row, index, dt, exit_on_error)
    return sql


def _handle_error(e, k, _k, row, index, dt, exit_on_error):
    pass
    logger.error(False, 'CSV ERROR:')
    details = {"k": k, "_k": _k, "error_type": type(e), "error": e}
    logger.error(False, '', '', details)
    logger.error(False, "row: %s" % row)
    logger.error(False, "row#: {rownum}, col: {col}, type: {type}, value: {value}".format(
        rownum=index, col=k, type=dt, value=row[k]
    ))

    if exit_on_error:
        logger.critical(True, "exit_on_error for row is true, exiting!")
        sys.exit(1)
    remove_bad_line_number(index)