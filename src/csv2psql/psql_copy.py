import logger
import sys
from shutil import copyfile
from mangle import *
import re

reg_matcher = re.compile('^.*"((.*"){2})*.*$')


class PsqlCopyData:
    def __init__(self, copy_statement, data):
        self.copy_statement = copy_statement
        self.data = data

    def to_psql(self):
        return "\\%s%s\\.\n" % (self.copy_statement, self.data)


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
        # if odd quote is first char
        if v[0] == '"':
            raise Exception("Bad terminated string!")
    s = ''
    for c in str(v):
        if ord(c) < ord(' '):
            s += '\\x%02x' % (ord(c))
        else:
            s += c
    return s


def validify_date_len(dates, k, _tbl):
    if not dates:
        return
    for date_format, cols in dates.iteritems():
        if not k in cols:
            val = _tbl[k].to_s
            if len(date_format) != len(str(val)):
                raise Exception("Date format length does not match format: % for value %" % (date_format, val))


def _make_data(dict_reader, _tbl, tablename, dates, exit_on_error=False):
    data = ''
    index = 0
    for row in dict_reader:
        index += 1
        outrow = []
        for k in dict_reader.fieldnames:
            assert k in row
            try:
                _k = mangle(k)
                if _k in _tbl and 'type' in _tbl[_k]:
                    dt = _tbl[_k]['type']
                else:
                    dt = str
                validify_date_len(dates, k, _tbl)
                outrow.append(_psqlencode(row[k], dt))
            except ValueError as e:
                _handle_error(e, k, _k, row, index, dt, tablename, exit_on_error)
            except Exception as e:
                _handle_error(e, k, _k, row, index, dt, tablename, exit_on_error)
        data += "\t".join(outrow)
        data += "\n"

    return data


def out_as_copy_stdin(fields, tablename, delimiter, _tbl, dates, exit_on_error=False):
    """
    :param fields:
    :param tablename:
    :param delimiter: not used but could be if we were just using the csv
    :param _tbl: hashmap holding datatypes and values to be checked for integrity
    :param exit_on_error:  If a row fails to pass a data type if this is true the import is aborted. Else we skip the row.
    :return: None

    Purpose is to ensure data integrity by checking original csv data against the intended type for a col/row.

    Expects Piped Data to be Piped to psql
    """
    # for k, v in [('fields', fields), ('tablename', tablename), ('delimiter', delimiter), ('_tbl', _tbl)]:
    # logger.info(True, "out_as_copy_stdin: %s: %s" % (k, v))


    nullStr = "NULL AS ''"
    copy_statement = "COPY %s FROM stdin %s\n" % (tablename, nullStr)
    data = _make_data(fields, _tbl, tablename, exit_on_error)
    return PsqlCopyData(copy_statement, data)


def out_as_copy_csv(fields, tablename, delimiter, _tbl, csvfilename, dates, exit_on_error=False):
    """
    :param fields:
    :param tablename:
    :param delimiter: not used but could be if we were just using the csv
    :param _tbl: hashmap holding datatypes and values to be checked for integrity
    :param csvfilename: original csv name
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
    copy_statement = "\COPY {tablename} FROM '{csvfilename}' {nullhandle} CSV HEADER DELIMITER '{delimiter}';".format(
        csvfilename=csvfilename, tablename=tablename, nullhandle=nullStr, delimiter=delimiter)

    data = _make_data(fields, _tbl, tablename, exit_on_error)
    return PsqlCopyData(copy_statement, data)


def _handle_error(e, k, _k, row, index, dt, tablename, exit_on_error):
    # details = {"k": k, "_k": _k, "error_type": type(e), "error": e}
    # logger.error(False, '', '', details)
    logger.error(False, "CSV ERROR: skipping line for table: %s" % tablename)
    logger.error(False, "row: %s" % row)
    logger.error(False, "row#: %s, col: %s, type: %s, value: %s" % (index, k, dt, row[k]))

    if exit_on_error:
        logger.critical(True, "exit_on_error for row is true, exiting!")
        sys.exit(1)
