# !/usr/bin/env python
# -*- coding: utf-8 -*-
'''
```
Converts a CSV file into a PostgreSQL table.

Usage:
    - cat input.csv | csv2psql [options] | psql
    - cat input.csv | csv2psql [--now *options]

options include:
--now           pipe the sql into the postgres driver and push to sql immediately

--postgres_url  url to send data to for postgres

--schema=name   use name as schema, and strip table name if needed

--role=name     use name as role for database transaction

--key=a:b:c     create a primary key using columns named a, b, c.

--unique=a:b:c  create a unique index using columns named a, b, c.

--append        skips table creation and truncation, inserts only

--cascade       drops tables with cascades

--sniff=N       limit field type detection to N rows (default: 1000)

--utf8          force client encoding to UTF8

--datatype=name[,name]:type
                sets the data type for field NAME to TYPE
--dumptype=type use type copy or sql (COPY is PSQL COPY, SQL is PURE INSERT/UPDATES)

--joinkeys= keys[key1,key2]:keyname
                Array of column name delimited by commas : to new key_name

--dates=[keys1,key2]:format
        comma delimited list of keys with a date format

--tablename     tablename to override using the *.csv filename

--databasename  databasename is required upon is_merge

--is_merge indicated to create a table with temp_ in front of the table name

--is_dump  uses pg_dump to possibly get a temp table's
           schema (as long as --key exists && --append is not present).
           Lastly merging sql code is generated to merge a table with its temp_table.

--serial=name add a column that self generates itself an id of type SERIAl

--timestamp=name add a column of timestamp which will give a time when the data was inserted

--primaryfirst=bool defaults to false

--analyze_table=bool

--do_add_cols - indicator to add modified_time, and other cols (timestamp,serial) . To delay till last run

--append_sql Indicates that stdin is reading in text to send straight to post gres

--new_table_name=text Expected to be used with --dump , change old tablename to new_table_name

environment variables:
CSV2PSQL_SCHEMA      default value for --schema
CSV2PSQL_ROLE        default value for --role
```
'''
__author__ = "Nicholas McCready"
__original_author__ = "Darren Hardy <hardy@nceas.ucsb.edu>"
__version__ = '0.4.2'
__credits__ = "Copyright (c) 2011-2013 NCEAS (UCSB). All rights reserved."

import sys

assert sys.version_info >= (2, 6), "Requires python v2.6 or better"

import os
from os import popen, path
import getopt
import logic
from mangle import *

# try to dynamically keep the documentaiton / README up todate w/ one file
dir = path.dirname(__file__)
readme = os.path.join(dir, '../../README.md')
real = os.path.realpath(".")

if os.path.exists(readme):
    sed = "sed -i '/\#\# Options/q' %s" % real + '/README.md'
    # print sed
    popen(sed).read()
    with open(readme, "a") as myfile:
        myfile.write(__doc__)
# end of README sync

_verbose = False


def _usage():
    print '''%s\n\nWritten by %s''' % (__doc__, __author__)


_schemas = ['public']

_data_types = ['int4', 'float8', 'str', 'integer', 'float', 'double', 'text', 'bigint', 'int8', 'smallint', 'short']


def csv2psql(filename, tablename, **flags):
    ''' Main entry point. Converts CSV `filename` into PostgreSQL `tablename`.
    To detect data types for each field, it reads `flags.maxsniff` rows in
    the CSV file. If `flags.maxsniff` = -1 then it reads the entire CSV file.
    Set `maxsniff` = 0 to disable data type detection.
    '''
    logic.csv2psql(filename, tablename, **flags)


def main(argv=None):
    # import pydevd
    # pydevd.settrace('localhost', port=9797, stdoutToServer=True, stderrToServer=True, suspend=False)
    '''command-line interface'''
    tablename = None
    if argv is None:
        argv = sys.argv[1:]
        # print "argv: "
        # print argv
        # print "end argv: "
    try:
        # init default flags
        flags = dict()
        flags['maxsniff'] = 1000

        opts, args = \
            getopt.getopt(argv, "ak:s:q", ["help", "version", "schema=", "key=",
                                           "unique=", "cascade", "append", "utf8",
                                           "sniff=", "delimiter=", "datatype=",
                                           "role=", "is_merge=", "joinkeys=",
                                           "dates=", "tablename=", "databasename=",
                                           "is_dump=", "is_merge=", "primaryfirst=", "serial=",
                                           "timestamp=", "do_add_cols=", "analyze_table=",
                                           "now", "postgres_url=", "append_sql",
                                           "new_table_name="])
        # print "opts: "
        # print opts
        # print "end opts"
        # print

        for o, a in opts:
            # print a
            if o in ("--version"):
                print __version__
                return 0
            elif o in ("--help"):
                _usage()
                return 0
            elif o in ("--cascade"):
                flags['cascade'] = True
            elif o in ("-a", "--append"):
                flags['create_table'] = False
                flags['truncate_table'] = False
                flags['load_data'] = True
                flags['maxsniff'] = 0
            elif o in ("-s", "--schema"):
                flags['schema'] = a
            elif o in ("--role"):
                flags['default_user'] = a
            elif o in ("--sniff"):
                flags['maxsniff'] = int(a)
            elif o in ("-k", "--key"):
                flags['pkey'] = a.split(':')
            elif o in ("--unique"):
                flags['uniquekey'] = a.split(':')
            elif o in ("--utf8"):
                flags['force_utf8'] = True
            elif o in ("--delimiter"):
                flags['delimiter'] = a
            elif o in ("--datatype"):
                if 'datatype' not in flags:
                    flags['datatype'] = dict()
                (k, v) = a.split(':')
                v = v.strip().lower()
                if v in _data_types:
                    for k in [mangle(_k) for _k in k.split(',')]:
                        flags['datatype'][k] = v
                else:
                    raise getopt.GetoptError('unknown data type %s (use %s)' % (v, _data_types))
            elif o in ("-q"):
                _verbose = False
            elif o in ("--is_merge"):
                flags['is_merge'] = True if a.lower() == 'true' else False
            elif o in ("--tablename"):
                tablename = a.lower()
            elif o in ("--joinkeys"):
                ( keys, key_name ) = a.lower().split(':')
                keys = keys.lower().split(',')
                flags['joinkeys'] = (keys, key_name)

            elif o in ("--dates"):
                (dates_commas, date_format) = a.split(':')
                dates = dates_commas.lower().split(',')
                if not flags.has_key('dates'):
                    flags['dates'] = dict()
                flags['dates'][date_format] = dates
            elif o in ("--databasename"):
                flags["database_name"] = a.lower()
            elif o in ("--is_dump"):
                flags["is_dump"] = True if a.lower() == 'true' else False
            elif o in ("--primaryfirst"):
                flags["make_primary_key_first"] = True if a.lower() == 'true' else False
            elif o in ("--serial"):
                flags["serial"] = a.lower()
            elif o in ("--timestamp"):
                flags["timestamp"] = a.lower()
            elif o in ("--do_add_cols"):
                flags["do_add_cols"] = True if a.lower() == 'true' else False
            elif o in ("--analyze_table"):
                flags["analyze_table"] = True if a.lower() == 'true' else False
            elif o in ("--now"):
                flags["result_prints_std_out"] = False  # inverse of now
            elif o in ("--postgres_url"):
                flags['postgres_url'] = a
            elif o in ("--append_sql"):
                flags['append_sql'] = True
            elif o in ("--new_table_name"):
                flags['new_table_name'] = a.lower()
            else:
                raise getopt.GetoptError('unknown option %s' % (o))

        print "-- flags: %s" % flags

        if not tablename:
            assert False, 'tablename is required via --tablename'

        if flags.has_key('postgres_url'):
            if not flags.has_key('result_prints_std_out') or (flags["result_prints_std_out"] and flags['postgres_url']):
                assert False, '--postgres_url required if --now is specified'

        print "-- tablename %s" % tablename
        tablename = mangle_table(tablename)
        print "-- mangled tablename %s" % tablename

        csv2psql(sys.stdin, tablename, **flags)
        return 0

    except getopt.GetoptError, err:
        print >> sys.stderr, 'ERROR:', str(err), "\n\n"
        _usage()
        return -1


if __name__ == '__main__':
    sys.exit(main())
