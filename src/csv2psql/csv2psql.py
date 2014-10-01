#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Converts a CSV file into a PostgreSQL table.

Usage: csv2psql.py [options] ( input.csv | - ) [tablename] | psql

options include:
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

--joinkeys=comma delimited list of keys to join to make a primary key

environment variables:
CSV2PSQL_SCHEMA      default value for --schema
CSV2PSQL_ROLE        default value for --role
'''
__author__ = "Darren Hardy <hardy@nceas.ucsb.edu>"
__version__ = '0.4.2'
__credits__ = "Copyright (c) 2011-2013 NCEAS (UCSB). All rights reserved."

import sys
assert sys.version_info >= (2, 6), "Requires python v2.6 or better"

import os
import os.path
import getopt
import logic
from mangle import *

_verbose = False


def _usage():
    print '''%s\n\nWritten by %s''' % (__doc__, __author__)


_schemas = ['public']

_data_types = ['int4', 'float8', 'str', 'integer', 'float', 'double', 'text', 'bigint', 'int8', 'smallint', 'short']

_dump_types = ['copy', 'sql']


def csv2psql(filename, tablename, **flags):
    ''' Main entry point. Converts CSV `filename` into PostgreSQL `tablename`.
    To detect data types for each field, it reads `flags.maxsniff` rows in
    the CSV file. If `flags.maxsniff` = -1 then it reads the entire CSV file.
    Set `maxsniff` = 0 to disable data type detection.
    '''
    logic.csv2psql(filename, tablename, **flags)


def main(argv=None):
    '''command-line interface'''
    if argv is None:
        argv = sys.argv[1:]
    try:
        # init default flags
        flags = dict()
        flags['maxsniff'] = 1000

        opts, args = \
            getopt.getopt(argv, "ak:s:q", ["help", "version", "schema=", "key=",
                                           "unique=", "cascade", "append", "utf8",
                                           "sniff=", "delimiter=", "datatype=",
                                           "role=", "dumptype=", "joinkeys="])
        for o, a in opts:
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
            elif o in ("--dumptype"):
                if 'is_copy_dump' not in flags:
                    flags['is_copy_dump'] = False

                dump_t = a.lower()
                if dump_t in _dump_types:
                    flags['is_copy_dump'] = True if dump_t == 'copy' else False
                else:
                    raise getopt.GetoptError('unknown dump type %s (use %s)' % (dump_t, _dump_types))
            elif o in ("--joinkeys"):
                flags['joinkeys'] = a.lower().split(',')
            else:
                raise getopt.GetoptError('unknown option %s' % (o))

        if len(args) < 1:
            _usage()
            return -1
        # with a single argument -- we guess the table name
        elif len(args) == 1:
            fn = args[0]
            if fn == '-':
                raise getopt.GetoptError('cannot guess tablename')

            tablename = os.path.splitext(os.path.basename(fn))[0]
            if 'schema' not in flags:
                for s in _schemas:
                    if tablename.startswith(s):
                        flags['schema'] = s
                        break

            print flags;
            csv2psql(fn, mangle_table(tablename), **flags)
            return 0
        elif len(args) == 2:
            fn = args[0]
            tablename = args[1]
            print flags;
            csv2psql(fn, mangle_table(tablename), **flags)
            return 0
        else:
            assert False, 'notreached'
    except getopt.GetoptError, err:
        print >> sys.stderr, 'ERROR:', str(err), "\n\n"
        _usage()
        return -1


if __name__ == '__main__':
    sys.exit(main())
