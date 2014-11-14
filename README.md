[![Dependency Status](https://gemnasium.com/nmccready/csv2psql.svg)](https://gemnasium.com/nmccready/csv2psql)
Master: [![Build Status](https://travis-ci.org/nmccready/csv2psql.png?branch=master)](https://travis-ci.org/nmccready/csv2psql)

To try it out:
```
  % python setup.py install
  % csv2psql --schema=public --key=student_id,class_id example/enrolled.csv > enrolled.sql
  % psql -f enrolled.sql
```

## Options

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

- skipp_stored_proc_modified_time  (defaults to False)

-delete_temp_table Defaults False

environment variables:
CSV2PSQL_SCHEMA      default value for --schema
CSV2PSQL_ROLE        default value for --role
```

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

- skipp_stored_proc_modified_time  (defaults to False)

-delete_temp_table Defaults False

environment variables:
CSV2PSQL_SCHEMA      default value for --schema
CSV2PSQL_ROLE        default value for --role
```
