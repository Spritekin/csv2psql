from reservedwords import psql_reserved_words


def mangle_table(s, reserved_words=False):
    '''Mangles an identifer into a psql-safe identifier for a table
with optionally mangling reserved words.

From the PostgreSQL 8.4 manual:

SQL identifiers and key words must begin with a letter (a-z, but also
letters with diacritical marks and non-Latin letters) or an underscore
(_). Subsequent characters in an identifier or key word can be letters,
underscores, digits (0-9), or dollar signs ($). Note that dollar signs are
not allowed in identifiers according to the letter of the SQL standard,
so their use might render applications less portable. The SQL standard
will not define a key word that contains digits or starts or ends with
an underscore, so identifiers of this form are safe against possible
conflict with future extensions of the standard.

The system uses no more than NAMEDATALEN-1 bytes of an identifier; longer
names can be written in commands, but they will be truncated. By default,
NAMEDATALEN is 64 so the maximum identifier length is 63 bytes. If
this limit is problematic, it can be raised by changing the NAMEDATALEN
constant in src/include/pg_config_manual.h.

    >>> mangle_table('MyTable')
    'mytable'
    >>> mangle_table('00table')
    'x00table'
    >>> mangle_table('My Table')
    'my_table'
    >>> mangle_table('My.Table')
    'my.table'
    >>> mangle_table('year')
    'year'
    >>> mangle_table('year', True)
    'year_'
    >>> mangle_table('a table name much longer than 64 characters blah blah blah blah blah blah blah blah')
    'a_table_name_much_longer_than_64_characters_blah_blah_blah_bla'
    >>>

    '''
    assert s is not None and len(s) > 0
    if reserved_words and s in psql_reserved_words:
        s = s + "_"
    m = str()
    if s[0].isdigit():
        m += "x"
    for i in s:
        if i.isspace() or i == '_':
            m += "_"
        elif i == '.':
            m += "."
        elif i.isalnum():
            m += i.lower()
    return m[0:62]


def mangle(s):
    return mangle_table(s).replace('.', '_')