# Maximum length for PostgreSQL identifiers (e.g. table names, column names,
# function names).
# https://github.com/postgres/postgres/blob/master/src/include/pg_config_manual.h
NAMEDATALEN = 63

UNSUPPORTED_TYPES = [
    'backref',
    'generic',
    'rql',
]
