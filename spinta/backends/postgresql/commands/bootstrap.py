from spinta import commands
from spinta.components import Context
from spinta.backends.postgresql.components import PostgreSQL

import sqlalchemy as sa


@commands.bootstrap.register(Context, PostgreSQL)
def bootstrap(context: Context, backend: PostgreSQL):
    # XXX: I found, that this some times leaks connection, you can check that by
    #      comparing `backend.engine.pool.checkedin()` before and after this
    #      line.
    # TODO: update appropriate rows in _schema and save `applied` date
    #       of schema migration
    validated_schemas = []
    with backend.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        for table in backend.tables.values():
            schema = table.schema
            if not schema:
                continue

            if schema in validated_schemas:
                continue

            if conn.dialect.has_schema(conn, schema):
                validated_schemas.append(schema)
                continue

            conn.execute(sa.schema.CreateSchema(schema))
            validated_schemas.append(schema)
        backend.schema.create_all(conn, checkfirst=True)
