from spinta import commands
from spinta.components import Context
from spinta.backends.postgresql.components import PostgreSQL


@commands.bootstrap.register(Context, PostgreSQL)
def bootstrap(context: Context, backend: PostgreSQL):
    # XXX: I found, that this some times leaks connection, you can check that by
    #      comparing `backend.engine.pool.checkedin()` before and after this
    #      line.
    # TODO: update appropriate rows in _schema and save `applied` date
    #       of schema migration
    backend.schema.create_all(checkfirst=True)
