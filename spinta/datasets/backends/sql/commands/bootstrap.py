from spinta import commands
from spinta.components import Context
from spinta.datasets.backends.sql.components import Sql


@commands.bootstrap.register(Context, Sql)
def bootstrap(context: Context, backend: Sql):
    pass
