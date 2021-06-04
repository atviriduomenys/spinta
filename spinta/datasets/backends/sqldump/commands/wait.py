from spinta import commands
from spinta.components import Context
from spinta.datasets.backends.sqldump.components import SqlDump


@commands.wait.register(Context, SqlDump)
def wait(context: Context, backend: SqlDump, *, fail: bool = False) -> bool:
    # SqlDump is not a real backend.
    return True
