import sys
from typing import Any
from typing import Dict

from spinta import commands
from spinta.components import Context
from spinta.datasets.backends.sqldump.components import SqlDump


@commands.load.register(Context, SqlDump, dict)
def load(context: Context, backend: SqlDump, config: Dict[str, Any]):
    if config['dsn'] == '-':
        backend.stream = sys.stdin
    else:
        backend.stream = open(config['dsn'])
