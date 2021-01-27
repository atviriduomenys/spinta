import sys
from pathlib import Path
from typing import Any
from typing import Dict

from spinta import commands
from spinta.components import Context
from spinta.datasets.backends.sqldump.components import SqlDump


@commands.load.register(Context, SqlDump, dict)
def load(context: Context, backend: SqlDump, config: Dict[str, Any]):
    if config['dsn'] == '-':
        backend.stream = sys.stdin
    elif isinstance(config['dsn'], str):
        backend.path = Path(config['dsn'])
    else:
        backend.path = config['dsn'] or None
