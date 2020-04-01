from typing import TextIO, List, Dict, Any, Union

import json
import sys
import pathlib

from spinta import commands
from spinta.accesslog import AccessLog
from spinta.components import Context, Config, Store


class FileAccessLog(AccessLog):
    file: Union[TextIO, pathlib.Path]
    close: bool

    def __exit__(self, *exc):
        if self.close:
            self.file.close()

    def log(
        self,
        *,
        txn: str = None,
        method: str = None,
        reason: str = None,
        resources: List[Dict[str, Any]],
        fields: List[str],
    ):
        if self.file is None:
            return
        message = self.create_message(txn, method, reason, resources, fields)
        message = json.dumps(message)
        print(message, file=self.file)


@commands.load.register(Context, FileAccessLog, Config)
def load(context: Context, accesslog: FileAccessLog, config: Config):
    commands.load[Context, AccessLog, Config](context, accesslog, config)

    file = config.rc.get('accesslog', 'file')
    close = False

    if file == 'stdout':
        file = sys.stdout
    elif file == 'stderr':
        file = sys.sterr
    elif file == '/dev/null':
        file = None
    else:
        close = True
        file = pathlib.Path(file)

        # Try to open file to check if it is writable.
        with file.open('a'):
            pass

    accesslog.file = file
    accesslog.close = close
    return accesslog


@commands.load.register(Context, FileAccessLog, Store)
def load(context: Context, accesslog: FileAccessLog, store: Store):  # noqa
    file = store.accesslog.file
    if isinstance(file, pathlib.Path):
        file = file.open('a')
    accesslog.file = file
    accesslog.close = store.accesslog.close
