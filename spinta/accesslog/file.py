from typing import Any
from typing import Dict
from typing import TextIO, Union

import json
import sys
import pathlib

from starlette.requests import Request

from spinta import commands
from spinta.accesslog import AccessLog
from spinta.components import Context, Config, Store


class FileAccessLog(AccessLog):
    file: Union[TextIO, pathlib.Path] = None

    _file: TextIO = None
    _closed: bool = True

    def __exit__(self, *exc):
        if not self._closed:
            self._file.close()

    def log(self, message: Dict[str, Any]) -> None:
        if self._file is None:
            return
        message = json.dumps(message)
        print(message, file=self._file)


@commands.load.register(Context, FileAccessLog, Config)
def load(context: Context, accesslog: FileAccessLog, config: Config):
    commands.load[Context, AccessLog, Config](context, accesslog, config)

    file = config.rc.get('accesslog', 'file')

    if file == 'stdout':
        file = sys.stdout
    elif file == 'stderr':
        file = sys.stderr
    elif file in ('null', '/dev/null'):
        file = None
    else:
        file = pathlib.Path(file)

        # Try to open file to check if it is writable.
        with file.open('a'):
            pass

    accesslog.file = file
    return accesslog


def _prepare_file(
    accesslog: AccessLog,
    file: Union[TextIO, pathlib.Path, None],
) -> None:
    if isinstance(file, pathlib.Path):
        file = file.open('a')
        accesslog._closed = False
    else:
        accesslog._closed = True
    accesslog._file = file


@commands.load.register(Context, FileAccessLog, Store)
def load(context: Context, accesslog: FileAccessLog, store: Store):
    if isinstance(store.accesslog, FileAccessLog):
        _prepare_file(accesslog, store.accesslog.file)


@commands.load.register(Context, FileAccessLog, Request)
def load(context: Context, accesslog: FileAccessLog, request: Request):
    commands.load[Context, AccessLog, Request](context, accesslog, request)
    store: Store = context.get('store')
    if isinstance(store.accesslog, FileAccessLog):
        _prepare_file(accesslog, store.accesslog.file)
