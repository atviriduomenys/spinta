from typing import List, Dict, Any

import json

from spinta import commands
from spinta.accesslog import AccessLog
from spinta.components import Context, Store


class PythonAccessLog(AccessLog):
    stream: list

    def log(self, **kwargs):
        message = self.create_message(**kwargs)
        # `message` must be JSON serializable.
        json.dumps(message)
        self.stream.append(message)


@commands.load.register(Context, PythonAccessLog, Store)
def load(context: Context, accesslog: PythonAccessLog, store: Store):
    if context.has('accesslog.stream'):
        accesslog.stream = context.get('accesslog.stream')
    else:
        accesslog.stream = []
