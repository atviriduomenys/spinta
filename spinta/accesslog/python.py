from typing import List, Dict, Any

import json

from spinta import commands
from spinta.accesslog import AccessLog
from spinta.components import Context, Store


class PythonAccessLog(AccessLog):
    stream: list

    def log(
        self,
        *,
        txn: str = None,
        method: str = None,
        reason: str = None,
        resources: List[Dict[str, Any]],
        fields: List[str],
    ):
        message = self.create_message(txn, method, reason, resources, fields)
        # `message` must be JSON serializable.
        json.dumps(message)
        self.stream.append(message)


@commands.load.register(Context, PythonAccessLog, Store)
def load(context: Context, accesslog: PythonAccessLog, store: Store):
    if context.has('accesslog.stream'):
        accesslog.stream = context.get('accesslog.stream')
    else:
        accesslog.stream = []
