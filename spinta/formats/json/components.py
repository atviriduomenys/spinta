from decimal import Decimal
from typing import Any
from uuid import UUID

import ujson as json

from spinta.formats.components import Format


class Json(Format):
    content_type = "application/json"
    accept_types = {
        "application/json",
    }
    params = {}
    container_name = "_data"

    def __call__(self, data, *, context_value: Any = None):
        page = None
        # A JSON-LD collection has a top-level `@context`, written before `_data` (10.4). Without it the output is
        # the same as before.
        head = "" if context_value is None else f'"@context":{json.dumps(context_value, ensure_ascii=False)},'
        yield f'{{{head}"{self.container_name}":['
        for i, row in enumerate(data):
            if "_page" in row and row["_page"] is not None:
                page = row["_page"]
            sep = "," if i > 0 else ""
            yield sep + json.dumps(self.data(row), default=encoder, ensure_ascii=False)
        yield "]}" if page is None else "],"
        if page is not None:
            yield f'"_page":{{"next":"{page}"}}}}'

    def data(self, data: dict) -> dict:
        return {k: v for k, v in data.items() if k != "_page"}


def encoder(o):
    if isinstance(o, UUID):
        return str(o)
    if isinstance(o, Decimal):
        return float(o)
