from uuid import UUID

import ujson as json
from decimal import Decimal

from spinta.formats.components import Format


class Json(Format):
    content_type = 'application/json'
    accept_types = {
        'application/json',
    }
    params = {}
    container_name = '_data'

    def __call__(self, data):
        yield f'{{"{self.container_name}":['
        for i, row in enumerate(data):
            sep = ',' if i > 0 else ''
            yield sep + json.dumps(self.data(row), default=encoder, ensure_ascii=False)
        yield ']}'

    def data(self, data: dict) -> dict:
        return data


def encoder(o):
    if isinstance(o, UUID):
        return str(o)
    if isinstance(o, Decimal):
        return float(o)

