from dataclasses import dataclass
from typing import Iterable

from .enums import Level
from .model import ModelHeader, ModelRef
from .components import KeyMap

from .schema import NA
from .row import RowList
from .adapters.loaders import DataAdapter


@dataclass
class RowMetaItem(DataAdapter):
    key_map: KeyMap
    asttoexpr: callable

    def load(self, model: RowList, data: dict[str | tuple, object]) -> Iterable[tuple[str, None | object]]:
        """Load and resolve model metadata based on the RowList."""

        for row in model.rows:
            if isinstance(row.type, ModelHeader):
                val = None
                for _row in model.rows:
                    if _row.property == row.ref:
                        val = data[_row.path]
                        break
                if val is None or val is NA:
                    yield '_id', None
                    yield '_type', row.property
                else:
                    object_id = self.key_map.encode(row.property, val)
                    yield '_id', object_id
                    yield '_type', row.property
            if isinstance(row.type, ModelRef):
                val = data[row.path]
                object_id = self.key_map.encode(row.ref, val)
                if callable(row.value):
                    where_query = {
                        'name': 'eq',
                        'args': [{'name': 'bind', 'args': ['_id']}, object_id]
                    }

                    query = self.asttoexpr(where_query)
                    val = row.value(query)
                    if row.maturity == Level.open:
                        val = dict(val).get(row.ref)
                    else:
                        val = dict(val).get('_id')
                yield ".".join(map(str, row.path)), val
