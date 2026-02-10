from dataclasses import dataclass
from typing import Iterable

from pandas import NA

from spinta.adapters.loaders import DataAdapter
from spinta.core.enums import Level
from spinta.core.ufuncs import asttoexpr
from spinta.datasets.backends.dataframe.backends.xml.components import RowList
from spinta.datasets.backends.dataframe.backends.xml.model import ModelHeader, ModelRef
from spinta.datasets.keymaps.components import KeyMap


@dataclass
class RowMetaItem(DataAdapter):
    key_map: KeyMap

    def load(self, model: RowList, data: dict[str | tuple, object]) -> Iterable[tuple[str, None | object]]:
        """Load and resolve model metadata based on the manifest."""

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

                    query = asttoexpr(where_query)
                    val = row.value(query)
                    if row.maturity == Level.open:
                        val = dict(val).get(row.ref)
                    else:
                        val = dict(val).get('_id')
                yield ".".join(map(str, row.path)), val
