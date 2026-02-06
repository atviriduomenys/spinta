from dataclasses import dataclass
from typing import Iterable

from pandas import NA

from spinta.core.enums import Level
from spinta.core.ufuncs import asttoexpr
from spinta.datasets.backends.dataframe.backends.xml.adapter.spinta import SpintaManifestRef
from spinta.datasets.backends.dataframe.backends.xml.domain.data_adapter import DataAdapter
from spinta.datasets.backends.dataframe.backends.xml.domain.model import Manifest, ManifestHeader, ManifestRef
from spinta.datasets.keymaps.components import KeyMap


@dataclass
class MetaXml(DataAdapter):
    key_map: KeyMap

    def load(self, manifest: Manifest, data: dict[str, object]) -> Iterable[tuple[str, None | object]]:
        """Load and resolve model metadata based on the manifest."""

        for row in manifest.rows:
            if row.type == ManifestHeader:
                val = None
                for _row in manifest.rows:
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
            if row.type == ManifestRef or isinstance(row.type, SpintaManifestRef):
                val = data[row.path]
                object_id = self.key_map.encode(row.ref, val)
                if callable(row.value):
                    where_query = {
                        'name': 'eq',
                        'args': [{ 'name': 'bind', 'args': ['_id'] }, object_id]
                    }

                    query = asttoexpr(where_query)
                    val = row.value(query)
                    if row.maturity == Level.open:
                        val = dict(val).get(row.ref)
                    else:
                        val = dict(val).get('_id')
                yield ".".join(map(str, row.path)), val

