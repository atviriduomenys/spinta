from dataclasses import dataclass
from typing import Iterable

from pandas import NA

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
                    id = self.key_map.encode(row.property, val)
                    yield '_id', id
                    yield '_type', row.property
            if row.type == ManifestRef:
                val = data[row.path]
                id = self.key_map.encode(row.ref, val)
                if callable(row.value):
                    val = row.value(id)
                yield ".".join(map(str, row.path)), val

