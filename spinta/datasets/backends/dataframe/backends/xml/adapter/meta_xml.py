from collections.abc import Iterator
from dataclasses import dataclass
from typing import Iterable, Mapping

from spinta.datasets.backends.dataframe.backends.xml.adapter.spinta import ManifestHeader, ManifestRef, Spinta
from spinta.datasets.backends.dataframe.backends.xml.adapter.xml_model import XmlModel
from spinta.datasets.backends.dataframe.backends.xml.domain.data_adapter import DataAdapter
from spinta.datasets.backends.dataframe.backends.xml.domain.model import Manifest
from spinta.datasets.keymaps.components import KeyMap


@dataclass
class MetaXml(DataAdapter):
    key_map: KeyMap

    def load(self, manifest: Manifest, data: dict[str, object]) -> Iterable[Mapping[str, object]]:
        """Load and resolve model metadata based on the manifest."""

        # data
        for row in manifest.rows:
            # keymap.encode(sel.prop.model.model_type(), val
            if row.type == ManifestHeader:
                val = None
                for _row in manifest.rows:
                    if _row.property == row.ref:
                        val = data[_row.path]
                        break
                id = self.key_map.encode(row.property, val)
                yield '_id', id 
                yield '_type', row.property
            if row.type == ManifestRef:
                val = data[row.path]
                id = self.key_map.encode(row.ref, val)
                if callable(row.value):
                    val = row.value(id)
                yield ".".join(map(str, row.path)), val #dict({ '_id': id, '_type': row.ref }) 

                
# jeigu manifesto tipas yra spinta ir manifestrow yra ref
# pnanasu, kad reiks loadint external modeli norint suzonit target value?