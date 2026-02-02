from dataclasses import dataclass
from typing import Iterable, List, Mapping, Optional, Tuple

import dask.dataframe as dd

from spinta.components import Context
from spinta.core.enums import Access
from spinta.datasets.backends.dataframe.backends.xml.adapter.spinta import ManifestHeader, Spinta
from spinta.datasets.backends.dataframe.backends.xml.domain import DataAdapter, DataAdapterError
from spinta.datasets.backends.dataframe.backends.xml.domain.model import Manifest
from spinta.types.datatype import String

@dataclass
class DaskXml(DataAdapter):
    df: dd.DataFrame
    context: Context

    def _resolve_dataframe_row(self, manifest: Manifest, properties: list[str], row_properties: Mapping[str, object]) -> Mapping[str, object]:
        columns = {}
        for row in manifest.rows:
            if row.property in properties:
                if row_properties.get(row.source) is not None:
                    columns[row.path] = row_properties[row.source]

        return columns

    def load(self, 
             manifest: Manifest, 
        ) -> Iterable[Mapping[str, object]]:
        # columns = [row.source for row in manifest.rows if row.source and row.access == Access.open]
        columns = [row.source for row in manifest.rows if row.source and not row.type == ManifestHeader]
        properties = [row.property for row in manifest.rows if row.property and not row.type == ManifestHeader]
        unique_columns = list(set(columns))
        for partition in self.df.to_delayed():
            partition_df = dd.from_delayed(partition)
            for _, df_row in partition_df[unique_columns].iterrows():
                try:
                    yield self._resolve_dataframe_row(manifest, properties, df_row)
                except KeyError as e:
                    raise DataAdapterError(f"Column for property '{df_row.property}' not found in DataFrame.") from e
                except Exception as e:
                    raise DataAdapterError(f"Error processing row: {e}") from e