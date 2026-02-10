from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Optional

import dask.dataframe as dd

from spinta.adapters.loaders import DataAdapter, DataAdapterError
from spinta.datasets.backends.dataframe.backends.xml.model import ModelHeader
from spinta.utils.schema import NA

from .row import RowList


@dataclass
class DaskXml(DataAdapter):
    df: dd.DataFrame
    df_mask: Optional[Dict[str, Dict[str, Any]]] = None

    def _resolve_dataframe_row(self, row_list: RowList, properties: list[str], row_properties: Mapping[str | tuple | None, object]) -> Mapping[str, object]:
        columns = {}
        for row in row_list.rows:
            if not isinstance(row.type, ModelHeader) and row.property in properties:
                if row_properties.get(row.source) is not NA:
                    columns[row.path] = row_properties[row.source]

        return columns

    def _compute_df_mask(self, partition_df: dd.DataFrame) -> Optional[dd.Series]:
        if not self.df_mask:
            return None
        for col, rule in self.df_mask.items():
            if not isinstance(rule, dict):
                rule = {"op": "eq", "value": rule}

            op = rule["op"]
            val = rule["value"]
            mask = True
            if op == "eq":
                mask &= partition_df[col] == val
            elif op == "ne":
                mask &= partition_df[col] != val
            elif op == "lt":
                mask &= partition_df[col] < val
            elif op == "le":
                mask &= partition_df[col] <= val
            elif op == "gt":
                mask &= partition_df[col] > val
            elif op == "ge":
                mask &= partition_df[col] >= val
            elif op == "startswith":
                mask &= partition_df[col].str.startswith(val, na=False)
            elif op == "contains":
                mask &= partition_df[col].str.contains(val, case=False, na=False)
            else:
                raise DataAdapterError(f"Unsupported filter op '{op}' for column '{col}'.")
        return mask

    def load(
        self,
        row_list: RowList,
    ) -> Iterable[Mapping[str, object]]:
        columns = [row.source for row in row_list.rows if row.source and not isinstance(row.type, ModelHeader)]
        properties = [row.property for row in row_list.rows if row.property and not isinstance(row.type, ModelHeader)]
        unique_columns = list(set(columns))
        for partition in self.df.to_delayed():
            partition_df = dd.from_delayed(partition)
            partition_df = partition_df[unique_columns]
            mask = self._compute_df_mask(partition_df)
            if mask is not None:
                partition_df = partition_df[mask]
            for _, df_row in partition_df.iterrows():
                try:
                    yield self._resolve_dataframe_row(row_list, properties, df_row)
                except Exception as e:
                    raise DataAdapterError(f"Error processing row: {e}") from e
