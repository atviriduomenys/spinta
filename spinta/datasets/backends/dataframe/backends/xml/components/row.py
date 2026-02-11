from dataclasses import dataclass
from typing import Callable, List, Sequence

from spinta.adapters.loaders import ModelAdapter, TransformationModel
from spinta.components import Model
from spinta.core.enums import Level
from spinta.datasets.backends.dataframe.backends.xml.model import (
    ModelHeader,
    ModelItem,
    ModelRef
)
from spinta.types.datatype import URI, Ref
from spinta.types.text.components import Text


class RowModelRef(ModelRef):
    maturity: Level

    def __init__(self, maturity: Level):
        self.maturity = maturity


@dataclass
class RowList(TransformationModel):
    """Domain representation of a manifest."""

    rows: Sequence[ModelItem]


class Row(ModelAdapter):
    manifest_paths: List[str]
    ref_resolver: Callable

    def __init__(self, manifest_paths: List[str] = None, ref_resolver: Callable = None):
        self.manifest_paths = manifest_paths or []
        self.ref_resolver = ref_resolver

    def _resolve_properties(self, prop_name: str, prop) -> Sequence[ModelItem]:
        """Resolve properties from a given model.

        Args:
            prop_name (str): The name of the property.
            prop: The property object containing path information.
        Returns:
            Sequence[ModelItem]: A list of ModelItem objects representing the resolved properties.
        """

        if prop_name.startswith('_'):
            return []

        rows = [ModelItem(
            path=(prop_name,),
            property=prop_name,
            type=str(prop.dtype),
            ref=prop_name,
            source=prop.external.name,
            access=prop.access if hasattr(prop, "access") else None,
        )]

        if isinstance(prop.dtype, Text):
            rows = []
            for lang, lang_prop in prop.dtype.langs.items():
                path = f"{prop_name}@{lang}"
                external_name = lang_prop.external.name
                row = ModelItem(
                    path=(path,),
                    property=path,
                    type=str(lang_prop.dtype),
                    ref=prop_name,
                    source=external_name,
                    access=lang_prop.access if hasattr(lang_prop, "access") else None,
                )
                rows.append(row)

        if isinstance(prop.dtype, Ref):
            pkeys = prop.dtype.model.external.pkeys
            rows = []
            for pkey in pkeys:
                row = ModelItem(
                    path=(prop_name,),
                    property=prop_name,
                    type=RowModelRef(maturity=prop.level),
                    ref=pkey.name,
                    source=prop.external.name,
                    access=prop.access if hasattr(prop, "access") else None,
                    maturity=prop.level,
                    value=lambda query, model_dtype=prop.dtype.model, backend=prop.dtype.model.backend: next(
                        self.ref_resolver(query, model_dtype, backend),
                        None,
                    ),
                )
                rows.append(row)
        if isinstance(prop.dtype, URI):
            rows = [ModelItem(
                path=(prop_name,),
                property=prop_name,
                type=str(prop.dtype),
                ref=prop_name,
                source=prop.external.name,
                access=prop.access if hasattr(prop, "access") else None,
            )]

        if self.manifest_paths:
            rows = [row for row in rows if ".".join(row.path) in self.manifest_paths]
        return rows

    def _resolve_model_metadata(self, model: Model) -> Sequence[ModelItem]:
        rows = []
        for pkey in model.external.pkeys:
            rows.append(ModelItem(
                path=(pkey,),
                property=pkey.model.name,
                type=ModelHeader(),
                ref=pkey.external.prop.name,
                source=pkey.model.external.name,
                access=None,
            ))

        return rows

    def from_model(self, model: Model) -> TransformationModel:
        rows = []
        for key, value in model.properties.items():
            _rows = self._resolve_properties(key, value)
            rows.extend(_rows)

        rows.extend(self._resolve_model_metadata(model))

        return RowList(rows=rows)
