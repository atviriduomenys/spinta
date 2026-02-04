from typing import List, Sequence
from spinta import commands
from spinta.components import Context, Model

from spinta.core.ufuncs import asttoexpr
from spinta.datasets.backends.dataframe.backends.xml.domain.adapter import ManifestAdapter
from spinta.datasets.backends.dataframe.backends.xml.domain.model import Manifest, ManifestRow
from spinta.types.datatype import URI, Ref, String
from spinta.types.text.components import Text


class ManifestHeader():
    pass

class ManifestRef():
    pass

class Spinta(ManifestAdapter):
    manifest_paths: List[str]
    context: Context

    def __init__(self, context: Context, manifest_paths: List[str] = None):
        self.context = context
        self.manifest_paths = manifest_paths or []
    
    def _resolve_properties(self, prop_name: str, prop) -> Sequence[ManifestRow]:
        """Resolve properties from a given model.

        Args:
            prop_name (str): The name of the property.
            prop: The property object containing path information.
        Returns:
            Sequence[ManifestRow]: A list of ManifestRow objects representing the resolved properties.
        """

        if prop_name.startswith('_'):
            return []

        rows = [ManifestRow(
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
                row = ManifestRow(
                    path=(path,),
                    property=path,
                    type=str(lang_prop.dtype),
                    ref=prop_name,
                    source=external_name,
                    access=lang_prop.access if hasattr(lang_prop, "access") else None,
                )
                rows.append(row)
        # filter out rows that are not in manifest_paths if manifest_paths is defined
        if isinstance(prop.dtype, Ref):
            ast = {
                'name': 'select',
                'args': [{'name': 'bind', 'args': ['_id'] }]
            }
    
            query = asttoexpr(ast)
            rows = [ManifestRow(
                path=(prop_name,),
                property=prop_name,
                type=ManifestRef, #str(prop.dtype),
                ref=prop.dtype.model.name,
                source=prop.external.name,
                access=prop.access if hasattr(prop, "access") else None,
                value=lambda _id: commands.getone(self.context, prop.dtype.model, prop.dtype.model.backend, id_=_id),  #"""select id"""
            )]
        if isinstance(prop.dtype, URI):
            rows = [ManifestRow(
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

    def _resolve_model_metadata(self, model: Model) -> Sequence[ManifestRow]:
        rows = []
        for pkey in model.external.pkeys:
            rows.append(ManifestRow(
                path=(pkey,),
                property=pkey.model.name,
                type=ManifestHeader,
                ref=pkey.external.prop.name,
                source=pkey.model.external.name,
                access=None,
            ))
        
        return rows

    def from_model(self, model: Model) -> Manifest:
        rows = []           
        for key, value in model.properties.items():
            _rows = self._resolve_properties(key, value)
            rows.extend(_rows)
        
        rows.extend(self._resolve_model_metadata(model))

        return Manifest(rows=rows)