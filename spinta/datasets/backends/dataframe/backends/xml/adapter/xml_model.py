from typing import Any, Iterator, Optional, cast

from spinta.datasets.backends.dataframe.backends.xml.adapter.spinta import ManifestHeader, ManifestRef, SpintaManifestRef
from spinta.datasets.backends.dataframe.backends.xml.domain.data_adapter import DataAdapter
from spinta.datasets.backends.dataframe.backends.xml.domain.model import DataModel, Manifest

from spinta.typing import ObjectData


class XmlModel(DataModel):
    """XML Model Component"""
    data: dict[str, object]
    manifest: Manifest
    metadata_loader: Optional[DataAdapter]

    def __init__(
            self,
            manifest: Manifest, 
            data: dict[str, object], 
            metadata_loader: Optional[DataAdapter] = None
            ):
        self.manifest = manifest
        self.data = data
        self.metadata_loader = metadata_loader
    
    def __call__(self) -> dict[str, object]:
        return self.data

    def __dict__(self) -> dict:
        return self.data

    def __iter__(self) -> Iterator[tuple[str, str]]:
        """ object data iterator """
        data_keys = []

        if self.metadata_loader:
            """ should it load the metadata per model or per model attribute? """
            for key, value in self.metadata_loader.load(manifest=self.manifest, data=self.data):
                yield key, value
                
        for key in self.data.keys():
            if isinstance(key, (list, tuple)):
                path = ".".join(map(str, key))
            else:
                path = str(key)
            data_keys.append(path)
        for manifest_row in self.manifest.rows:
            if isinstance(manifest_row.path, (list, tuple)):
                path = ".".join(map(str, manifest_row.path))
            else:
                path = str(manifest_row.path)
            if (path in data_keys
                and manifest_row.type != ManifestHeader
                and manifest_row.type != ManifestRef
                and not isinstance(manifest_row.type, SpintaManifestRef)
                and not isinstance(manifest_row.type, ManifestRef)):
                yield path, self.data[manifest_row.path]

    def __getattribute__(self, name: str) -> Any:
        return super().__getattribute__(name)
            
    
    @staticmethod
    def to_object_data(manifest: Manifest, data: dict[str, object], metadata_loader: Optional[DataAdapter] = None) -> ObjectData:
        return cast(ObjectData, XmlModel(manifest, data, metadata_loader))
