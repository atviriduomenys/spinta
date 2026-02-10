from typing import Any, Iterator, Optional, cast

from spinta.datasets.backends.dataframe.backends.xml.adapter.row import RowList
from spinta.datasets.backends.dataframe.backends.xml.adapter.row_meta_item import RowMetaItem
from spinta.datasets.backends.dataframe.backends.xml.domain.model import DataModel, ModelRef

from spinta.typing import ObjectData


class RowFormatter(DataModel):
    """XML Model Component"""
    data: dict[str, object]
    model: RowList
    metadata_loader: Optional[RowMetaItem]

    def __init__(
            self,
            model: RowList,
            data: dict[str, object],
            metadata_loader: Optional[RowMetaItem] = None
    ):
        self.model = model
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
            for key, value in self.metadata_loader.load(model=self.model, data=self.data):
                yield key, value

        for key in self.data.keys():
            if isinstance(key, (list, tuple)):
                path = ".".join(map(str, key))
            else:
                path = str(key)
            data_keys.append(path)
        for manifest_row in self.model.rows:
            if isinstance(manifest_row.path, (list, tuple)):
                path = ".".join(map(str, manifest_row.path))
            else:
                path = str(manifest_row.path)
            if (path in data_keys and not isinstance(manifest_row.type, ModelRef)):
                yield path, self.data[manifest_row.path]

    def __getattribute__(self, name: str) -> Any:
        return super().__getattribute__(name)

    @staticmethod
    def to_object_data(model: RowList, data: dict[str, object], metadata_loader: Optional[RowMetaItem] = None) -> ObjectData:
        return cast(ObjectData, RowFormatter(model, data, metadata_loader))
