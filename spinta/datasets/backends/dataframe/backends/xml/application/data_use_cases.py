"""Application use cases for manifest-driven data loading."""

from __future__ import annotations

from typing import Callable, Iterable, TypeVar

from spinta.datasets.backends.dataframe.backends.xml.domain.data_adapter import DataAdapter
from spinta.datasets.backends.dataframe.backends.xml.domain.adapter import ManifestAdapter
from spinta.datasets.backends.dataframe.backends.xml.domain.model import Manifest
from spinta.datasets.backends.dataframe.backends.xml.domain.services import SelectorError

T = TypeVar('T')

def stream_model_data(
    model: object,
    manifest_adapter: ManifestAdapter,
    data_adapter: DataAdapter,
    metadata_adapter: DataAdapter,
    model_adapter: Callable[[Manifest, dict[str, object]], T],
) -> Iterable[T]:
    """Stream data rows using a model-based manifest """

    manifest = manifest_adapter.from_model(model)

    def _lazy_stream() -> Iterable[T]:
        try:
            for row in data_adapter.load(manifest):
                yield model_adapter(manifest=manifest, data=row, metadata_loader=metadata_adapter) # type: ignore
            return
        except Exception as e:
            raise e
        
    return _lazy_stream()


__all__ = [
    "stream_model_data",
]
