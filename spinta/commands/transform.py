from collections.abc import Callable, Iterable
from typing import TypeVar

from spinta import commands
from spinta.adapters.loaders import DataAdapter, ModelAdapter
from spinta.components import Model
from spinta.datasets.backends.dataframe.backends.xml.components.dask_xml import DaskXml
from spinta.datasets.backends.dataframe.backends.xml.components.row import Row
from spinta.datasets.backends.dataframe.backends.xml.components.row_meta_item import RowMetaItem


T = TypeVar('T')


@commands.stream_model_data.register(Model, Row, DaskXml, RowMetaItem)
def stream_model_data(
    model: Model,
    model_adapter: ModelAdapter,
    data_adapter: DataAdapter,
    metadata_adapter: DataAdapter,
    *,
    transformation_adapter: Callable[..., T],
) -> Iterable[T]:
    """Stream model object data using a model-based transformation """

    transformed_model = model_adapter.from_model(model)

    def _lazy_stream() -> Iterable[T]:
        try:
            for object_data in data_adapter.load(transformed_model):
                yield transformation_adapter(model=transformed_model, data=object_data, metadata_loader=metadata_adapter)
            return
        except Exception as e:
            raise e

    return _lazy_stream()
