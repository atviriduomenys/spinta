from typing import Iterator

import dask
from zeep.helpers import serialize_object

from spinta import commands
from spinta.components import Context, Property, Model
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.dataframe.backends.soap.components import Soap
from spinta.datasets.backends.dataframe.commands.read import (
    parametrize_bases,
    get_dask_dataframe_meta,
    dask_get_all
)
from spinta.dimensions.param.components import ResolvedParams
from spinta.typing import ObjectData


def _get_data_soap(url: str, backend: Soap) -> list[dict]:
    return serialize_object(backend.soap_operation(), target_cls=dict)


@commands.getall.register(Context, Model, Soap)
def getall(
    context: Context,
    model: Model,
    backend: Soap,
    *,
    query: Expr = None,
    resolved_params: ResolvedParams = None,
    extra_properties: dict[str, Property] = None,
    **kwargs
) -> Iterator[ObjectData]:
    bases = parametrize_bases(
        context,
        model,
        model.external.resource,
        resolved_params
    )
    bases = list(bases)
    builder = backend.query_builder_class(context)
    builder.update(model=model)

    meta = get_dask_dataframe_meta(model)
    df = dask.bag.from_sequence(bases).map(
        _get_data_soap, backend=backend
    ).flatten().to_dataframe(meta=meta)

    yield from dask_get_all(context, query, df, backend, model, builder, extra_properties)
