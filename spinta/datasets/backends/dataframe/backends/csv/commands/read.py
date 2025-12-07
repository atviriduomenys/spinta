from typing import Iterator

import dask

from spinta import commands
from spinta.components import Context, Property, Model
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.dataframe.backends.csv.components import Csv
from spinta.datasets.backends.dataframe.commands.read import parametrize_bases, dask_get_all
from spinta.datasets.backends.dataframe.ufuncs.components import TabularResource
from spinta.dimensions.param.components import ResolvedParams
from spinta.typing import ObjectData


@commands.getall.register(Context, Model, Csv)
def getall(
    context: Context,
    model: Model,
    backend: Csv,
    *,
    query: Expr = None,
    resolved_params: ResolvedParams = None,
    extra_properties: dict[str, Property] = None,
    **kwargs,
) -> Iterator[ObjectData]:
    resource_builder = TabularResource(context)
    resource_builder.resolve(model.external.resource.prepare)
    bases = parametrize_bases(context, model, model.external.resource, resolved_params, model.external.name)

    builder = backend.query_builder_class(context)
    builder.update(model=model)
    df = dask.dataframe.read_csv(list(bases), sep=resource_builder.seperator)
    yield from dask_get_all(context, query, df, backend, model, builder, extra_properties)
