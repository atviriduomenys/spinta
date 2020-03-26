import json

from spinta import commands
from spinta.external import ExternalBackend
from spinta.components import Context, Model, Source
from spinta.core.ufuncs import Expr
from spinta.core.datasets import iterparams


class Json(ExternalBackend):
    pass


class JsonResource(Source):
    pass


@commands.getall.register()
async def getall(
    context: Context,
    model: Model,
    backend: Json,
    *,
    query: Expr,
):
    for params in iterparams(model):
        url = model.external.resource.source.format(**params)
        with backend.io.open(url, text=True) as f:
            data = json.load(f)
            data = data[model.external.source]
            if not isinstance(data, list):
                data = [data]
            for row in data:
                row = {
                    '_type': model.model_type(),
                    **row,
                }
                yield commands.cast_backend_to_python(context, model, backend, row)
