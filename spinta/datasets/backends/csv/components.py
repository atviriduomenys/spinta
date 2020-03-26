import csv
import urllib.parse

from spinta import commands
from spinta.external import ExternalBackend
from spinta.components import Context, Model
from spinta.core.ufuncs import Expr
from spinta.core.datasets import iterparams


class Csv(ExternalBackend):
    pass


@commands.getall.register()
async def load(context: Context, model: Model, backend: Csv):
    base = model.external.resource.source
    url = model.external.source
    model.external.source = urllib.parse.urljoin(base, url) if base else url


@commands.getall.register()
async def getall(
    context: Context,
    model: Model,
    backend: Csv,
    *,
    query: Expr,
):
    for params in iterparams(model):
        url = model.external.url.format(**params)
        with backend.io.open(url, text=True) as f:
            reader = csv.DictReader(f)
            for row in reader:
                row = {
                    '_type': model.model_type(),
                    **row,
                }
                yield commands.to_native(context, model, backend, row)
