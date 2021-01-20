import uuid
import csv
import urllib.parse
from typing import Dict, Any

from spinta import commands
from spinta.fetcher import fetch
from spinta.core.ufuncs import Expr
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.datasets.components import Entity
from spinta.datasets.utils import iterparams
from spinta.datasets.backends.csv.components import Csv


@commands.load.register(Context, Csv, dict)
def load(context: Context, backend: Csv, config: Dict[str, Any]):
    pass


@commands.prepare.register(Context, Csv, Manifest)
def prepare(context: Context, backend: Csv, manifest: Manifest):
    pass


@commands.bootstrap.register(Context, Csv)
def bootstrap(context: Context, backend: Csv):
    pass


@commands.getall.register(Context, Entity, Csv)
def getall(
    context: Context,
    external: Entity,
    backend: Csv,
    *,
    query: Expr = None,
):
    model = external.model
    base = external.resource.external
    props = {
        p.external.name: p.name
        for p in model.properties.values()
        if p.external
    }
    for params in iterparams(model):
        url = urllib.parse.urljoin(base, external.name) if base else external.name
        url = url.format(**params)
        with fetch(context, url, text=True).open() as f:
            for row in csv.DictReader(f):
                row = {
                    params[k]: v
                    for k, v in row.items()
                    if k in props
                }
                row['_type'] = model.model_type()
                row['_id'] = str(uuid.uuid4())
                yield commands.cast_backend_to_python(context, model, backend, row)
