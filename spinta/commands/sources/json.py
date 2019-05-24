import json

from spinta.components import Context
from spinta.types.dataset import Model
from spinta.fetcher import fetch
from spinta.commands import pull
from spinta.commands.sources import Source


class Json(Source):
    pass


@pull.register()
def pull(context: Context, source: Json, node: Model, *, params: dict):
    url = node.parent.source.name.format(**params)
    with fetch(context, url, text=True).open() as f:
        data = json.load(f)
    data = data[source.name]
    if isinstance(data, list):
        yield from data
    else:
        yield data
