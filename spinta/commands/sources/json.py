import json

from spinta.components import Context
from spinta.types.dataset import Model
from spinta.fetcher import fetch
from spinta.commands import pull
from spinta.commands.sources import Source


class Json(Source):
    pass


@pull.register()
def pull(context: Context, source: Json, node: Model, *, name: str):
    with fetch(context, node.parent.source.name, text=True).open() as f:
        data = json.load(f)
    data = data[name]
    if isinstance(data, list):
        yield from data
    else:
        yield data
