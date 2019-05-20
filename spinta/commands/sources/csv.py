import csv
import urllib.parse

from spinta.components import Context
from spinta.types.dataset import Model
from spinta.fetcher import fetch
from spinta.commands import pull
from spinta.commands.sources import Source


class Csv(Source):
    pass


@pull.register()
def pull(context: Context, source: Csv, node: Model, *, name: str):
    base = node.parent.source.name
    url = urllib.parse.urljoin(base, name) if base else name
    with fetch(context, url, text=True).open() as f:
        yield from csv.DictReader(f)
