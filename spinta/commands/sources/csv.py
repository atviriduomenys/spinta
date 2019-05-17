import csv

from spinta.components import Context
from spinta.types.dataset import Model
from spinta.fetcher import fetch
from spinta.commands import pull
from spinta.commands.sources import Source


class SourceMap:
    Dataset = Source
    Model = Source
    Property = Source


@pull.register()
def pull(context: Context, source: Source, node: Model):
    with fetch(context, source.name, text=True).open() as f:
        yield from csv.DictReader(f)
