import csv

from spinta.dispatcher import command
from spinta.components import Context
from spinta.types.dataset import Model, Property
from spinta.fetcher import fetch


@command()
def read_csv():
    pass


@read_csv.register()
def read_csv(context: Context, model: Model, *, source=None, dependency=None):
    source = source.format(**dependency)
    with fetch(context, source, text=True).open() as f:
        yield from csv.DictReader(f)


@read_csv.register()
def read_csv(context: Context, model: Property, *, source=None, value=None):
    return value.get(source)
