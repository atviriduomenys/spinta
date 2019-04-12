import csv

from spinta.dispatcher import command
from spinta.components import Context, Model, Property


@command()
def read_csv():
    pass


@read_csv.register()
def _(context: Context, model: Model, *, source=None, dependency=None):
    session = context.get('pull.session')
    source = source.format(**dependency)
    with session.get(source, text=True) as f:
        yield from csv.DictReader(f)


@read_csv.register()
def _(context: Context, model: Property, *, source=None, value=None):
    return value.get(source)
