import sqlalchemy as sa

from spinta.components import Context
from spinta.commands.sources import Source
from spinta.commands import prepare
from spinta.commands import pull
from spinta.types.dataset import Resource, Model


class Sql(Source):
    pass


@prepare.register()
def prepare(context: Context, source: Sql, node: Resource):
    source.engine = sa.create_engine(source.name)
    source.meta = sa.MetaData(source.engine)
    source.meta.reflect(only=[
        source.name
        for model in node.models()
        for source in model.source
        if model.source
    ])


@pull.register()
def pull(context: Context, source: Sql, node: Model, *, params: dict):
    sql = node.parent.source
    name = source.name.format(**params)
    query = sa.select([sql.meta.tables[name]])
    for row in sql.engine.execute(query):
        yield dict(row)
