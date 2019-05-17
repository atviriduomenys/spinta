import sqlalchemy as sa

from spinta.components import Context
from spinta.commands.sources import Source
from spinta.commands import prepare
from spinta.commands import pull
from spinta.types.dataset import Dataset, Model


class Sql(Source):
    pass


@prepare.register()
def prepare(context: Context, source: Sql, node: Dataset):
    source.engine = sa.create_engine(source.name)
    source.schema = sa.MetaData(source.engine)
    source.schema.reflect(only=[model.source.name for model in node.objects.values() if model.source])


@pull.register()
def pull(context: Context, source: Sql, node: Model, *, name: str):
    sql = node.parent.source
    query = sa.select([sql.schema.tables[name]])
    for row in source.dataset.conn.execute(query):
        yield dict(row)
