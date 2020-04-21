import uuid

import sqlalchemy as sa

from spinta import commands
from spinta.core.ufuncs import Expr
from spinta.core.config import RawConfig
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.datasets.components import Entity
from spinta.datasets.utils import iterparams
from spinta.datasets.backends.sql.query import SqlQueryBuilder
from spinta.datasets.backends.sql.components import Sql


@commands.load.register(Context, Sql, RawConfig)
def load(context: Context, backend: Sql, rc: RawConfig):
    dsn = rc.get('backends', backend.name, 'dsn', required=True)
    backend.engine = sa.create_engine(dsn, echo=False)
    backend.schema = sa.MetaData(backend.engine)


@commands.wait.register(Context, Sql)
def wait(context: Context, backend: Sql):
    return True


@commands.prepare.register(Context, Sql, Manifest)
def prepare(context: Context, backend: Sql, manifest: Manifest):
    backend.schema.reflect()


@commands.bootstrap.register(Context, Sql)
def bootstrap(context: Context, backend: Sql):
    pass


@commands.getall.register(Context, Entity, Sql)
def getall(
    context: Context,
    entity: Entity,
    backend: Sql,
    *,
    query: Expr = None,
):
    conn = context.get(f'transaction.{backend.name}')
    builder = SqlQueryBuilder(context)
    builder.update(model=entity.model)
    props = {
        p.external.name: p.name
        for p in entity.model.properties.values()
        if p.external
    }
    for params in iterparams(entity.model):
        table = entity.name.format(**params)
        table = backend.schema.tables[table]

        env = builder.init(table)
        expr = env.resolve(query)
        where = env.execute(expr)
        qry = env.build(where)

        for row in conn.execute(qry):
            row = {
                props[k]: v
                for k, v in row.items()
                if k in props
            }
            row['_type'] = entity.model.model_type()
            row['_id'] = str(uuid.uuid4())
            yield commands.cast_backend_to_python(context, entity.model, backend, row)
