import logging

import sqlalchemy as sa
import sqlalchemy.exc

from spinta import commands
from spinta.core.ufuncs import Expr
from spinta.core.ufuncs import asttoexpr
from spinta.core.config import RawConfig
from spinta.components import Context
from spinta.components import Model
from spinta.manifests.components import Manifest
from spinta.types.datatype import PrimaryKey
from spinta.types.datatype import Ref
from spinta.datasets.utils import iterparams
from spinta.datasets.backends.sql.query import SqlQueryBuilder
from spinta.datasets.backends.sql.components import Sql

log = logging.getLogger(__name__)


@commands.load.register(Context, Sql, RawConfig)
def load(context: Context, backend: Sql, rc: RawConfig):
    dsn = rc.get('backends', backend.name, 'dsn', required=True)
    schema = rc.get('backends', backend.name, 'schema')
    backend.engine = sa.create_engine(dsn, echo=False)
    backend.schema = sa.MetaData(backend.engine, schema=schema)
    backend.dbschema = schema


@commands.wait.register(Context, Sql)
def wait(context: Context, backend: Sql, *, fail: bool = False) -> bool:
    rc = context.get('rc')
    dsn = rc.get('backends', backend.name, 'dsn', required=True)
    engine = sa.create_engine(dsn)
    try:
        conn = engine.connect()
    except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DBAPIError):
        if fail:
            raise
        else:
            return False
    else:
        conn.close()
        engine.dispose()
        return True


@commands.prepare.register(Context, Sql, Manifest)
def prepare(context: Context, backend: Sql, manifest: Manifest):
    # XXX: Moved reflection to spinta/datasets/backends/sql/components:Sql.get_table
    # log.info(f"Reflecting database for {backend.name!r} backend, this might take time...")
    # backend.schema.reflect()
    pass


@commands.bootstrap.register(Context, Sql)
def bootstrap(context: Context, backend: Sql):
    pass


@commands.getall.register(Context, Model, Sql)
def getall(
    context: Context,
    model: Model,
    backend: Sql,
    *,
    query: Expr = None,
):
    conn = context.get(f'transaction.{backend.name}')
    builder = SqlQueryBuilder(context)
    builder.update(model=model)

    if model.external.prepare:
        prepare = asttoexpr(model.external.prepare)
        if query:
            if query.name == 'and' and prepare.name == 'and':
                query.args = query.args + prepare.args
            elif query.name == 'and':
                query.args = query.args + (prepare,)
            elif prepare.name == 'and':
                query = Expr('and', query, *prepare.args)
            else:
                query = Expr('and', query, prepare)
        else:
            query = prepare

    keymap = context.get(f'keymap.{model.keymap.name}')

    for params in iterparams(model):
        table = model.external.name.format(**params)
        table = backend.get_table(model, table)

        env = builder.init(backend, table)
        expr = env.resolve(query)
        where = env.execute(expr)
        qry = env.build(where)

        for row in conn.execute(qry):
            res = {
                '_type': model.model_type(),
            }
            for key, sel in env.select.items():
                if sel.prop:
                    if isinstance(sel.prop.dtype, PrimaryKey):
                        val = [row[k] for k in sel.item]
                        val = keymap.encode(model.model_type(), val)
                    elif isinstance(sel.prop.dtype, Ref):
                        val = [row[k] for k in sel.item]
                        val = keymap.encode(sel.prop.dtype.model.model_type(), val)
                        val = {'_id': val}
                    else:
                        if sel.prep:
                            val = sel.prep
                        else:
                            val = row[sel.item]
                else:
                    val = row[sel.item]
                res[key] = val
            res = commands.cast_backend_to_python(context, model, backend, res)
            yield res
