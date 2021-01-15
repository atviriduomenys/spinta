try:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations
except ImportError:
    pass

from spinta import commands
from spinta.core.ufuncs import asttoexpr, ufunc
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.backends.postgresql.components import PostgreSQL


@commands.migrate.register(Context, Manifest, PostgreSQL)
def migrate(context: Context, manifest: Manifest, backend: PostgreSQL):
    config = context.get('config')
    resolvers = ufunc.resolver.ufuncs()
    executors = ufunc.executor.ufuncs()

    Env = config.components['migrations']
    conn = context.get(f'transaction.{backend.name}')
    ctx = MigrationContext.configure(conn)
    op = Operations(ctx)
    scope = {
        'op': op,
        'backend': backend
    }

    async def execute(versions):
        for version in versions:
            for action in version['actions']:
                ast = action['upgrade']
                env = Env(context, resolvers, executors, scope)
                expr = asttoexpr(ast)
                expr = env.resolve(expr)
                env.execute(expr)
            yield version

    return execute
