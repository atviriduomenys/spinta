import datetime

from alembic.migration import MigrationContext
from alembic.operations import Operations

from spinta import commands
from spinta import spyna
from spinta.core.ufuncs import asttoexpr, ufunc
from spinta.commands.write import write
from spinta.components import Context, Manifest, Action
from spinta.backends.postgresql.components import PostgreSQL


@commands.migrate.register()
def migrate(context: Context, backend: PostgreSQL):  # noqa
    pass


async def _migrate(context: Context, manifest: Manifest, backend: PostgreSQL):
    config = context.get('config')
    resolvers = ufunc.resolver.ufuncs()
    executors = ufunc.executor.ufuncs()

    Env = config.components['migrations']
    conn = backend.engine.connect()
    ctx = MigrationContext.configure(conn)
    op = Operations(ctx)
    scope = {
        'op': op,
    }

    model = manifest.objects['model']['_version']
    versions = commands.getall(
        context, model, backend,
        action=Action.SEARCH,
        select=['parents', 'actions', 'schema'],
        query=spyna.parse('applied = null')
    )

    for version in versions:
        for action in version['actions']:
            ast = action['upgrade']
            env = Env(context, resolvers, executors, scope)
            expr = asttoexpr(ast)
            expr = env.resolve(expr)
            env.execute(expr)

            await write(context, model, [
                {
                    '_op': 'patch',
                    '_type': '_version',
                    'applied': datetime.datetime.now(datetime.timezone.utc),
                },
                {
                    '_op': 'upsert',
                    '_type': '_version',
                    'schema': version['schema'],
                },
            ])
