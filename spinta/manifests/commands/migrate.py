from spinta import commands
from spinta.backends.helpers import validate_and_return_begin
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.migrate import MigrationConfig
from spinta.components import Context
from spinta.manifests.components import Manifest


@commands.migrate.register(Context, Manifest, MigrationConfig)
def migrate(context: Context, manifest: Manifest, migration_config: MigrationConfig):
    with context:
        require_auth(context)
        backend = manifest.backend
        if backend:
            context.attach(f"transaction.{backend.name}", validate_and_return_begin, context, backend)
            commands.migrate(context, manifest, backend, migration_config)
