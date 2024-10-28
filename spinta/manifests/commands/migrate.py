from spinta import commands
from spinta.backends.helpers import validate_and_return_begin
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.migrate import MigrateRename, MigrateMeta
from spinta.components import Context
from spinta.exceptions import ModelNotFound, PropertyNotFound
from spinta.manifests.components import Manifest


def _validate_migrate_rename(context: Context, rename: MigrateRename, manifest: Manifest):
    tables = rename.tables.values()
    for table in tables:
        models = commands.get_models(context, manifest)
        if table["new_name"] not in models.keys():
            raise ModelNotFound(model=table["new_name"])
        model = commands.get_model(context, manifest, table["new_name"])
        for column in table["columns"].values():
            if column not in model.flatprops.keys():
                raise PropertyNotFound(property=column)


@commands.migrate.register(Context, Manifest, MigrateMeta)
def migrate(context, manifest, migrate_meta):
    with context:
        require_auth(context)
        _validate_migrate_rename(context, migrate_meta.rename, manifest)
        backend = manifest.backend
        if backend:
            context.attach(f'transaction.{backend.name}', validate_and_return_begin, context, backend)
            commands.migrate(context, manifest, backend, migrate_meta)

