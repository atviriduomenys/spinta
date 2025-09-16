from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.components import Context
from spinta.manifests.internal_sql.components import InternalSQLManifest


@commands.reload_backend_metadata.register(Context, InternalSQLManifest, PostgreSQL)
def reload_backend_metadata(context: Context, manifest: InternalSQLManifest, backend: PostgreSQL):
    backend.schema.clear()
    backend.tables = {}
    commands.prepare(context, backend, manifest, ignore_duplicate=True)
