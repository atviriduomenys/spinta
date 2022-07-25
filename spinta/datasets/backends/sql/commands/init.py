from spinta import commands
from spinta.components import Context
from spinta.datasets.backends.sql.components import Sql
from spinta.manifests.components import Manifest


@commands.prepare.register(Context, Sql, Manifest)
def prepare(context: Context, backend: Sql, manifest: Manifest):
    # XXX: Moved reflection to
    #      spinta/datasets/backends/sql/components:Sql.get_table
    # log.info(f"Reflecting database for {backend.name!r} backend, this might take time...")
    # backend.schema.reflect()
    pass
