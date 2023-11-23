from spinta import commands
from spinta.components import Context
from spinta.manifests.internal_sql.components import InternalSQLManifest
import sqlalchemy as sa

from spinta.manifests.internal_sql.helpers import get_table_structure


@commands.bootstrap.register(Context, InternalSQLManifest)
def bootstrap(context: Context, manifest: InternalSQLManifest):
    store = context.get('store')
    url = sa.engine.make_url(manifest.path)
    url.get_dialect()
    engine = sa.create_engine(url)
    inspector = sa.inspect(engine)
    meta = sa.MetaData(engine)
    if not inspector.has_table('_manifest'):
        table = get_table_structure(meta)
        table.create()

    for backend in store.backends.values():
        commands.bootstrap(context, backend)
