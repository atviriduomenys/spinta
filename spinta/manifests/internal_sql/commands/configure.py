from typing import Optional
import sqlalchemy as sa

from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.internal_sql.components import InternalSQLManifest
from spinta.manifests.internal_sql.helpers import get_table_structure


@commands.configure.register(Context, InternalSQLManifest)
def configure(context: Context, manifest: InternalSQLManifest):
    rc: RawConfig = context.get('rc')
    path: Optional[str] = rc.get('manifests', manifest.name, 'path')
    manifest.path = path
    url = sa.engine.make_url(manifest.path)
    engine = sa.create_engine(url)
    manifest.engine = engine
    meta = sa.MetaData(engine)
    manifest.table = get_table_structure(meta)
