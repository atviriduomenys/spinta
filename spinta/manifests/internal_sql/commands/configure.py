from typing import Optional

from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.internal_sql.components import InternalSQLManifest


@commands.configure.register(Context, InternalSQLManifest)
def configure(context: Context, manifest: InternalSQLManifest):
    rc: RawConfig = context.get('rc')
    path: Optional[str] = rc.get('manifests', manifest.name, 'path')
    manifest.path = path
