from typing import Optional

from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.sql.components import SqlManifest


@commands.configure.register(Context, SqlManifest)
def configure(context: Context, manifest: SqlManifest):
    rc: RawConfig = context.get('rc')
    path: Optional[str] = rc.get('manifests', manifest.name, 'path')
    prepare: Optional[str] = rc.get('manifests', manifest.name, 'prepare')
    manifest.prepare = prepare
    manifest.path = path
