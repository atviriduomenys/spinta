from typing import Optional

from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.xsd.components import XSDManifest


@commands.configure.register(Context, XSDManifest)
def configure(context: Context, manifest: XSDManifest):
    rc: RawConfig = context.get('rc')
    path: Optional[str] = rc.get('manifests', manifest.name, 'path')
    prepare: Optional[str] = rc.get('manifests', manifest.name, 'prepare')
    manifest.prepare = prepare
    manifest.path = path
