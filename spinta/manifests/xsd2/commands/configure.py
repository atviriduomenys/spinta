from typing import Optional

from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.xsd2.components import XsdManifest2


@commands.configure.register(Context, XsdManifest2)
def configure(context: Context, manifest: XsdManifest2):
    rc: RawConfig = context.get("rc")
    path: Optional[str] = rc.get("manifests", manifest.name, "path")
    prepare: Optional[str] = rc.get("manifests", manifest.name, "prepare")
    manifest.prepare = prepare
    manifest.path = path
