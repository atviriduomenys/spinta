from typing import Optional

from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.helpers import check_manifest_path
from spinta.manifests.wsdl.components import WsdlManifest


@commands.configure.register(Context, WsdlManifest)
def configure(context: Context, manifest: WsdlManifest):
    rc: RawConfig = context.get('rc')
    path: Optional[str] = rc.get('manifests', manifest.name, 'path')
    if path:
        check_manifest_path(manifest, path)
    else:
        path = None
    manifest.path = path
