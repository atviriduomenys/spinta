from typing import Optional

from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.helpers import check_manifest_path
from spinta.manifests.wsdl.components import WsdlManifest
from spinta.manifests.wsdl.helpers import normalize_wsdl_path


@commands.configure.register(Context, WsdlManifest)
def configure(context: Context, manifest: WsdlManifest):
    rc: RawConfig = context.get("rc")
    path: Optional[str] = rc.get("manifests", manifest.name, "path")
    prepare: Optional[str] = rc.get("manifests", manifest.name, "prepare")
    manifest.prepare = prepare
    manifest.path = normalize_wsdl_path(path) if path else None
    if manifest.path and not manifest.path.startswith(("http://", "https://")):
        check_manifest_path(manifest, manifest.path)
