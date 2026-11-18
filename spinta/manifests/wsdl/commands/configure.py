from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.wsdl.components import WsdlManifest


@commands.configure.register(Context, WsdlManifest)
def configure(context: Context, manifest: WsdlManifest):
    rc: RawConfig = context.get("rc")
    manifest.path = rc.get("manifests", manifest.name, "path")
    manifest.prepare = rc.get("manifests", manifest.name, "prepare")
