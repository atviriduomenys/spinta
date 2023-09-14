from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.helpers import check_manifest_path
from spinta.manifests.dict.components import DictManifest


@commands.configure.register(Context, DictManifest)
def configure(context: Context, manifest: DictManifest):
    rc: RawConfig = context.get('rc')
    path = rc.get('manifests', manifest.name, 'path')
    if path:
        check_manifest_path(manifest, path)
    else:
        path = None
    manifest.path = path
