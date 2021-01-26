import pathlib

from spinta import commands
from spinta.components import Context
from spinta.exceptions import ManifestFileDoesNotExist
from spinta.manifests.tabular.components import TabularManifest


@commands.configure.register(Context, TabularManifest)
def configure(context: Context, manifest: TabularManifest):
    rc = context.get('rc')
    path = rc.get('manifests', manifest.name, 'path')
    if path:
        path = pathlib.Path(path)
        if not path.exists():
            raise ManifestFileDoesNotExist(manifest, path=path)
    else:
        path = None
    manifest.path = path
