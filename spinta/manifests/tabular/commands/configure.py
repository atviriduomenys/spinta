import pathlib
from typing import Optional

from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.exceptions import ManifestFileDoesNotExist
from spinta.manifests.tabular.components import AsciiManifest
from spinta.manifests.tabular.components import CsvManifest
from spinta.manifests.tabular.components import TabularManifest


@commands.configure.register(Context, TabularManifest)
def configure(context: Context, manifest: TabularManifest):
    rc: RawConfig = context.get('rc')
    path: Optional[str] = rc.get('manifests', manifest.name, 'path')
    if path:
        if (
            not path.startswith(('http://', 'https://')) and
            not pathlib.Path(path).exists()
        ):
            raise ManifestFileDoesNotExist(manifest, path=path)
    else:
        path = None
    manifest.path = path

    if isinstance(manifest, (CsvManifest, AsciiManifest)):
        manifest.file = rc.get('manifests', manifest.name, 'file')
