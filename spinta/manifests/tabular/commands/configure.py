import pathlib

from spinta import commands
from spinta.components import Context
from spinta.manifests.tabular.components import TabularManifest


@commands.configure.register(Context, TabularManifest)
def configure(context: Context, manifest: TabularManifest):
    rc = context.get('rc')
    manifest.path = rc.get(
        'manifests', manifest.name, 'path',
        required=True,
        cast=pathlib.Path,
        exists=True,
    )
