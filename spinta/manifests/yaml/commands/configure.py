import pathlib

from spinta import commands
from spinta.components import Context
from spinta.manifests.yaml.components import InlineManifest
from spinta.manifests.yaml.components import YamlManifest
from spinta.manifests.yaml.helpers import yaml_config_params


@commands.configure.register(Context, YamlManifest)
def configure(context: Context, manifest: YamlManifest):
    rc = context.get('rc')
    manifest.path = rc.get(
        'manifests', manifest.name, 'path',
        cast=pathlib.Path,
        required=True,
    )
    yaml_config_params(context, manifest)


@commands.configure.register(Context, InlineManifest)
def configure(context: Context, manifest: InlineManifest):
    rc = context.get('rc')
    manifest.path = None
    manifest.manifest = rc.get(
        'manifests', manifest.name, 'manifest',
        required=True,
    )
