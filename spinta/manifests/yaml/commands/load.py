import pathlib
import logging

from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.yaml.components import YamlManifest
from spinta.manifests.yaml.helpers import yaml_config_params
from spinta.manifests.yaml.helpers import read_manifest_schemas
from spinta.manifests.yaml.helpers import read_freezed_manifest_schemas

log = logging.getLogger(__name__)


@commands.load.register(Context, YamlManifest)
def load(
    context: Context,
    manifest: YamlManifest,
    *,
    into: Manifest = None,
    freezed: bool = False,
):
    rc = context.get('rc')
    manifest.path = rc.get('manifests', manifest.name, 'path', cast=pathlib.Path, required=True)
    yaml_config_params(context, manifest)

    if freezed:
        log.info('Loading freezed manifest %r from %s.', manifest.name, manifest.path.resolve())
        schemas = read_freezed_manifest_schemas(manifest)
    else:
        log.info('Loading manifest %r from %s.', manifest.name, manifest.path.resolve())
        schemas = read_manifest_schemas(manifest)

    target = into or manifest
    load_manifest_nodes(context, target, schemas)
