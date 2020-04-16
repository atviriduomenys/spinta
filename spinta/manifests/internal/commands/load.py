import logging

from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.internal.components import InternalManifest
from spinta.manifests.yaml.helpers import yaml_config_params
from spinta.manifests.yaml.helpers import read_manifest_schemas
from spinta.manifests.yaml.helpers import read_freezed_manifest_schemas

log = logging.getLogger(__name__)


@commands.load.register(Context, InternalManifest)
def load(
    context: Context,
    manifest: InternalManifest,
    *,
    into: Manifest = None,
    freezed: bool = True,
):
    yaml_config_params(context, manifest)

    if freezed:
        schemas = read_freezed_manifest_schemas(manifest)
    else:
        schemas = read_manifest_schemas(manifest)

    target = into or manifest
    load_manifest_nodes(context, target, schemas)
