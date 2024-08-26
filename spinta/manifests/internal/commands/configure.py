from spinta import commands
from spinta.components import Context
from spinta.manifests.internal.components import InternalManifest
from spinta.manifests.yaml.helpers import yaml_config_params
from spinta.utils.path import resource_filename


@commands.configure.register(Context, InternalManifest)
def configure(context: Context, manifest: InternalManifest):
    manifest.path = resource_filename('spinta', 'manifest')
    yaml_config_params(context, manifest)
