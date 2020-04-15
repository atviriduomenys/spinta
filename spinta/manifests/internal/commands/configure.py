import pathlib

import pkg_resources as pres

from spinta import commands
from spinta.components import Context
from spinta.manifests.internal.components import InternalManifest
from spinta.manifests.yaml.helpers import yaml_config_params


@commands.configure.register(Context, InternalManifest)
def configure(context: Context, manifest: InternalManifest):
    manifest.path = pathlib.Path(pres.resource_filename('spinta', 'manifest'))
    yaml_config_params(context, manifest)
