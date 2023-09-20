from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.rdf.components import RdfManifest


@commands.configure.register(Context, RdfManifest)
def configure(context: Context, manifest: RdfManifest):
    rc: RawConfig = context.get('rc')
    manifest.path = rc.get('manifests', manifest.name, 'path')
    if manifest.path.endswith('.rdf'):
        manifest.format = 'application/rdf+xml'
    else:
        manifest.format = 'turtle'
