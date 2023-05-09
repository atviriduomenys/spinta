from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.memory.components import MemoryManifest


@commands.configure.register(Context, MemoryManifest)
def configure(context: Context, manifest: MemoryManifest):
     rc: RawConfig = context.get('rc')
     manifest.path = rc.get('manifests', manifest.name, 'path')
