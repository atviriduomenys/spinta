from typing import List

from spinta.utils.aiotools import adrain
from spinta import commands
from spinta.components import Context
from spinta.commands.write import push_stream
from spinta.manifests.components import Manifest
from spinta.manifests.backend.helpers import read_sync_versions
from spinta.manifests.backend.helpers import versions_to_dstream
from spinta.manifests.backend.components import BackendManifest


@commands.sync.register()
async def sync(
    context: Context,
    manifest: BackendManifest,
    *,
    sources: List[Manifest] = None,
):
    stream = read_sync_versions(context, manifest)
    stream = versions_to_dstream(manifest, stream)
    await adrain(push_stream(context, stream))
