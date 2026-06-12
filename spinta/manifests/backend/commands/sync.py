from typing import List

from spinta import commands
from spinta.commands.write import push_stream
from spinta.components import Context
from spinta.manifests.backend.components import BackendManifest
from spinta.manifests.backend.helpers import read_sync_versions, versions_to_dstream
from spinta.manifests.components import Manifest
from spinta.utils.aiotools import adrain


@commands.sync.register()
async def sync(
    context: Context,
    manifest: BackendManifest,
    *,
    sources: List[Manifest] = None,
):
    stream = read_sync_versions(context, manifest)
    stream = versions_to_dstream(context, manifest, stream)
    await adrain(push_stream(context, stream))
