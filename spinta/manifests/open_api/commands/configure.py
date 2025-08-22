from __future__ import annotations

from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.open_api.components import OpenAPIManifest


@commands.configure.register(Context, OpenAPIManifest)
def configure(context: Context, manifest: OpenAPIManifest) -> None:
    rc: RawConfig = context.get("rc")
    path: str | None = rc.get("manifests", manifest.name, "path")
    prepare: str | None = rc.get("manifests", manifest.name, "prepare")
    manifest.prepare = prepare
    manifest.path = path
