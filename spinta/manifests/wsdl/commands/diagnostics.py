from __future__ import annotations

from spinta import commands
from spinta.components import Context
from spinta.manifests.wsdl.components import WsdlManifest


@commands.store_schema_diagnostics.register(Context, WsdlManifest, str, list)
def store_schema_diagnostics(
    context: Context,
    manifest: WsdlManifest,
    path: str,
    diagnostics: list,
    **kwargs,
) -> None:
    from spinta.manifests.wsdl.helpers import normalize_wsdl_path

    normalized_path = normalize_wsdl_path(path)

    manifest.schema_diagnostics = list(diagnostics)

    if not diagnostics:
        return

    if not context.has("wsdl.schema_diagnostics", value=True):
        context.set("wsdl.schema_diagnostics", [])

    stored = context.get("wsdl.schema_diagnostics")
    for diagnostic in diagnostics:
        stored.append((normalized_path, str(diagnostic)))
